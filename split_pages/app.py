import io
import re
import math
import urllib.parse
import boto3

from PIL import Image

'''
Folder structure:

covenants-deeds-images
    -raw
        -mn-ramsey-county
        -wi-milwaukee-county
    -ocr
        -txt
            -mn-ramsey-county
            -wi-milwaukee-county
        -json
            -mn-ramsey-county
            -wi-milwaukee-county
        -stats
            -mn-ramsey-county
            -wi-milwaukee-county
        -hits
            -mn-ramsey-county
            -wi-milwaukee-county
    -web
        -mn-ramsey-county
        -wi-milwaukee-county
'''

Image.MAX_IMAGE_PIXELS = 1000000000
s3 = boto3.client('s3')

def check_oversized_dimen(bucket, key, im):
    '''
    Test pixel dimensions to make sure Textract won't reject it.
    Max dimensions for Textract are 10,000 pixels in either dimension

    Note: This is separate from MEMORY max size, which is covered separately
    '''

    width, height = im.size
    max_dimension = max([width, height])

    if max_dimension <= 10000:
        # It's fine, let's move on
        return False

    # OK, something is over 10,000 pixels. Let's resize so the max dimension is 10,000
    if width == max_dimension:
        new_width = 10000
        new_height = int(
            float(height) * float(new_width/float(width)))
    else:
        new_height = 10000
        new_width = int(float(width)
                        * float(new_height/float(height)))

    im = im.resize((new_width, new_height), Image.Resampling.LANCZOS)

    buffer = io.BytesIO()
    im.save(buffer, format="tiff")
    buffer.seek(0)

    # Now overwrite the original TIF image, which should re-trigger the Step Function
    s3.put_object(
        Body=buffer,
        Bucket=bucket,
        Key=key,
        StorageClass='GLACIER_IR',
        ContentType='image/tif',
        # ACL='public-read'
    )
    return True


def check_oversized_mem(im, max_bytes=10485760):
    '''
    Check if nbytes is small enough for textract, which has a limit of 10485760 bytes

    returns:
        Value 1, bool: Was the image oversized, requiring resave?
        Value 2, im: PIL image object
    '''
    buffer = io.BytesIO()
    im.save(buffer, format="tiff")
    byte_size = buffer.getbuffer().nbytes

    if byte_size > max_bytes:
        print(f'Memory resize needed. Original size: {byte_size}. Reduction needed')

        # Adapted from https://stackoverflow.com/questions/66455731/how-to-calculate-the-resulting-filesize-of-image-resize-in-pil
        original_bytes_per_pixel = byte_size / (im.size[0] * im.size[1])

        new_bytes_per_pixel = original_bytes_per_pixel * (max_bytes / byte_size)
        new_bytes_ratio = math.sqrt(new_bytes_per_pixel / original_bytes_per_pixel)
        new_width, new_height = int(0.95* new_bytes_ratio * im.size[0]), int(0.95* new_bytes_ratio * im.size[1])

        im = im.resize((new_width, new_height), Image.ANTIALIAS)

        buffer = io.BytesIO()
        im.save(buffer, format="tiff")
        final_byte_size = buffer.getbuffer().nbytes
        print(f'Resized to {final_byte_size} bytes ({round(final_byte_size / max_bytes, 2)}% of max)')
        return True, im
    else:
        print('Memory within limits.')
        return False, im


def split_tiff(bucket, key, im):
    num_pages = im.n_frames

    if num_pages == 1:
        print('Only 1 page found, exiting.')
        return [{'bucket': bucket, 'key': key, 'page_num': 1}]
    else:
        print(f"{num_pages} pages")
        print(im.mode)

        page_buffers = []
        for page_num in range(0, num_pages):
            im.seek(page_num)
            page_im = im.copy()

            # Check memory size...
            mem_too_big, page_im = check_oversized_mem(page_im)

            buffer = io.BytesIO()
            page_im.save(buffer, format="tiff")
            buffer.seek(0)

            page_buffers.append({'bucket': bucket, 'key': key, 'page_num': page_num + 1, 'img_buffer': buffer})

    return page_buffers

def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    if 'Records' in event:
        # Get the object from a more standard put event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    else:
        # Get the object from an EventBridge event
        bucket = event['detail']['bucket']['name']
        key = event['detail']['object']['key']

    if "SPLITPAGE_" in key:
        return {
            "statusCode": 200,
            "body": {
                "message": "Event firing on creation of split page. Ignore and stop step function",
                "pages": []
            }
        }

    if ".DS_Store" in key:
        return {
            "statusCode": 200,
            "body": {
                "message": ".DS_Store file. Ignore!",
                "pages": []
            }
        }

    response = s3.get_object(Bucket=bucket, Key=key)

    im = Image.open(response['Body'])

    bool_oversized_dimen = check_oversized_dimen(bucket, key, im)
    if bool_oversized_dimen:
        print('Oversized TIF (dimensions). Will try again.')
        return {
            "statusCode": 200,
            "body": {
                "message": "Oversized image detected (dimensions). A resized copy has been written over the raw file, which should trigger another Step Function.",
                "pages": []
            }
        }

    split_result = split_tiff(bucket, key, im)
    if len(split_result) == 1:
        print('Single page TIFF, checking memory size...')

        # Check memory size...
        mem_too_big, im = check_oversized_mem(im)
        if mem_too_big:
            buffer = io.BytesIO()
            im.save(buffer, format="tiff")
            buffer.seek(0)

            # Overwrite image in raw bucket if resized.
            s3.put_object(
                Body=buffer,
                Bucket=bucket,
                Key=key,
                StorageClass='GLACIER_IR',
                ContentType='image/tif',
                # ACL='public-read'
            )

            # Now exit, because save will trigger new run.
            print('Oversized TIF (memory). Will try again.')
            return {
                "statusCode": 200,
                "body": {
                    "message": "Oversized image detected (dimensions). A resized copy has been written over the raw file, which should trigger another Step Function.",
                    "pages": []
                }
            }
        else:
            out_keys = split_result

    elif len(split_result) > 1:
        print(f'Multi-page TIF split into {len(split_result)} pages.')

        out_keys = []
        for page in split_result:
            key_minus_extension = re.split(r'\.tif(?:f)?', page['key'], flags=re.IGNORECASE)[0]
            out_key = f"{key_minus_extension}_SPLITPAGE_{page['page_num']}.tif"

            out_keys.append({'bucket': page['bucket'], 'key': out_key, 'page_num': page['page_num']})

            # Upload split image page to raw bucket
            s3.put_object(
                Body=page['img_buffer'],
                Bucket=bucket,
                Key=out_key,
                StorageClass='GLACIER_IR',
                ContentType='image/tif',
                # ACL='public-read'
            )

    else:
        print('Something strange happened. Error.')
        return {
            "statusCode": 400,
            "body": {
                "message": "Could not parse image as single or multi-page tiff."
            }
        }

    return {
        "statusCode": 200,
        "body": {
            "message": "Success",
            "bucket": bucket,
            "orig": key,
            "page_count": len(split_result),
            "pages": out_keys
            # "location": ip.text.replace("\n", "")
        },
    }
