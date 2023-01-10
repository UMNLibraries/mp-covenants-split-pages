import io
import re
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

def check_oversized(bucket, key, im):
    '''
    Test pixel dimensions to make sure Textract won't reject it.
    Max dimensions for Textract are 10,000 pixels in either dimension
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

    im = im.resize((new_width, new_height), Image.ANTIALIAS)
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

def split_tiff(bucket, key, im):
    num_pages = im.n_frames

    if num_pages == 1:
        print('Only 1 page found, exiting.')
        return [{'bucket': bucket, 'key': key, 'page_num': 1}]
    else:
        print(f"{num_pages} pages")
        print(im.mode)
#         if im.mode == 'L':
#             try:
#                 im = im.convert('RGB')
#             except:
#                 arr = np.array(im)
#                 im = Image.fromarray(arr)
#                 im.save('test_images/test_convert.tif', format="tiff")
        page_buffers = []
        for page_num in range(0, num_pages):
            im.seek(page_num)
            page_im = im.copy()
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

    response = s3.get_object(Bucket=bucket, Key=key)

    im = Image.open(response['Body'])

    bool_oversized = check_oversized(bucket, key, im)
    if bool_oversized:
        print('Oversized TIF. Will try again.')
        return {
            "statusCode": 200,
            "body": {
                "message": "Oversized image detected. A resized copy has been written over the raw file, which should trigger another Step Function.",
                "pages": []
            }
        }

    split_result = split_tiff(bucket, key, im)
    if len(split_result) == 1:
        print('Single page TIFF, pass to next step as is...')
        out_keys = split_result

    elif len(split_result) > 1:
        print(f'Multi-page TIF split into {len(split_result)} pages.')

        out_keys = []
        for page in split_result:
            key_minus_extension = re.split(r'\.tif(?:f)?', page['key'])[0]
            out_key = f"{key_minus_extension}_SPLITPAGE_{page['page_num']}.tif"

            out_keys.append({'bucket': page['bucket'], 'key': out_key, 'page_num': page['page_num']})

            # Upload image page to destination bucket
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
