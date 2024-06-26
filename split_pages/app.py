import io
import re
import math
import time
import urllib.parse
import boto3
import botocore

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

min_page_time = 0.6
test_mode = False

def check_oversized_dimen(im):
    '''
    Test pixel dimensions to make sure Textract won't reject it.
    Max dimensions for Textract are 10,000 pixels in either dimension

    Note: This is separate from MEMORY max size, which is covered separately
    '''

    width, height = im.size
    max_dimension = max([width, height])

    if max_dimension <= 10000:
        # It's fine, let's move on
        return False, im

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

    return True, im


def check_img_mode(im):
    '''
    Test image color mode to make sure Textract won't reject it.
    For sure index type won't work (mode == '1')

    returns:
        Value 1, bool: Was the image in an incompatible color mode, requiring resave?
        Value 2, im: PIL image object
    '''
    print(im.mode)
    if im.mode in ['1']:
        print('attempting conversion')
        im = im.convert('RGB')

        return True, im

    return False, im


def check_oversized_mem(im, max_bytes=10380902):
    '''
    Check if nbytes is small enough for textract, which has a limit of 10485760 bytes
    Rounding down by 1% to make up for decode differences

    returns:
        Value 1, bool: Was the image oversized, requiring resave?
        Value 2, im: PIL image object
    '''
    buffer = io.BytesIO()
    im.save(buffer, format="tiff", compression="jpeg")
    byte_size = buffer.getbuffer().nbytes

    if byte_size > max_bytes:
        print(f'Memory resize needed. Original size: {byte_size}. Reduction needed')

        # Adapted from https://stackoverflow.com/questions/66455731/how-to-calculate-the-resulting-filesize-of-image-resize-in-pil
        original_bytes_per_pixel = byte_size / (im.size[0] * im.size[1])

        new_bytes_per_pixel = original_bytes_per_pixel * (max_bytes / byte_size)
        new_bytes_ratio = math.sqrt(new_bytes_per_pixel / original_bytes_per_pixel)
        new_width, new_height = int(0.95* new_bytes_ratio * im.size[0]), int(0.95* new_bytes_ratio * im.size[1])

        im = im.resize((new_width, new_height), Image.Resampling.LANCZOS)

        buffer = io.BytesIO()
        im.save(buffer, format="tiff", compression="jpeg")
        # buffer.seek(0)
        final_byte_size = buffer.getbuffer().nbytes
        print(f'Resized to {final_byte_size} bytes ({round(final_byte_size / max_bytes, 2)}% of max)')
        return True, im
    else:
        print('Memory within limits.')
        return False, im


def put_tif_buffer(bucket, key, buffer):
    try:
        s3_response = s3.put_object(
            Body=buffer,
            Bucket=bucket,
            Key=key,
            StorageClass='GLACIER_IR',
            ContentType='image/tif',
            # ACL='public-read'
        )
        return True
    except botocore.exceptions.ClientError as error:
        return {
            "statusCode": 400,
            "body": {
                "message": f"Boto clienterror: {error}."
            }
        }


def sleep_if_needed(min_page_time, start_time):
    '''If necessary, wait before moving on to next upload'''
    if min_page_time > 0:
        elapsed = time.time() - start_time
        time_remaining = min_page_time - elapsed
        if time_remaining > 0:
            print(f'Pausing {time_remaining} seconds')
            time.sleep(time_remaining)


def lambda_handler(event, context):
    """
    Do pre-processing steps needed before OCR is possible. Passed on information about each
    modified or original file to looping step 2.
    """
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

    # if "SPLITPAGE_" in key:
    #     return {
    #         "statusCode": 200,
    #         "body": {
    #             "message": "Event firing on creation of split page. Ignore and stop step function",
    #             "pages": []
    #         }
    #     }

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
    bool_modified = False  # If the image goes through the whole process unmodified, no re-save is needed
    num_pages = im.n_frames
    unmodified_pages = []
    modified_pages = []

    for page_num in range(0, num_pages):
        # Save star page of processing each time for use in possible sleep later
        start_time = time.time()

        if num_pages > 1:
            # key_minus_extension = re.split(r'\.tif(?:f)?', key, flags=re.IGNORECASE)[0]
            # out_key = f"{key_minus_extension}_SPLITPAGE_{page_num+1}.tif"
            key_parts = re.split(r'\.(?=[A-Za-z]{3,4}$)', key, flags=re.IGNORECASE)
            key_minus_extension = key_parts[0]
            extension = key_parts[1]
            out_key = f"{key_minus_extension}_SPLITPAGE_{page_num+1}.tif"  # I guess everything modified will be a TIF? Maybe should be a JPEG for storage, but can revist later.

            bool_modified = True  # Multipage automatically means re-save needed
            im.seek(page_num)
            page_im = im.copy()
        else:
            out_key = key
            page_im = im

         # Check image mode...
        bool_wrong_img_mode, page_im = check_img_mode(page_im)
        if bool_wrong_img_mode:
            bool_modified = True
 
        # Check oversized dimen...
        bool_oversized_dimen, page_im = check_oversized_dimen(page_im)

        # Check memory size...
        bool_mem_too_big, page_im = check_oversized_mem(page_im)
        if bool_mem_too_big:
            bool_modified = True

        if bool_modified:
            # All modified images require re-save, which will trigger another run of Step Function, with goal of getting to passing through unmodified on the next run 
            buffer = io.BytesIO()
            page_im.save(buffer, format="tiff", compression="jpeg")
            buffer.seek(0)

            # Check for test folder upload, which indicates that modified files will have _modified appended to avoid overwriting raw samples
            if 'test/' in out_key.lower() and 'splitpage' not in out_key.lower():
                key_parts = re.split(r'\.(?=[A-Za-z]{3,4}$)', key, flags=re.IGNORECASE)
                key_minus_extension = key_parts[0]
                extension = key_parts[1]
                out_key = f"{key_minus_extension}_MODIFIED.{extension}"

            modified_pages.append({'bucket': bucket, 'key': out_key, 'page_num': page_num + 1})

            put_tif_buffer(bucket, out_key, buffer)
            sleep_if_needed(min_page_time, start_time)

        else:
            # These unmodified pages will go on to the next step directly from here
            unmodified_pages.append({'bucket': bucket, 'key': out_key, 'page_num': page_num + 1})

    return {
        "statusCode": 200,
        "body": {
            "message": "Success",
            "bucket": bucket,
            "orig": key,
            "page_count": num_pages,
            "modified_pages": modified_pages,
            "pages": unmodified_pages  # This should only ever be one page, but leaving loop to preserve step function logic
            # "location": ip.text.replace("\n", "")
        },
    }
