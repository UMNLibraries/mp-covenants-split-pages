import io
import re
import math
import time
import urllib.parse
import boto3
import botocore
from io import BytesIO

from PIL import Image
import pymupdf # PyMuPDF

Image.MAX_IMAGE_PIXELS = 1000000000
s3 = boto3.client('s3')

min_page_time = 0.6
test_mode = False


def convert_pdf_to_img(s3_response):
    ''' Convert PDF files to a single- or multi-page TIFF before more processing'''

    doc = pymupdf.open(stream=BytesIO(s3_response['Body'].read()), filetype="pdf")
    num_pages = doc.page_count
    print(f"The document has {num_pages} pages.")

    pil_images = []

    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        
        # Render the page to a Pixmap with the specified DPI
        pix = page.get_pixmap(dpi=300)
        
        # Convert the Pixmap to a PIL Image
        # Determine mode based on alpha channel presence
        mode = "RGBA" if pix.alpha else "RGB"
        img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
        
        pil_images.append(img)
    
    doc.close()
    return pil_images


def tif_pages_to_list(im):
    ''' Convert image to list of PIL Image instances for each page. This will insure similar output for converted PDF pages and multi-page TIFs '''
    try:
        num_pages = im.n_frames
    except AttributeError:
        # If only one page, return list with one item, the original open PIL Image
        num_pages = 1
        return [im], False

    if num_pages == 1:
        return [im], False

    # For multi-page images...
    page_ims = []
    for page_num in range(0, num_pages):
        im.seek(page_num)
        page_ims.append(im.copy())
    return page_ims, True
    

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


def check_oversized_mem(im, max_bytes=10380902):
    '''
    Check if nbytes is small enough for textract, which has a limit of 10485760 bytes
    Rounding down by 1% to make up for decode differences

    returns:
        Value 1, bool: Was the image oversized, requiring resave?
        Value 2, im: PIL image object
    '''
    buffer = io.BytesIO()
    # im.save(buffer, format="tiff", compression="jpeg")
    im.save(buffer, format="tiff", compression="tiff_lzw")
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
        # im.save(buffer, format="tiff", compression="jpeg")
        im.save(buffer, format="tiff", compression="tiff_lzw")

        final_byte_size = buffer.getbuffer().nbytes
        print(f'Resized to {final_byte_size} bytes ({round(final_byte_size / max_bytes, 2)}% of max)')
        return True, im
    else:
        print('Memory within limits.')
        return False, im


def put_tif_buffer(bucket, key, buffer):
    print(bucket, key)
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
        print('put error')
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


def build_out_key_path(key):
    ''' Don't send test data to raw/, because that will trigger step function '''
    if 'test/' in key:
        return key
    
    if not key.startswith('raw/'):
        return 'raw/' + key
    
    return key


def lambda_handler(event, context):
    """
    Do pre-processing steps needed before OCR is possible. At end, pass on information about each
    modified or original file to looping step 2.
    """
    # print("Received event: " + json.dumps(event, indent=2))
    # TODO: Need option for separate in bucket and out bucket
    # TODO: Add "raw" prefix if in another bucket
    in_bucket = None
    out_bucket = None

    if 'Records' in event:
        # Get the object from a more standard put event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    else:
        # Get the object from an EventBridge event
        bucket = event['detail']['bucket']['name']
        key = event['detail']['object']['key']
        if 'in_bucket' in event['detail']['object']:
            in_bucket = event['detail']['object']['in_bucket']
        if 'out_bucket' in event['detail']['object']:
            out_bucket = event['detail']['object']['out_bucket']

    if ".DS_Store" in key:
        return {
            "statusCode": 200,
            "body": {
                "message": ".DS_Store file. Ignore!",
                "pages": []
            }
        }

    print(key)

    key_parts = re.split(r'\.(?=[A-Za-z0-9]{3,4}$)', key, flags=re.IGNORECASE)
    key_minus_extension = key_parts[0]
    extension = key_parts[1]

    if in_bucket:
        response = s3.get_object(Bucket=in_bucket, Key=key)
    else:
        response = s3.get_object(Bucket=bucket, Key=key)

    bool_modified = False  # If the image goes through the whole process unmodified, no re-save is needed

    # Convert from PDF if necessary
    if extension in ['pdf', 'PDF']:
        print('need to convert pdf')
        page_ims = convert_pdf_to_img(response)
        bool_modified = True
    else:
        im = Image.open(response['Body'])
        page_ims, bool_modified = tif_pages_to_list(im)

    unmodified_pages = []
    modified_pages = []

    # for page_num in range(0, num_pages):
    num_pages = len(page_ims)
    for page_num, page_im in enumerate(page_ims):
        # Save star page of processing each time for use in possible sleep later
        start_time = time.time()

        if num_pages > 1:
            out_key = f"{key_minus_extension}_SPLITPAGE_{page_num+1}.tif"  # I guess everything modified will be a TIF? Maybe should be a JPEG for storage, but can revist later.
        else:
            out_key = key

        # Check image mode...
        bool_wrong_img_mode, page_im = check_img_mode(page_im)
        if bool_wrong_img_mode:
            bool_modified = True

        # Check oversized dimen...
        bool_oversized_dimen, page_im = check_oversized_dimen(page_im)
        if bool_oversized_dimen:
            bool_modified = True

        # Check memory size...
        bool_mem_too_big, page_im = check_oversized_mem(page_im)
        if bool_mem_too_big:
            bool_modified = True

        if bool_modified:
            # All modified images require re-save, which will trigger another run of Step Function, with goal of getting to passing through unmodified on the next run 
            buffer = io.BytesIO()
            # page_im.save(buffer, format="tiff", compression="jpeg")
            page_im.save(buffer, format="tiff", compression="tiff_lzw")
            buffer.seek(0)

            # Check for test folder upload, which indicates that modified files will have _modified appended to avoid overwriting raw samples
            if 'test/' in out_key.lower() and 'splitpage' not in out_key.lower():
                if re.match(r'.+\.\d{3}$', key):
                    # file with no extension, assume it's a tif with no .tif at the end, e.g. file.001
                    out_key = f"{key}_MODIFIED.tif"
                else:
                    out_key = f"{key_minus_extension}_MODIFIED.tif"
            else:
                if re.match(r'.+\.\d{3}$', key):
                    # file with no extension, assume it's a tif with no .tif at the end, e.g. file.001
                    out_key = f"{key}.tif"

            out_key = build_out_key_path(out_key)
            if out_bucket:
                modified_pages.append({'bucket': out_bucket, 'key': out_key, 'page_num': page_num + 1})
                put_tif_buffer(out_bucket, out_key, buffer)
            else:
                modified_pages.append({'bucket': bucket, 'key': out_key, 'page_num': page_num + 1})
                put_tif_buffer(bucket, out_key, buffer)
            sleep_if_needed(min_page_time, start_time)

        else:
            # TODO: key coming out null
            # These unmodified pages will go on to the next step directly from here
            unmodified_pages.append({'bucket': bucket, 'key': build_out_key_path(out_key), 'page_num': page_num + 1})

    return {
        "statusCode": 200,
        "body": {
            "message": "Success",
            "bucket": bucket if not in_bucket else None,
            "in_bucket": in_bucket,
            "out_bucket": out_bucket,
            "orig": key,
            "page_count": num_pages,
            "modified_pages": modified_pages,
            "pages": unmodified_pages  # This should only ever be one page, but leaving loop to preserve step function logic
            # "location": ip.text.replace("\n", "")
        },
    }
