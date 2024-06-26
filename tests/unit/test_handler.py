import io
# import sys
# import base64
import json
import toml
import boto3
import pytest
# from pympler import asizeof
from PIL import Image

from split_pages import app

with open('samconfig.toml', 'r') as f:
    config = toml.load(f)
    s3_bucket = config['default']['deploy']['parameters']['s3_bucket']
    s3_region = config['default']['deploy']['parameters']['region']

s3 = boto3.client('s3')

def open_s3_image(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    im = Image.open(response['Body'])
    return im


def get_s3_contents(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return str(response['Body'].read())


def check_max_byte_size(im, max_byte_size=10380902):
    '''10380902 is 99% of the hard Textract limit'''
    # Method 1: buffer.nbytes
    buffer = io.BytesIO()
    im.save(buffer, format="tiff", compression="jpeg")
    byte_size = buffer.getbuffer().nbytes

    # # Method 2: pympler asizeof
    # byte_size = asizeof.asizeof(im)

    # Method 3: sys.getsizeof
    # buffer = io.BytesIO()
    # im.save(buffer, format="tiff", compression="jpeg")
    # byte_size = sys.getsizeof(buffer)

    # Method 4: base64 encoding
    # im_contents = get_s3_contents(page['bucket'], page['key'])
    # byte_size = len(base64.b64encode(im_contents.encode('utf-8')))

    assert byte_size <= max_byte_size


def build_put_event(bucket, region, infile, size=100000):
    return {
        "version": "0",
        "id": "187c05fd-3810-f137-7e72-d4d7c5b7efa4",
        "detail-type": "Object Created",
        "source": "aws.s3",
        "account": "813228900636",
        "time": "2024-06-21T15:55:50Z",
        "region": region,
        "resources": [
            f"arn:aws:s3:::{bucket}"
        ],
        "detail": {
            "version": "0",
            "bucket": {
                "name": bucket
            },
            "object": {
                "key": infile,
                "size": size,
                "etag": "8e29d0cb274128925b950351126b9d0a",
                "sequencer": "006675A2860D0D6FE6"
            },
            "request-id": "S3CY6VQ5T3W27C37",
            "requester": "813228900636",
            "source-ip-address": "75.72.150.179",
            "reason": "PutObject"
        }
    }


@pytest.fixture()
def index_1_page_tif_event_1():
    """ Generates API GW Event"""
    return build_put_event(s3_bucket, s3_region, "test/mn-olmsted-county/mn_olmsted_H81540_index_color_1_page.tiff")

@pytest.fixture()
def index_1_page_tif_event_2():
    return build_put_event(s3_bucket, s3_region, "test/nc-forsyth-county/nc_forsyth_00440001.001_index_color_1_page.tif")

@pytest.fixture()
def index_multi_page_tif_event_1():
    return build_put_event(s3_bucket, s3_region, "test/mn-anoka-county/mn_anoka_26360876_index_color_multipage.TIF")

@pytest.fixture()
def rgb_weird_multi_page_jpeg_event_1():
    '''I think this is in reality a multipage TIF saved with a JPG extension. Weird!'''
    return build_put_event(s3_bucket, s3_region, "test/mn-sherburne-county/mn_sherburne_Abstract 86709_rgb.jpg")

@pytest.fixture()
def rgb_multi_page_tif_event_1():
    return build_put_event(s3_bucket, s3_region, "test/mn-sherburne-county/mn_sherburne_Abstract 86713_rgb_multi_page.tif")

@pytest.fixture()
def rgb_multi_page_tif_bigmem_event_1():
    # Oversized memory Dakota Torrens deed
    return build_put_event(s3_bucket, s3_region, "test/mn-dakota-county/mn_dakota_doc_NONE_book_183_page_578_bigmem_multipage.tif")

@pytest.fixture()
def multi_page_tif_bigmem_event_2():
    # Oversized memory Sherburne multipage file
    return build_put_event(s3_bucket, s3_region, "test/mn-sherburne-county/mn_sherburne_Abstract 104316_bigmem.tif")

@pytest.fixture()
def multi_page_tif_bigmem_event_3():
    # Oversized memory Sherburne multipage file
    return build_put_event(s3_bucket, s3_region, "test/mn-sherburne-county/mn_sherburne_Abstract 100615_bigmem.tif")



def test_index_1_page_tif_1(index_1_page_tif_event_1):

    ret = app.lambda_handler(index_1_page_tif_event_1, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert "message" in ret["body"]
    assert data["message"] == "Success"
    assert data["modified_pages"] == [{'bucket': 'covenants-deed-images', 'key': 'test/mn-olmsted-county/mn_olmsted_H81540_index_color_1_page_MODIFIED.tiff', 'page_num': 1}]

    # Check if page 1 also in correct mode
    im = open_s3_image(data["modified_pages"][0]['bucket'], data["modified_pages"][0]['key'])
    assert im.mode == 'RGB'


def test_index_1_page_tif_2(index_1_page_tif_event_2):

    ret = app.lambda_handler(index_1_page_tif_event_2, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert data["message"] == "Success"
    assert data["modified_pages"] == [{'bucket': 'covenants-deed-images', 'key': 'test/nc-forsyth-county/nc_forsyth_00440001.001_index_color_1_page_MODIFIED.tif', 'page_num': 1}]

    # Check if page 1 in correct mode
    im = open_s3_image(data["modified_pages"][0]['bucket'], data["modified_pages"][0]['key'])
    assert im.mode == 'RGB'


def test_rgb_weird_multi_page_jpeg_1(rgb_weird_multi_page_jpeg_event_1):

    ret = app.lambda_handler(rgb_weird_multi_page_jpeg_event_1, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert data["message"] == "Success"
    assert len(data["modified_pages"]) == 2
    assert data["modified_pages"][0]['key'] == "test/mn-sherburne-county/mn_sherburne_Abstract 86709_rgb_SPLITPAGE_1.tif"

    # Check if page 2 also in correct mode
    im = open_s3_image(data["modified_pages"][1]['bucket'], data["modified_pages"][1]['key'])
    assert im.mode == 'RGB'


def test_rgb_multi_page_tif_1(rgb_multi_page_tif_event_1):

    ret = app.lambda_handler(rgb_multi_page_tif_event_1, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert data["message"] == "Success"
    assert len(data["modified_pages"]) == 2

    # Check if page 2 also in correct mode
    im = open_s3_image(data["modified_pages"][1]['bucket'], data["modified_pages"][1]['key'])
    assert im.mode == 'RGB'


def test_rgb_multi_page_tif_bigmem_1(rgb_multi_page_tif_bigmem_event_1):

    ret = app.lambda_handler(rgb_multi_page_tif_bigmem_event_1, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert data["message"] == "Success"
    assert len(data["modified_pages"]) == 2

    for page in data["modified_pages"]:
        # Check if page in correct mode
        im = open_s3_image(page['bucket'], page['key'])
        assert im.mode == 'RGB'

        check_max_byte_size(im)


def test_multi_page_tif_bigmem_3(multi_page_tif_bigmem_event_3):

    ret = app.lambda_handler(multi_page_tif_bigmem_event_3, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert data["message"] == "Success"

    for page in data["modified_pages"][0:1]:
        # Check if page in correct mode
        im = open_s3_image(page['bucket'], page['key'])

        # Check if page lower than memory threshold
        check_max_byte_size(im)
        