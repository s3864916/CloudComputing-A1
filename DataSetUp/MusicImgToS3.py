import logging
import boto3
import requests
from io import BytesIO
from botocore.exceptions import ClientError
import json


BUCKET_NAME = "music-img-s33864916"

def create_bucket(bucket_name, region=None):
    """
    Create an S3 bucket in a specified region
    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).
    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        # Option 1
        response = s3_client.upload_file(file_name, bucket, object_name)

        # Option 2
        # with open(file_name, "rb") as f:
        #     s3_client.upload_fileobj(f, bucket, object_name)

    except ClientError as e:
        logging.error(e) 
        return False
    return True



def get_img_url_list(dynamodb=None):
    url_list = []
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
        
    table = dynamodb.Table('music')
    response = table.scan(ProjectionExpression='img_url')
    for i in response['Items']:
        url_list.append(i['img_url'])

    # Remove duplicate items
    url_list = list(set(url_list))

    return url_list


def getURLFromRead(file_name):
    # Load data from A1.json file
    with open(file_name, 'r') as f:
        data = json.load(f)

    # Extract img_url values from songs list
    url_list = [song['img_url'] for song in data['songs']]

    # Remove duplicate URLs
    url_list = list(set(url_list))
    return url_list


def downloadImgsToS3(url_list, bucket):
    s3 = boto3.client('s3')

    # Download each image from its URL and upload it to S3
    for url in url_list:
        response = requests.get(url, stream=True)
        image_data = response.raw

        # Generate object_name for the image based on the URL
        object_name = url.split('/')[-1]
        # object_name = url

        # Upload the image to S3
        try:
            print("Uploading:", object_name)
            s3.upload_fileobj(image_data, bucket, object_name)

        except ClientError as e:
            logging.error(e) 
            return False


def main():
    # print("Creating bucket...")
    # create_bucket(BUCKET_NAME)

    # print("Getting image url list from DynamoDB...")
    # url_list = get_img_url_list()

    print("Getting image url list from a1.json...")
    url_list = getURLFromRead(file_name="a1.json")

    print("Downloading image and Uploading to S3 bucket")
    downloadImgsToS3(url_list, BUCKET_NAME)
    return


if __name__ == "__main__":
    main()