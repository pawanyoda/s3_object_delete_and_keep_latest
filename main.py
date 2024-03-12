import boto3
from collections import defaultdict

s3_session = boto3.Session(profile_name='aws_test')


def get_latest_object(objects):
    # Sort objects based on last modified timestamp
    sorted_objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)
    # Return the latest object
    return sorted_objects[0]


def delete_old_objects(objects, latest_object_key):
    for obj in objects:
        if obj['Key'] != latest_object_key:
            # Delete the object
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])


def list_objects(bucket_name, prefix=''):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    all_objects = []
    for page in pages:
        if 'Contents' in page:
            all_objects.extend(page['Contents'])

    return all_objects


def keep_latest_object_per_directory(bucket_name, prefix=''):
    directories = defaultdict(list)

    # List all objects with the given prefix
    objects = list_objects(bucket_name, prefix)

    # Organize objects by directory
    for obj in objects:
        directory = '/'.join(obj['Key'].split('/')[:-1])
        directories[directory].append(obj)

    # Keep the latest object in each directory
    for directory, objects in directories.items():
        latest_object = get_latest_object(objects)
        print(f"delete_latest_object{latest_object['Key']}")
        delete_old_objects(objects, latest_object['Key'])


if __name__ == "__main__":
    # Initialize S3 client
    s3_client = s3_session.client('s3')

    # Specify bucket name and prefix
    bucket_name = 'aws_test'
    prefix = 'dev/stats/'

    # Keep the latest object in each directory
    keep_latest_object_per_directory(bucket_name, prefix)
