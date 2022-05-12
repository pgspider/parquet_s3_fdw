#!/usr/bin/env python3

import pandas as pd
import sys, getopt
import shutil
import boto3

def backup(path):
    shutil.copy(path, path + ".bk")

def revert_local(path):
    shutil.move(path + ".bk", path)

def split_s3_path(s3_path):
    path_parts=s3_path.replace("s3://","").split("/")
    bucket=path_parts.pop(0)
    key="/".join(path_parts)
    return bucket, key

def revert_s3(path, key_id='minioadmin', access_key='minioadmin'):
    s3 = boto3.resource('s3',
                        endpoint_url='http://127.0.0.1:9000',
                        aws_access_key_id=key_id,
                        aws_secret_access_key=access_key)
    bucket, file_path = split_s3_path(path)
    revert_local('/tmp/data.parquet')
    s3.Bucket(bucket).upload_file('/tmp/data.parquet', file_path)

def delete_first_row_s3(path, key_id='minioadmin', access_key='minioadmin'):
    s3 = boto3.resource('s3',
                        endpoint_url='http://127.0.0.1:9000',
                        aws_access_key_id=key_id,
                        aws_secret_access_key=access_key)
    bucket, file_path = split_s3_path(path)
    s3.Bucket(bucket).download_file(file_path, '/tmp/data.parquet')
    delete_first_row_local('/tmp/data.parquet')
    s3.Bucket(bucket).upload_file('/tmp/data.parquet', file_path)

def delete_first_row_local(path):
    backup(path)
    df = pd.read_parquet(path)
    df = df.drop([0])
    df.to_parquet(path, engine='pyarrow', index=False)

def revert(path):
    is_s3 = path.lower().startswith("s3://")
    if not is_s3:
        revert_local(path)
    else:
        revert_s3(path)

def delete_first_row(path):
    is_s3 = path.lower().startswith("s3://")
    if not is_s3:
        delete_first_row_local(path)
    else:
        delete_first_row_s3(path)

def main(argv):
    path = ''
    reversed = False
    try:
        opts, args = getopt.getopt(argv,"rp:",["path="])
    except getopt.GetoptError:
        print('test.py -p <inputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-p", "--path"):
            path = arg
        if opt == "-r":
            reversed = True

    if path == '':
        print('Path is not exist')
    if reversed == False:
        delete_first_row(path)
        print(path)
    else:
        revert(path)
        print(path)

if __name__ == "__main__":
    main(sys.argv[1:])
