#!/usr/bin/python
import boto3
import math
import os
import datetime
import time
import bz2
import shutil
import os
import gnupg
from botocore.client import Config

aws_access_key = 'aws_access_key_here'
aws_secret_key = 'aws_secret_key_here'
phrase = 'AES_key'
gpg = gnupg.GPG(gnupghome='/var/log/scribe/logger')

log_dir = '/path/to/your/logs'
log_sub = [ 'subdir1',
            'subdir2'
            ]
bucket = 's3_bucket_name'


def upload_to_s3(file_list, bucket_name):
    conn = boto3.resource('s3', aws_access_key_id = aws_access_key, aws_secret_access_key = aws_secret_key, config=Config(signature_version='s3v4'))
    for file in file_list:
        print 'Uploading: ' + file + '\n'
        remote_name = file.split('/')
        conn.Object(bucket_name, file).put(Body=open(log_dir + file, 'rb'))

def is_older(filename):
    now = time.time()
    try:
        mtime = os.path.getmtime(log_dir + '/' + filename)
        if mtime < (now - 4 * 3600):
            return True
        else:
            return False
    except:
        print 'File ' + filename + ' doesnt exist'


def validate_object(bucket, file_list, access_key, secret_key):
    conn = boto3.resource('s3', aws_access_key_id = access_key, aws_secret_access_key = secret_key, config=Config(signature_version='s3v4'))
    my_bucket = conn.Bucket(bucket)
    for file in file_list:
        key = file
        objs = list(my_bucket.objects.filter(Prefix=key))
        if len(objs) > 0 and objs[0].key == key:
            print key + ' alredy uploaded'
            file_list.remove(key)
        else:
            print key + ' doesnt exist'

def get_files_from_dir(log_dir, log_sub):
    log_files = []
    for directory in log_sub:
        path = log_dir + directory + '/'
        for root, dirs, files in os.walk(path):
            for file in files:
                if not (file.endswith('.bz2'):
                    log_files.append(directory + '/' + file)
    return log_files

def encrypt(filename):
    print 'Encrypting: ' + log_dir + filename
    with open(log_dir + filename, 'rb') as f:
        status = gpg.encrypt_file(f, None, passphrase=phrase, symmetric='AES256', output=log_dir + filename + '.aes')
    print 'ok: ', status.ok
    print 'status: ', status.status
    print 'stderr: ', status.stderr
    if status.ok:
        return True
    else:
        return False

def decrypt(filename):
    with open(log_dir + filename, 'rb') as f:
        status = gpg.decrypt_file(f, output=log_dir + filename + '-decrypted')
    print 'ok: ', status.ok
    print 'status: ', status.status
    print 'stderr: ', status.stderr
    if status.ok:
        return True
    else:
        return False


def upload(filename):
    parsed_name = filename.split('/')
    dir = parsed_name[0]
    file = parsed_name[1].split('-')
    gz_filename = file[0] + '-archive'
    for el in file[1:]:
        gz_filename = gz_filename + '-' +  el
    gz_filename = gz_filename + '.bz2'
    full_file_path = log_dir + dir + '/' + gz_filename
    print 'Compressing: ' + log_dir + filename 
    with open(log_dir + filename, 'rb') as f_in, bz2.BZ2File(log_dir + dir + '/' + gz_filename, 'w') as f_out:
        shutil.copyfileobj(f_in, f_out)
    print 'Removing: ' + log_dir + filename
    os.remove(log_dir + filename)
    if encrypt(dir + '/' + gz_filename):
        upload_to_s3([dir + '/' + gz_filename + '.aes',], bucket)
        os.remove(log_dir + dir + '/' + gz_filename + '.aes')

file_list = get_files_from_dir(log_dir, log_sub)
print 'Objects: ' + str(len(file_list))
for file in file_list:
    if is_older(file):
        upload(file)
