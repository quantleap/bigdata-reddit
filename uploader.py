import math, os
import boto
from boto.s3.connection import S3Connection
from filechunkio import FileChunkIO


def upload_multipart(source_path, bucket):
    # Get file info
    source_size = os.stat(source_path).st_size

    # Create a multipart upload request
    mp = bucket.initiate_multipart_upload(os.path.basename(source_path))

    # Use a chunk size of 50 MiB (feel free to change this)
    chunk_size = 52428800
    chunk_count = int(math.ceil(source_size / float(chunk_size)))

    # Send the file parts, using FileChunkIO to create a file-like object
    # that points to a certain byte range within the original file. We
    # set bytes to never exceed the original file size.
    for i in range(chunk_count):
        print('upload {} {}/{}'.format(source_path, i+1, chunk_count))
        offset = chunk_size * i
        no_bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset, bytes=no_bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)

    # Finish the upload
    mp.complete_upload()


if __name__ == '__main__':
    # Connect to S3
    con = S3Connection('AKIAI3R3NYBFNFOY5VQA', 'kep1oEfDpp4uAnRBNQHMG02VX1cYB5mIN99P37vV')
    bucket = con.get_bucket('uva-bigdata-reddit-west-2c')
    for y in [2012, 2013, 2014, 2015]:
        for m in range(1, 13):
            if not (y == 2012 and m == 6):
                source_path = '/Volumes//Users/Tom/Downloads/reddit_data/{0}/RC_{0}-{1:02d}.bz2'.format(y, m)
                upload_multipart(source_path, bucket)

