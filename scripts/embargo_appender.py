from urllib.parse import urlparse

import boto3
import csv
import os

embargo_file = '../files/FileEmbargo.txt'
output_file = '../files/embargo_files.csv'


def extract_handles(bucket_name, institution):
  session = boto3.session.Session()

  s3_client = session.client('s3')

  paginator = s3_client.get_paginator('list_objects_v2')
  prefix = f'HANDLE_REPORTS/{institution}/'
  pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

  s3_keys = []
  for page in pages:
    for obj in page['Contents']:
      s3_keys.append(obj['Key'])

    with open(embargo_file, 'r') as csv_file:
      csv_reader = csv.reader(csv_file, delimiter='|')
      with open(output_file, 'w', newline='') as new_csv_file:
        csv_writer = csv.writer(new_csv_file, delimiter='|')
        for row in csv_reader:
          if row and len(row) > 0:
            handle = row[0].strip()
            handle_path = urlparse(handle).path
            for key in s3_keys:
              if handle_path in key:
                identifier = os.path.basename(key)
                row.append(identifier)
                csv_writer.writerow(row)
                break
