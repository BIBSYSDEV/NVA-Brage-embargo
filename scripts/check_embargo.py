import csv, sys

embargo_file_name = 'embargo_files.csv'

# Name of the S3 bucket to extract identifiers from

if (len(sys.argv) < 3):
  print ('Usage: check_embargo.py <inst> <env>')
  exit(1)

from load_registration import load_registration
from scripts.embargo_appender import extract_handles

# The institution for which the script will extract publication identifiers
institution = sys.argv[1]

# The environment where the script runs (e.g., 'dev', 'prod')
environment = sys.argv[2]
if (environment == 'prod'):
  bucket_name = 'brage-migration-reports-755923822223'
elif (environment == 'test'):
  bucket_name = 'brage-migration-reports-812481234721'
elif (environment == 'dev'):
  bucket_name = 'brage-migration-reports-884807050265'
else:
  print('Environment ' + env + " not supported!")
  exit(2)

def check_embargo(environment):
  log_file = open('../files/logfile.txt', 'w', encoding="utf-8")
  with open(f'../files/{embargo_file_name}', newline='') as embargo_file:
    embargo_file_reader = csv.reader(embargo_file, delimiter='|')
    for row in embargo_file_reader:
      found_file = False
      correct_import = True
      brage_file_name = row[1].strip()
      brage_embargo_date = row[2].strip()
      if len(row) == 4:
        id = row[3].strip()
        registration = load_registration(id, environment)
        for file in registration['associatedArtifacts']:
          nva_file_name = file['name']
          if nva_file_name == brage_file_name:
            found_file = True
            nva_embargo_date = ''
            if 'embargoDate' in file:
              nva_embargo_date = file['embargoDate'].split('T')[0]
            if file['visibleForNonOwner']:
              print('Should not be visible for non owner')
              correct_import = False
            if not nva_embargo_date == brage_embargo_date:
              print('Date incorrect')
              correct_import = False
        if not correct_import and found_file:
          # log handle
          # log manglende publikasjonsId
          log_file.write(
            brage_file_name + ' : ' + brage_embargo_date + ' : ' + id + ' : Embargo not set\n')
          print(brage_file_name + ' - Failed')
        else:
          print(brage_file_name + ' - OK')
  if not log_file.closed:
    log_file.close()


if __name__ == '__main__':
  # Function to extract handles from the given bucket for the given institution
  extract_handles(bucket_name, institution)

  # Function to check embargo status in the given environment
  check_embargo(environment)
