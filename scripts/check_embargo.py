import csv, datetime, pytz, sys
from dateutil import parser

embargo_file_name = 'embargo_files.csv'

# Name of the S3 bucket to extract identifiers from

if (len(sys.argv) < 3):
  print ('Usage: check_embargo.py <env> <inst>')
  exit(1)

from load_registration import load_registration
from embargo_appender import extract_handles

# The institution for which the script will extract publication identifiers
institution = sys.argv[2]

# The environment where the script runs (e.g., 'dev', 'prod')
environment = sys.argv[1]
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
  log_file = open('../files/logfile-' + institution + '.txt', 'w', encoding="utf-8")
  error_file = open('../files/errorfile-' + institution + '.txt', 'w', encoding="utf-8")
  tz = pytz.timezone('Europe/Oslo')
  with open(f'../files/{embargo_file_name}', newline='') as embargo_file:
    embargo_file_reader = csv.reader(embargo_file, delimiter='|')
    for row in embargo_file_reader:
      found_file = False
      correct_import = True
      brage_file_name = row[1].strip()
      brage_embargo_date = tz.localize(parser.isoparse(row[2].strip()))
      if len(row) == 4:
        id = row[3].strip()
        registration = load_registration(id, environment)
        for file in registration['associatedArtifacts']:
          nva_file_name = file['name']
          if nva_file_name == brage_file_name:
            found_file = True
            nva_embargo_date = None
            delta = None
            if 'embargoDate' in file:
              nva_embargo_date = parser.isoparse(file['embargoDate'])
              delta = nva_embargo_date - brage_embargo_date
            else:
              error_file.write('Missing embargo date in NVA!')
              print('Missing embargo date in NVA!')
            if file['visibleForNonOwner']:
              message = 'Should not be visible for non owner\n'
              error_file.write(message)
              print(message)
              correct_import = False
            if not delta is None and delta.days == 0 and delta.seconds > (60 * 60 * 24):
              message = 'Date incorrect: brage(' + brage_embargo_date.isoformat() + ') vs NVA(' + nva_embargo_date.isoformat() + ')\n'
              error_file.write(message)
              print(message)
              correct_import = False
        if not correct_import and found_file:
          # log handle
          # log manglende publikasjonsId
          error_file.write(row[0] + ' | ' + brage_file_name + ' : Failed\n')
          print(brage_file_name + ' - Failed')
        else:
          message = brage_file_name + ' : OK\n'
          log_file.write(message)
  if not log_file.closed:
    log_file.close()
  if not error_file.closed:
    error_file.close()


if __name__ == '__main__':
  # Function to extract handles from the given bucket for the given institution
  extract_handles(bucket_name, institution)

  # Function to check embargo status in the given environment
  check_embargo(environment)
