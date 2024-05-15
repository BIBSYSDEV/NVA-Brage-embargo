import csv, datetime, pytz, sys
from dateutil import parser

# Name of the S3 bucket to extract identifiers from

if (len(sys.argv) < 3):
  print ('Usage: check_embargo.py <env> <inst>')
  exit(1)

from load_registration import load_registration
from embargo_appender import extract_handles

# The institution for which the script will extract publication identifiers
institution = sys.argv[2]

embargo_file_name = f'../files/embargo_files_{institution}.csv'

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
      check_embargo_file(row, log_file, error_file, tz)

  if not log_file.closed:
    log_file.close()
  if not error_file.closed:
    error_file.close()

def sanitize_date(input_date, tz):
  idx_of_first_dash = input_date.find('-')
  if idx_of_first_dash > 4:
    input_date = '9999' + input_date[idx_of_first_dash:]
  return tz.localize(parser.isoparse(input_date))

def check_embargo_file(row, log_file, error_file, tz):
  found_file = False
  correct_import = True
  handle = row[0].strip()
  brage_file_name = row[1].strip()
  raw_embargo_date = row[2].strip()
  # print('Checking file ' + brage_file_name + ' for handle ' + row[0] + '\n')
  brage_embargo_date = sanitize_date(raw_embargo_date, tz)
  if len(row) == 4:
    id = row[3].strip()
    registration = load_registration(id, environment)
    if 'associatedArtifacts' in registration:
      for file in registration['associatedArtifacts']:
        if 'type' in file and file['type'] != 'AssociatedLink':
          nva_file_name = file['name']
          if nva_file_name == brage_file_name:
            found_file = True
            nva_embargo_date = None
            delta = None
            if 'embargoDate' in file:
              nva_embargo_date = parser.isoparse(file['embargoDate'])
              delta = nva_embargo_date - brage_embargo_date
            else:
              do_log(error_file, handle, brage_file_name, id, 'Missing embargo date in NVA: ' + id)
            if file['visibleForNonOwner']:
              message = 'Should not be visible for non owner'
              do_log(error_file, handle, brage_file_name, id, message)
              correct_import = False
            if not delta is None and delta.days == 0 and delta.seconds > (60 * 60 * 24):
              message = 'Date incorrect: brage(' + brage_embargo_date.isoformat() + ') vs NVA(' + nva_embargo_date.isoformat() + ')'
              do_log(error_file, handle, brage_file_name, id, message)
              correct_import = False
    else:
      correct_import = False
      message = 'No file migrated!'
      do_log(error_file, handle, brage_file_name, id, message)
    
    if correct_import and found_file:
      do_log(log_file, handle, brage_file_name, id, 'OK!')
    else:
      # log handle
      # log manglende publikasjonsId
      do_log(error_file, handle, brage_file_name, id, 'Failed!')
  else:
    # incomplete row in embargo file
    message = 'Incomplete entry in embargo file: ' + len(row)
    do_log(error_file, handle, brage_file_name, id, message)

def do_log(log_file, handle, brage_file, id, message):
  log_file.write(handle + '|' + brage_file + '|' + id + '|' + message + '\n')

if __name__ == '__main__':
  # Function to extract handles from the given bucket for the given institution
  extract_handles(bucket_name, institution)

  # Function to check embargo status in the given environment
  check_embargo(environment)
