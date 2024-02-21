from load_registration import load_registration
import csv

from scripts.embargo_appender import extract_handles

embargo_file_name = 'embargo_files.csv'

# Name of the S3 bucket to extract identifiers from
bucket_name = 'brage-migration-reports-884807050265'

# The environment where the script runs (e.g., 'dev', 'prod')
environment = 'dev'

# The institution for which the script will extract publication identifiers
institution = 'niku'

def check_embargo(environment):
  log_file = open('../files/logfile.txt', 'w', encoding="utf-8")
  with open(f'../files/{embargo_file_name}', newline='') as embargo_file:
    embargo_file_reader = csv.reader(embargo_file, delimiter='|')
    for row in embargo_file_reader:
      correct_import = True
      brage_file_name = row[1].strip()
      brage_embargo_date = row[2].strip()
      if len(row) == 4:
        id = row[3].strip()
        registration = load_registration(id, environment)
        for file in registration['associatedArtifacts']:
          nva_file_name = file['name']
          if nva_file_name == brage_file_name:
            nva_embargo_date = ''
            if 'embargoDate' in file:
              nva_embargo_date = file['embargoDate'].split('T')[0]
            if file['visibleForNonOwner']:
              print('Should not be visible for non owner')
              correct_import = False
            if nva_embargo_date == brage_embargo_date:
              print('Date correct')
            else:
              print('Date incorrect')
              correct_import = False
        if not correct_import:
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
