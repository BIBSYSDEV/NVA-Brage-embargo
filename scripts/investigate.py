import csv
import requests
import sys
import urllib.parse

def investigate():
	filename = sys.argv[1]

	with open(filename, newline='') as embargo_file:
		embargo_file_reader = csv.reader(embargo_file, delimiter='|')
		for row in embargo_file_reader:
			handle = normalize_handle(row[0])

			print('===' + handle + '===')

			handle_search = 'https://api.test.nva.aws.unit.no/search/resources?aggregation=none&handle=' + urllib.parse.quote_plus(handle) + '&from=0&results=100&order=modifiedDate&sort=desc'
			do_search_and_investigate(handle, handle_search, True)

def do_search_and_investigate(handle, search_uri, should_do_freetext_investigation):
	response = requests.get(search_uri)

	if response.status_code == 200:
		search_response = response.json()
		total_hits = search_response['totalHits']
		# print(str(total_hits) + ' hits in NVA!')

		if total_hits > 0:
			log_matching_publication(handle, search_response['hits'])
		else:
			if should_do_freetext_investigation:
				do_free_text_investigation(handle)
			else:
				print('No matching publication found in NVA!')
	else:
		print('Got status code ' + response.status_code + ' from NVA search!')

def do_free_text_investigation(handle):
	handle_search = 'https://api.test.nva.aws.unit.no/search/resources?aggregation=none&query=' + urllib.parse.quote_plus(handle) + '&from=0&results=100&order=modifiedDate&sort=desc'
	do_search_and_investigate(handle, handle_search, False)

def log_matching_publication(handle, hits):
	found = False
	for hit in hits:
		if 'handle' in hit:
			if (normalize_handle(hit['handle']) == handle):
				found = True
				log_file_details(hit, handle, True)
		
		if not found and 'additionalIdentifiers' in hit:
			for identifier in hit['additionalIdentifiers']:
				if identifier['value'] == handle:
					log_file_details(hit, handle, False)
					found = True

	if not found:
		print('Could not find anything in NVA. Probably a flawed merge during migration...')

def log_file_details(hit, handle, isPrimaryHandle):
	#if (hit['handle']):
	#	print('Handle in NVA: ' + hit['handle'])
	#for additionalIdentifier in hit['additionalIdentifiers']:
	#	print('Additional identifier in NVA: ' + additionalIdentifier['value'] + '(' + additionalIdentifier['sourceName'] + ')')
	print('NVA result: ' + hit['id'])
	if isPrimaryHandle:
		print('Result has primary handle!')
	else:
		print('Result does not have primary handle! Probably merged with result from another instance. Check handle: ' + hit['handle'])
	
	if 'associatedArtifacts' in hit:
		file_found = False
		for artifact in hit['associatedArtifacts']:
			if (artifact['type'] == 'PublishedFile'):
				file_found = True
				if ('embargoDate' in artifact):
					print(artifact['name'] + ' has embargo date ' + artifact['embargoDate'])
				else:
					print(artifact['name'] + ' has no embargo date')
		if not file_found:
			print('No published files found in NVA!')
	else:
		print('Result has no artifacts in NVA!')

def normalize_handle(handle):
	if handle.startswith('http://'):
		return 'https://' + handle[7:]

	return handle

if __name__ == '__main__':
	investigate()