################################################################################
# processfiles.py | Created 2018/03/11                                         #
#                                                                              #
# Author: Nicholas Tulli (nat5142 [at] psu [dot] edu)                          #
#                                                                              #
# This file will connect to NOAA NCDC server via ftp, download files from the  #
# server, and process each file using Python classes found in `metar.py`.      #
#                                                                              #
# Developed for Python 3.6                                                     #
################################################################################

# ------- MODULE LIBRARY ------- #
import re
import json
import os 
import ftplib
import subprocess
from metar import Metar, Stations
import atexit
# ------------------------------ #


''' 
	Define exit handler function to ensure progress is saved in the event
	of premature script termination
'''
def exit_handler():
	jsonfile = open('./filedata.json', 'w')
	jsonfile.write(json.dumps(metadata))
	jsonfile.close()
	return

atexit.register(exit_handler)


# load in credentials to access database
tokenpath = '~/Desktop/Code/dbAccess.json'
tokens = os.path.expanduser(tokenpath)
with open(tokens) as db:
	credentials = json.load(db)

'''
	`filedata.json` is a JSON object containing data about files 
	that have been processed. In the event of premature script
	termination, the contents of this file will be used to pick
	up where the script was last terminated.
'''
try:
	with open('./filedata.json', 'r') as d:
		metadata = json.load(d)
except FileNotFoundError:
	metadata = {}

stationList = Stations(credentials, 'nat5142').stations
stationRE = re.compile('\d{5}(?P<station>\w{4})')

directory = '/pub/data/asos-fivemin/6401-%s/'

'''
	Data collected for this project was limited to the past
	4 years (arbitrarily)
'''
for year in range(2014,2018):
	ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
	ftp.login()
	ftp.cwd(directory % (str(year)))
	print(ftp.pwd())
	files = ftp.nlst()
	if str(year) not in metadata.keys():
		metadata[str(year)] = {'count': len(files), 'files': []}
	print(len(files))
	#print([n['name'] for n in metadata[str(year)]['files']])

	for index, filename in enumerate(files):
		match = stationRE.match(filename).group('station')
		if match:
			if match not in [s['id'] for s in stationList]:
				print('Station not in database: %s' % (match))
			elif filename in [n['name'] for n in metadata[str(year)]['files']]:
				print('File already checked -- exists in filedata.json: %s' % (filename))
			else:
				station_id = next((d['stationID'] for d in stationList if d['id'] == match), None)
				print("Downloading %s ----- Index: %s" % (filename, index))
				with open('./%s' % (filename), 'wb') as dload:
					ftp.retrbinary("RETR %s" % (filename), dload.write)
				#print('finished')

				metar = Metar(filename, './', credentials, 'nat5142')
				records = metar.parse_file()
				metar.insertRecords(records, 'metar_records')
				print(len(records))
				print(metar.station)
				metadata[str(year)]['files'].append(
					{'name': filename, 'records': len(records)}
				)

				proc = subprocess.Popen('rm %s' % (filename), shell=True)
	ftp.quit()















































