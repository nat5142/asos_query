################################################################################
# metar.py - Created 2018/03/12                                                #
#                                                                              #
# Author: Nicholas Tulli                                                       #
#                                                                              #
# This file contains the Metar and Stations classes, which are used to process #
# asos-fivemin files collected from NCDC servers via ftp.                      #
#                                                                              #
# Created for Python 3.6.2                                                     #
################################################################################

# ------- MODULE LIBRARY ------- #
import re
import os
import mysql.connector
import json
# ------------------------------ #

class Metar:
	'''
		A Metar object contains the entirety of a text file of metar surface
		observations. 

		Attributes:
			- self.filename: the name of a text file of metar surface
							 observations
			- self.filepath: the path to the downloaded metar file
			- self.station: a 4-character ICAO identifier string, which is
						 	matched from the downloaded filename
			- self.tokens: database access credentials in JSON format
			- self.database: the name of a database for which the contents of
							 the metar file are to be inserted
			- self.records: a list where each element contains a line from
							the corresponding text file.
			- self.regex: a dictionary containing regular expression objects
						  that must be used to match the contents of each 
						  line in `self.records`
		
		Methods:
			- __open: opens a database connection
			- __close: closes a database connection
			- parse_file: loops over each line in a file and collects data
						  matching regular expression objects in self.regex
			- insertRecords: insert each line of the file into the specified
							 database
	'''

	def __init__(self, file, filepath, databaseTokens=None, db=None):
		self.tokens = databaseTokens
		self.database = db
		self.filepath = filepath
		self.filename = file
		with open(filepath+file) as f:
			self.records = [line.strip() for line in f.readlines()]
		'''
			Defined in dictionary self.regex:
				- stationRE: designed to match the ICAO ID found in filename
				- zdatetimeRE: designed to find UTC timestamp in line
				- ldatetimeRE: designed to find local timestamp in line
				- windRE: desgined to find wind parameters in line
				- tempRE: designed to find temperature data in line
		'''
		self.regex = {
			'stationRE': re.compile('\d{5}(?P<station>\w{4})'),
			'zdatetimeRE': re.compile('\s+(?P<utc_day>0[1-9]|[12][0-9]|3[01])(?P<utc_hour>[01][0-9]|2[0-3])(?P<utc_min>[0-5][0-9])Z'),
			'ldatetimeRE': re.compile('\s+\w{3}(?P<lyear>\d{4})(?P<lmonth>\d{2})(?P<lday>\d{2})(?P<lhour>\d{2})(?P<lmin>\d{2})'),
			'windRE': re.compile('(?P<wdir>[0-2][0-9]{2}|3[0-6][0-9]|VRB)(?P<wspd>\d{2,3})(?:G(?P<wgust>\d{2}))?KT$'),
			'tempRE': re.compile('(?P<temp>M\d{1,2}|[0-5][0-9])\/(?P<dew>M\d{1,2}|[0-5][0-9])?$')
		}
		self.station = self.regex['stationRE'].match(file).group('station')


	def __open(self):
		if self.tokens == None:
			print('Database tokens not provided.')
			return
		if self.database == None:
			print('Database not specified.')
			return

		cnx = mysql.connector.connect(user=self.tokens['root']['user'],
							password=self.tokens['root']['password'],
							host=self.tokens['root']['host'],
							database=self.database)

		curs = cnx.cursor(dictionary=True)

		return cnx, curs


	def __close(self, cnx, curs):
		curs.close()
		cnx.close()

		return


	def parse_file(self):

		parsedRecords = []
		
		for index, record in enumerate(self.records):
			linerecord = {
					'station_id': None,
					'ldatetime': None,
					'zdatetime': None,
					'wspd': None,
					'wdir': None,
					'wgust': None,
					'vrb': None,
					'temp': None,
					'dew': None
				}
			zdatetime = self.regex['zdatetimeRE'].search(record)
			ldatetime = self.regex['ldatetimeRE'].search(record)
			if zdatetime and ldatetime:
				zdt = zdatetime.groupdict()
				ldt = ldatetime.groupdict()

				linerecord['ldatetime'] = "%s-%s-%s %s:%s" % (ldt['lyear'], ldt['lmonth'], ldt['lday'], ldt['lhour'], ldt['lmin'])

				if zdt['utc_day'] >= ldt['lday']:
					linerecord['zdatetime'] = "%s-%s-%s %s:%s" % (ldt['lyear'], ldt['lmonth'], zdt['utc_day'], zdt['utc_hour'], zdt['utc_min'])
				else:
					if int(ldt['lday']) == 31 and int(ldt['lmonth']) == 12:
						linerecord['zdatetime'] = "%s-%s-%s %s:%s" % (int(ldt['lyear'])+1, '01', '01', zdt['utc_hour'], zdt['utc_min'])
					else:
						linerecord['zdatetime'] = "%s-%s-%s %s:%s" % (ldt['lyear'],int(ldt['lmonth'])+1,zdt['utc_day'],zdt['utc_hour'],zdt['utc_min'])
			else:
				print(index)

			for token in re.split('\s', record):
				temp = self.regex['tempRE'].match(token)
				if temp:
					temps = temp.groupdict()
					for key, value in temps.items():
						if value:
							if re.match('^\d+', value):
								temps[key] = int(value)
							if re.match('^M\d+', value):
								temps[key] = -int(value[1:])
					linerecord.update(temps)

				wind = self.regex['windRE'].match(token)
				if wind:
					if wind.group('wdir') == 'VRB':
						linerecord['vrb'] = 'VRB'
						linerecord['wdir'] = None
					else:
						linerecord['wdir'] = wind.group('wdir')
						linerecord['vrb'] = None
					linerecord['wspd'] = wind.group('wspd')

					if wind.group('wgust'):
						linerecord['wgust'] = wind.group('wgust')
					else:
						linerecord['wgust'] = None

			linerecord['station_id'] = self.station_id

			parsedRecords.append(linerecord)
		
		return parsedRecords


	def insertRecords(self, recordList, table):
		connect, cursor = self.__open()
		insert = '''INSERT INTO %s ''' % (table)
		insert += '''(stationID, zdatetime, ldatetime, temp, dew, wspd, wdir, wgust, vrb) \
					VALUES(%(station_id)s, %(zdatetime)s, %(ldatetime)s, %(temp)s, %(dew)s, %(wspd)s, %(wdir)s, %(wgust)s, %(vrb)s)'''
		errors = []

		for count, record in enumerate(recordList):
			try:
				cursor.execute(insert, record)
			except Exception as err:
				error_log = {
						'type': str(type(err).__name__),
						'description': str(err),
						'data': record,
						'file': self.filename,
						'station': self.station,
						'line': count,
						'index': len(errors)
					}
				errors.append(error_log)

		connect.commit()

		if errors:
			with open('./errlogs/' + self.filename[:-4] + '.json', 'w+') as jsonfile:
				jsonfile.write(json.dumps(errors))

		self.__close(connect, cursor)

		return



# ----------------------------------------------------------------------------------------------------------------------- #



# ----------------------------------------------------------------------------------------------------------------------- #

class Stations:

	def __init__(self, databaseTokens, database):
		cnx = mysql.connector.connect(user=databaseTokens['root']['user'],
						host=databaseTokens['root']['host'],
						database=database,
						password=databaseTokens['root']['password'])
		cursor = cnx.cursor(dictionary=True)
		select = '''SELECT * FROM stations'''
		cursor.execute(select)
		self.stations = cursor.fetchall()
		cursor.close()
		cnx.close()


# ----------------------------------------------------------------------------------------------------------------------- #














