# parse eBird Records
# Scott Anderson Jan 29, 2020
# NC Wildlife Resources Commission
# scott.anderson@ncwildlife.org
# python 3.8

# Parse through eBird Basic Dataset records and retrieve specified records
# inputs:
#		spp = species list
#		yr = year list
#		st = 2-letter state code list
#		cnty = full county name
#		mon = month of the year
#
# output files are prefixed with: rundate_yrs_spp_st_cnty_
# outputs:
#		PREFIX_ebd_checklists.csv (unique list of checklists)
# 		PREFIX_ebd_spp_count.csv
# 		PREFIX_ebd_spp.csv (unique list of species)
#
import os
import sys
import datetime
import bird_codes
import time
# from datetime import time
from collections import OrderedDict
params = {'yrs':[],'yrsall':[],'spp':[],'sppsci':[], 'st':[], 'stall':[], 'ctry':[], 'ctryall':[],'cnty':[], 'cntyall':[], 'mon':[], 'monall':[]}

########################################################################################################
########################################################################################################
# QUERY PARAMETERS
#==============================

# SPP EXAMPLES
# params['spp'] = [] # no species limits
# params['spp'] = ['NOCA'] #only records with Northern Cardinal
# params['spp'] = ['NOCA','BLJA'] #records with Northern Cardinal OR Blue Jay
# YRS EXAMPLES
# params['yrs'] = [] # no year limits
# params['Yrs'] = ['2017'] #2017 only
# params['yrs'] = ['2013-2017'] #any year between and including 2013 and 2017
# params['yrs'] = ['2013','2017'] #records from 2013 OR 2017
# ST EXAMPLES
# params['st'] = ['NC'] #only records from NC
# params['st'] = ['NC','VA'] #records from NC OR VA
# CTRY EXAMPLES
# params['ctry'] = ['NC'] #only records from NC
# params['ctry'] = ['NC','VA'] #records from NC OR VA
# CNTY EXAMPLES
# params['cnty'] = ['Dare'] #Dare county only (note that there may be more than one Dare County in the US - should restrict by state)
# params['cnty'] = ['Dare','Tyrell'] #Dare OR Tyrell county records
# MON EXAMPLES
# params['mon'] = ['10'] #Only records from October
# params['mon'] = ['10', '11'] #Records from October OR November

#PARAMETERS
#==============================
params['st'] = ['NC'] # STATE CRITERIA
params['ctry'] = ['US'] # COUNTRY CRITERIA
params['cnty'] = [] # COUNTY CRITERIA
params['mon'] = [] # MONTH CRITERIA
params['spp'] = [] # SPECIES CRITERIA
params['yrs'] = ['2019'] # YEAR CRITERIA
#
########################################################################################################
########################################################################################################

##########################################################################
##########################################################################
# EBIRD DATA FILE LOCATION
data_file_folder = 'ebd_relDec-2020\\'
fn = data_file_folder + 'ebd_relDec-2020.txt'
# OTHER EXAMPLES
# fn = data_file_folder + 'ebd_US-NC_201801_201912_relDec-2019.txt' #NC Data
# fn = data_file_folder + 'results\\20200116_2018_US_ebd_state_US-WA_ebd.txt' #TESTING AK 2018 Data
# fn = data_file_folder + '20200109_2018_US_ebd_subset.txt' #2018 US Subset
# fn = data_file_folder+'ebd_relNov-2019.txt' #FULL DATA FILE

##########################################################################
##########################################################################

########################################################################################################
# FIELD LISTS
# This list is used as a key to all data manipulation. If changes are made to these headings, they must be made throughout the code...
# updated 1/15/20
#'GLOBAL UNIQUE IDENTIFIER','LAST EDITED DATE','TAXONOMIC ORDER','CATEGORY','COMMON NAME','SCIENTIFIC NAME','SUBSPECIES COMMON NAME','SUBSPECIES SCIENTIFIC NAME','OBSERVATION COUNT','BREEDING BIRD ATLAS CODE','BREEDING BIRD ATLAS CATEGORY','AGE/SEX','COUNTRY','COUNTRY CODE','STATE','STATE CODE','COUNTY','COUNTY CODE','IBA CODE','BCR CODE','USFWS CODE','ATLAS BLOCK','LOCALITY','LOCALITY ID','LOCALITY TYPE','LATITUDE','LONGITUDE','OBSERVATION DATE','TIME OBSERVATIONS STARTED','OBSERVER ID','SAMPLING EVENT IDENTIFIER','PROTOCOL TYPE','PROTOCOL CODE','PROJECT CODE','DURATION MINUTES','EFFORT DISTANCE KM','EFFORT AREA HA','NUMBER OBSERVERS','ALL SPECIES REPORTED','GROUP IDENTIFIER','HAS MEDIA','APPROVED','REVIEWED','REASON','TRIP COMMENTS','SPECIES COMMENTS'

# updated 1/15/20
guid_h = 'GLOBAL UNIQUE IDENTIFIER'
state_h = 'STATE CODE'
sciname_h = 'SCIENTIFIC NAME'
obs_h = 'OBSERVER ID'
cnty_h = 'COUNTY'
cnty_code_h = 'COUNTY CODE'
ctry_code_h = 'COUNTRY CODE'
lat_h = 'LATITUDE'
lon_h = 'LONGITUDE'
check_h = 'SAMPLING EVENT IDENTIFIER'
date_h = 'OBSERVATION DATE'
procode_h = 'PROTOCOL CODE'
prjcode_h = 'PROJECT CODE'


########################################################################################################
# IMPORTANT VARIABLES

rundt = str(datetime.datetime.now().strftime('%Y%m%d')) #run date - to be used in file name
ebird_delim = '\t' #delimiter for source eBird file
# ebird_delim = ',' #delimiter for source eBird file
delim = '~'
out_delim = ','
fn_delim = '_' #delim for filename
nl = '\n'

folder = os.path.dirname(os.path.abspath(__file__)) #retrieves the current file location as the base folder

# US States
us_states = {'AK':['US-AK','United States','US','Alaska'],'AL':['US-AL','United States','US','Alabama'],'AR':['US-AR','United States','US','Arkansas'],'AZ':['US-AZ','United States','US','Arizona'],'CA':['US-CA','United States','US','California'],'CO':['US-CO','United States','US','Colorado'],'CT':['US-CT','United States','US','Connecticut'],'DC':['US-DC','United States','US','District of Columbia'],'DE':['US-DE','United States','US','Delaware'],'FL':['US-FL','United States','US','Florida'],'GA':['US-GA','United States','US','Georgia'],'HI':['US-HI','United States','US','Hawaii'],'IA':['US-IA','United States','US','Iowa'],'ID':['US-ID','United States','US','Idaho'],'IL':['US-IL','United States','US','Illinois'],'IN':['US-IN','United States','US','Indiana'],'KS':['US-KS','United States','US','Kansas'],'KY':['US-KY','United States','US','Kentucky'],'LA':['US-LA','United States','US','Louisiana'],'MA':['US-MA','United States','US','Massachusetts'],'MD':['US-MD','United States','US','Maryland'],'ME':['US-ME','United States','US','Maine'],'MI':['US-MI','United States','US','Michigan'],'MN':['US-MN','United States','US','Minnesota'],'MO':['US-MO','United States','US','Missouri'],'MS':['US-MS','United States','US','Mississippi'],'MT':['US-MT','United States','US','Montana'],'NC':['US-NC','United States','US','North Carolina'],'ND':['US-ND','United States','US','North Dakota'],'NE':['US-NE','United States','US','Nebraska'],'NH':['US-NH','United States','US','New Hampshire'],'NJ':['US-NJ','United States','US','New Jersey'],'NM':['US-NM','United States','US','New Mexico'],'NV':['US-NV','United States','US','Nevada'],'NY':['US-NY','United States','US','New York'],'OH':['US-OH','United States','US','Ohio'],'OK':['US-OK','United States','US','Oklahoma'],'OR':['US-OR','United States','US','Oregon'],'PA':['US-PA','United States','US','Pennsylvania'],'RI':['US-RI','United States','US','Rhode Island'],'SC':['US-SC','United States','US','South Carolina'],'SD':['US-SD','United States','US','South Dakota'],'TN':['US-TN','United States','US','Tennessee'],'TX':['US-TX','United States','US','Texas'],'UT':['US-UT','United States','US','Utah'],'VA':['US-VA','United States','US','Virginia'],'VT':['US-VT','United States','US','Vermont'],'WA':['US-WA','United States','US','Washington'],'WI':['US-WI','United States','US','Wisconsin'],'WV':['US-WV','United States','US','West Virginia'],'WY':['US-WY','United States','US','Wyoming']}
us_state_abbr = us_states.keys()

# These determine the fields to output in files
spp_fields = ['TAXONOMIC ORDER','CATEGORY','COMMON NAME','SCIENTIFIC NAME','SUBSPECIES COMMON NAME','SUBSPECIES SCIENTIFIC NAME','STATE CODE'] #list of unique species column headers
spp_count_fields = ['GLOBAL UNIQUE IDENTIFIER','TAXONOMIC ORDER','SCIENTIFIC NAME','COMMON NAME','CATEGORY','OBSERVATION COUNT','BREEDING BIRD ATLAS CODE','STATE CODE','COUNTY','LOCALITY ID','LATITUDE','LONGITUDE','OBSERVATION DATE','OBSERVER ID','SAMPLING EVENT IDENTIFIER','SPECIES COMMENTS']
check_fields = ['LAST EDITED DATE','COUNTRY','COUNTRY CODE','STATE','STATE CODE','COUNTY','COUNTY CODE','IBA CODE','BCR CODE','USFWS CODE','ATLAS BLOCK','LOCALITY','LOCALITY ID','LOCALITY TYPE','LATITUDE','LONGITUDE','OBSERVATION DATE','TIME OBSERVATIONS STARTED','OBSERVER ID','SAMPLING EVENT IDENTIFIER','PROTOCOL TYPE','PROTOCOL CODE','PROJECT CODE','DURATION MINUTES','EFFORT DISTANCE KM','EFFORT AREA HA','NUMBER OBSERVERS','ALL SPECIES REPORTED','GROUP IDENTIFIER','HAS MEDIA','APPROVED','REVIEWED','REASON','TRIP COMMENTS'] #list of column headers for checklist file

all_fields = [] #holds headers (first line)

# boolean markers for identifying if no criteria set
bstall = False
bcntyall = False
bctryall = False
byrsall = False
bsppall = False
bmonall = False


print ('-parameters set')
def error_text():
	############################################################
	#print error text if no valid criteria passed - prevent returning ALL files
	print (nl + '=============================================' + nl + 'please enter variables in the following format:' + nl)
	print ('-years-' + nl + 'yrs=2002 or yrs=2003,2005 or yrs=2003-2010' + nl)
	print ('-species-' + nl + 'spp=BACS or spp=BACS,WOTH,AMRE' + nl)
	print ('-state-' + nl + 'st=NC or st=NC,VA,SC'+nl)
	print ('-month-' + nl + 'mon=10 or mon=10,11,12'+nl)
	print ('-county-' + nl + 'cnty=Santa_Cruz or cnty=Santa_Cruz,Orange,Wake'+nl)
	print ('-country-' + nl + 'cntry=US or ctry=US,CA')
	print ('=============================================' + nl + '-execution halted-' + nl)
	sys.exit()

def load_cmd_args():
	#retrieve arguments, populate apprpriate arrays

	print (nl+'-looping through command line arguments')
	# DEFINE global variables - used to indicate if no criteria for variable, trumps individual booleans below
	global bstall
	global byrsall
	global bmonall
	global bsppall
	global bcntyall
	global bctryall
	global data_file_folder
	global fn #might use this later to put results in a subfolder where the this file resides


	#populate scientific names in parameters
	print (nl + '-populating species')
	if len(params['spp'])==0:
		params['sppsci'].append('all')
		bsppall = True
	else:
		for i in params['spp']:
			params['sppsci'].append(bird_codes.lookup_spp4(i)['SCINAME'])
	print (str(params['sppsci']))

	#populate all years in parameters
	print (nl + '-populating years')
	yr_in = params['yrs']
	if len(params['yrs'])==0:
		params['yrsall'].append('all')
		byrsall = True
	elif '-' in yr_in[0]: #should represent a range of years
		print ('range detected')
		yr_range = yr_in[0].split('-')
		for y in range(int(yr_range[0]),int(yr_range[1])+1):
			params['yrsall'].append(str(y))
	else: #one year or comma separated list
		for y in yr_in:
			params['yrsall'].append(str(y))
	print (str(params['yrsall']))

	#populate all states in parameters
	print (nl + '-populating states')
	if len(params['st'])==0:
		params['stall'].append('all')
		bstall = True
	else:
		for i in params['st']:
			print ('st: ' + str(i))
			if i in us_state_abbr:
				params['stall'].append(us_states[i][0])
			else:
				params['stall'].append(i)
	print (str(params['stall']))

	#populate countries in parameters
	print (nl + '-populating country')
	if len(params['ctry'])==0:
		params['ctryall'].append('all')
		bctryall = True
	else:
		for i in params['ctry']:
			params['ctryall'].append(i.replace('_',' '))
	print (str(params['ctryall']))

	#populate counties in parameters
	print (nl + '-populating county')
	if len(params['cnty'])==0:
		params['cntyall'].append('all')
		bcntyall = True
	else:
		for i in params['cnty']:
			params['cntyall'].append(i.replace('_',' '))
	print (str(params['cntyall']))

	#populate all months in parameters
	print (nl + '-populating months')
	if len(params['mon'])==0:
		params['monall'].append('all')
		bmonall = True
	else:
		for i in params['mon']:
			params['monall'].append(i)
	print (str(params['monall']))
	#check to see if minimum parameters present
	if bstall and byrsall and bsppall and bcntyall and bmonall and bctryall:
		print ('----------------------------------'+nl+'You must specify at least one criteria to produce results.'+nl+'----------------------------------')
		error_text()

def main():
	print ('main running...')
	load_cmd_args()

	# setup output files

	#filename includes all parameters and run date
	fn_prefix = fn_delim.join([rundt]+params['yrs']+params['spp']+params['ctry']+params['st']+params['cnty']+params['mon'])

	# setup output files (checklists, species count (all species records), spp (unique list of species))
	results_check_fn = os.path.join(folder,'results\\',fn_delim.join([fn_prefix]+['ebd_checklists.csv']))
	results_spp_count_fn = os.path.join(folder, 'results\\',fn_delim.join([fn_prefix]+['ebd_spp_count.csv']))
	results_spp_fn = os.path.join(folder,'results\\',fn_delim.join([fn_prefix]+['ebd_spp.csv']))
	err_fn = os.path.join(folder,'results\\',fn_delim.join([fn_prefix]+['err_log.txt']))

	out_check = open(results_check_fn,'w', encoding='utf-8')
	out_spp_count = open(results_spp_count_fn,'w', encoding='utf-8')
	out_spp = open(results_spp_fn,'w', encoding='utf-8')

	err = open(err_fn,'w', encoding='utf-8')

	print (nl + '-looping through source file')
	# Set needed variables before starting loop
	count = 1
	recsfound = 0

	byrs = False
	bcnty = False
	bctry = False
	bspp = False
	bst = False

	row_data = OrderedDict()

	#state grouping data
	currCheck = ''
	check_data = OrderedDict()
	spp_count_data = OrderedDict()
	spp_data = OrderedDict()

	# keep a list of keys to reduce redundant records
	# check_keys = []
	check_keys = set() #python set - faster than a list!
	spp_keys = set() #python set - faster than a list!
	# spp_keys = []

	###############################################################
	# OPEN source file, loop through lines
	start = time.perf_counter()
	with open(fn, encoding="utf-8") as f:
		for line in f:
			# print (line) # TESTING
			bNewCheck = False #boolean indicating if current reccord is a new checklist
			# line = line.replace(ebird_delim+'\n','').replace('\n','').replace("'",'') #remove final delimiter
			line = line.replace(out_delim,'').replace(ebird_delim+'\n','').replace('\n','').replace("'",'') #remove final delimiter
			r = line.split(ebird_delim) #split by EBD delimiter (tab)

			###############################################################
			# HEADER LINE
			if count ==1:

				###############################################################
				# populate header dict, write headers to output files
				# then add columns to appropriate files
				# populate all_fields array to hold header values
				all_fields = r
				# print (all_fields)

				#add headers to output files
				out_check.write(out_delim.join(check_fields))
				out_spp_count.write(out_delim.join(spp_count_fields))
				out_spp.write(out_delim.join(spp_fields))
				print (line)
			# ALL OTHER LINES
			else:

				row_data = OrderedDict([(all_fields[i],d.strip()) for i,d in enumerate(r)])


				############################################################################################
				# YRS, ST, CNTY, CTRY, MON do not need to be evaluated if checklist has not changed!
				# Did the checklist change?

				## Is this a new checklist?
				if row_data[check_h] not in check_keys:
					#########################################################################
					## CONFIRMED: checklists lines are NOT grouped!!! (i.e., all species records in a checklist are not grouped)
					currCheck = row_data[check_h] #reset current checklist key
					check_keys.add(currCheck) #add checklist key to set
					# check_keys.append(currCheck) #add checklist key to array

					bNewCheck = True

					#########################################################################
					#booleans for evaluating three criteria - if 'all', trumps individual criteria
					byrs = byrsall
					bst = bstall
					bcnty = bcntyall
					bctry = bctryall
					bmon = bmonall

					## check if line fulfills requirements
					if str(row_data[date_h][:4]) in params['yrsall']:
						byrs = True

					if row_data[ctry_code_h] in params['ctryall']:
						# print('country is true')
						bctry = True

					if row_data[cnty_h] in params['cntyall']:
						bcnty = True

					if str(row_data[date_h][5:7]) in params['monall']:
						bmon = True

					if str(row_data[state_h]) in params['stall']:
						bst = True

				#evaluate species criteria for every line of data
				bspp = bsppall

				# print (row_data)
				if row_data[sciname_h] in params['sppsci']:
					bspp = True

				#####################################################################################
				##  Are all criteria met?
				##	Create dictionaries with header fields as keys and current row data as values
				#####################################################################################
				# print ("yrs:" + str(byrs) + " bst:" + str(bst) + " county: " + str(bcnty) + " species: " + str(bspp) + " month: " + str(bmon) + " country: " + str(bctry) + ".")

				# if count == 20: break

				if byrs and bst and bcnty and bspp and bmon and bctry:
					# print (nl+'====== criteria met =======') #TESTING

					# loop through all fields and populate lists containing file data
					for i,d in row_data.items():

						if bNewCheck: # only output these if new checklist
							if i in check_fields:
								check_data[i] = d

						if i in spp_count_fields:
							spp_count_data[i] = d

						if i in spp_fields:
							spp_data[i] = d

					#####################################################################################
					## check to see if row contains duplicate data, output to relevant file if not

					# CHECKLISTS
					if bNewCheck: #new checklist!

						#output current line as new checklist record
						out_check.write(nl+out_delim.join(check_data.values()))
						# err.write(nl+'CHECKLIST: ' + out_delim.join(check_data.values()))

					# SPECIES - add to the species count
					if row_data['CATEGORY'] not in ['spuh','slash']: # we are not counting groups or slash species as unique spp

						#Check to output species record (unique spp list)
						if row_data[sciname_h] not in spp_keys: #collect unique species in the state
							spp_keys.add(row_data[sciname_h])
							# spp_keys.append(row_data[sciname_h])
							out_spp.write(nl+out_delim.join(spp_data.values()))
							# err.write(nl+'SPECIES: ' + out_delim.join(state_spp_data.values()))

					# SPECIES COUNT record (includ spuh and slash)
					out_spp_count.write(nl+out_delim.join(spp_count_data.values()))

					## found records counter
					recsfound += 1

			#Keeping count, pop up message every million records to show we're still working!
			count +=1
			count_prop = count % 1000000

			if count_prop==0:
				nowt = time.perf_counter()
				print (nl + '== ' + str(count) + ' records evaluated - ' + str(recsfound) +' found - '+ str(len(check_keys)) +' checklists found ==')
				# print (nl + '== ' + str(count) + ' ' + currState + ' records evaluated - ' + str(recsfound) +' found ==')
				#print ('== ' + 'check_key count: ' + str(len(check_keys)) + ' - ' + 'obs_key count: ' + str(len(obs_keys)) + ' - ' + 'spp_key count: ' + str(len(spp_keys)) + ' ==')
				print ('== ' + 'loop time: ' + str(round(nowt - start,1)) + 's ==' + nl)

				start = nowt
			# if count >100: # FOR TESTING
			# 	print ('=== process ended for testing ===')
			# 	break

		# output
		# for i,d in state_summary_stats.items():
		# 	print (i)
		# 	print (state_summary_stats[i])
			# st_data = [str(x) for x in state_summary_stats[i]]
			# line_out = [i] + st_data
			# out_state_summary.write(nl+out_delim.join([i]+st_data))

		##############################################
		# Completed state
		# print ('========================' +nl +'processing ' + currState + ' completed.' + nl + '=================================')



	print ('search completed. ' + str(count) + ' lines evaluated, ' + str(recsfound) + ' matching records found.' + nl)
	print ('== ' + 'output file: ' + str(fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty'])) + ' ==' + nl)

if __name__ == '__main__':
	main()
