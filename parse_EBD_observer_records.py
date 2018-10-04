#parse eBird Records to link with NCBT sites
#Scott Anderson Jun 6, 2017
#NC Wildlife Resources Commission
#scott.anderson@ncwildlife.org
#python 2.7

# Parse through eBird Basic Dataset records and retrieve specified records matching passed observer file/list
# inputs:
#		spp = species list
#		yr = year list
#		st = 2-letter state code list

import os
import sys
import datetime
import bird_codes
import time
import math
from collections import OrderedDict

params = {'obs':[], 'yrs':[],'yrsall':[],'spp':[],'sppsci':[], 'st':[], 'stall':[], 'cnty':[], 'cntyall':[]}
rundt = str(datetime.datetime.now().strftime('%Y%m%d'))

ebird_delim = '\t' 
delim = '~'
out_delim = ','
fn_delim = '_' #delim for filename
nl = '\n'
rootdir = ''
fn = ''

# updated 9/7/18
uid_col = 0 #GLOBAL UNIQUE IDENTIFIER
state_col = 15 #STATE CODE column
sciname_col = 5 #SCIENTIFIC NAME column
cnty_col = 16 #COUNTY NAME column
cnty_code_col = 17 #COUNTY CODE column
id_col = 0 #GLOBAL UNIQUE IDENTIFIER column
obs_col = 29 #OBSERVER ID column
lat_col = 25 #LATITUDE
lon_col = 26 #LONGITUDE
checklist_col = 30 #SAMPLING EVENT IDENTIFIER
date_col = 27 #DATE column

############################################################
############################################################
# QUERY PARAMETERS
params['spp'] = []
params['yrs'] = []
params['st'] = []
params['cnty'] = []
# ebirobs_fn = 'C:\\data\\@nc_birding_trail\\ebird\\results\\20180925_2017_NC_ebd_obs.csv' #observer file - blank out above criteria if want global data
obs_fn = 'C:\\data\\@nc_birding_trail\\ebird\\results\\20181003_2017_NC_Dare_Tyrell_Washington_Hyde_Beaufort_Currituck_10_ebd_obs.csv' #observer file - blank out above criteria if want global data

############################################################
############################################################


# to be used to determine rough distance from point using simple trig
lat_dist = 0.003
lon_dist = 0.004

# These fields are the keys for the relevant files
check_key = 'SAMPLING EVENT IDENTIFIER'
spp_key = 'SCIENTIFIC NAME'
obs_key = 'OBSERVER ID'

folder = 'C:\\data\\@nc_birding_trail\\ebird\\'

# check_fields = ['COUNTRY','COUNTRY CODE','STATE','STATE CODE','COUNTY','COUNTY CODE','IBA CODE','BCR CODE','USFWS CODE','ATLAS BLOCK','LOCALITY','LOCALITY ID',' LOCALITY TYPE','LATITUDE','LONGITUDE','OBSERVATION DATE','TIME OBSERVATIONS STARTED','OBSERVER ID','SAMPLING EVENT IDENTIFIER','PROTOCOL TYPE','PROTOCOL CODE','PROJECT CODE','DURATION MINUTES','EFFORT DISTANCE KM','EFFORT AREA HA','NUMBER OBSERVERS','ALL SPECIES REPORTED','GROUP IDENTIFIER','HAS MEDIA','APPROVED','REVIEWED','REASON','TRIP COMMENTS','SPECIES COMMENTS'] #list of checklist column headers - OLD

#list of checklist column headers - Added 9/7/18
check_fields = ['GLOBAL UNIQUE IDENTIFIER','LAST EDITED DATE','TAXONOMIC ORDER','CATEGORY','COMMON NAME','SCIENTIFIC NAME','SUBSPECIES COMMON NAME','SUBSPECIES SCIENTIFIC NAME','OBSERVATION COUNT','BREEDING BIRD ATLAS CODE','BREEDING BIRD ATLAS CATEGORY','AGE/SEX','COUNTRY','COUNTRY CODE','STATE','STATE CODE','COUNTY','COUNTY CODE','IBA CODE','BCR CODE','USFWS CODE','ATLAS BLOCK','LOCALITY','LOCALITY ID',' LOCALITY TYPE','LATITUDE','LONGITUDE','OBSERVATION DATE','TIME OBSERVATIONS STARTED','OBSERVER ID','SAMPLING EVENT IDENTIFIER','PROTOCOL TYPE','PROTOCOL CODE','PROJECT CODE','DURATION MINUTES','EFFORT DISTANCE KM','EFFORT AREA HA','NUMBER OBSERVERS','ALL SPECIES REPORTED','GROUP IDENTIFIER','HAS MEDIA','APPROVED','REVIEWED','REASON','TRIP COMMENTS','SPECIES COMMENTS']


obs_fields = ['OBSERVER ID','FIRST NAME','LAST NAME'] #list of observer column headers
spp_fields = ['GLOBAL UNIQUE IDENTIFIER','SCIENTIFIC NAME','OBSERVATION COUNT','BREEDING BIRD ATLAS CODE','BREEDING BIRD ATLAS CATEGORY','AGE/SEX','SAMPLING EVENT IDENTIFIER'] #list of species count column headers
spplist_fields = ['TAXONOMIC ORDER','CATEGORY','COMMON NAME','SCIENTIFIC NAME','SUBSPECIES COMMON NAME','SUBSPECIES SCIENTIFIC NAME'] #list of species count column headers
all_fields = []

ebird_rec_ids = []

# boolean markers for identifying if no criteria set
bstall = False
bcntyall = False
byrsall = False
bsppall = False
bobsall = False

us_states = {'AK':['US-AK','United States','US','Alaska'],'AL':['US-AL','United States','US','Alabama'],'AR':['US-AR','United States','US','Arkansas'],'AZ':['US-AZ','United States','US','Arizona'],'CA':['US-CA','United States','US','California'],'CO':['US-CO','United States','US','Colorado'],'CT':['US-CT','United States','US','Connecticut'],'DC':['US-DC','United States','US','District of Columbia'],'DE':['US-DE','United States','US','Delaware'],'FL':['US-FL','United States','US','Florida'],'GA':['US-GA','United States','US','Georgia'],'HI':['US-HI','United States','US','Hawaii'],'IA':['US-IA','United States','US','Iowa'],'ID':['US-ID','United States','US','Idaho'],'IL':['US-IL','United States','US','Illinois'],'IN':['US-IN','United States','US','Indiana'],'KS':['US-KS','United States','US','Kansas'],'KY':['US-KY','United States','US','Kentucky'],'LA':['US-LA','United States','US','Louisiana'],'MA':['US-MA','United States','US','Massachusetts'],'MD':['US-MD','United States','US','Maryland'],'ME':['US-ME','United States','US','Maine'],'MI':['US-MI','United States','US','Michigan'],'MN':['US-MN','United States','US','Minnesota'],'MO':['US-MO','United States','US','Missouri'],'MS':['US-MS','United States','US','Mississippi'],'MT':['US-MT','United States','US','Montana'],'NC':['US-NC','United States','US','North Carolina'],'ND':['US-ND','United States','US','North Dakota'],'NE':['US-NE','United States','US','Nebraska'],'NH':['US-NH','United States','US','New Hampshire'],'NJ':['US-NJ','United States','US','New Jersey'],'NM':['US-NM','United States','US','New Mexico'],'NV':['US-NV','United States','US','Nevada'],'NY':['US-NY','United States','US','New York'],'OH':['US-OH','United States','US','Ohio'],'OK':['US-OK','United States','US','Oklahoma'],'OR':['US-OR','United States','US','Oregon'],'PA':['US-PA','United States','US','Pennsylvania'],'RI':['US-RI','United States','US','Rhode Island'],'SC':['US-SC','United States','US','South Carolina'],'SD':['US-SD','United States','US','South Dakota'],'TN':['US-TN','United States','US','Tennessee'],'TX':['US-TX','United States','US','Texas'],'UT':['US-UT','United States','US','Utah'],'VA':['US-VA','United States','US','Virginia'],'VT':['US-VT','United States','US','Vermont'],'WA':['US-WA','United States','US','Washington'],'WI':['US-WI','United States','US','Wisconsin'],'WV':['US-WV','United States','US','West Virginia'],'WY':['US-WY','United States','US','Wyoming']}
us_state_abbr = us_states.iterkeys()



print '-parameters set'

def error_text():
	############################################################
	#print error text if no valid criteria passed - prevent returning ALL files
	print nl + '=============================================' + nl + 'please enter variables in the following format:' + nl
	print '-years-' + nl + 'yrs=2002 or yrs=2003,2005 or yrs=2003-2010' + nl
	print '-species-' + nl + 'spp=BACS or spp=BACS,WOTH,AMRE' + nl
	print '-state-' + nl + 'st=NC or st=NC,VA,SC'
	print '-county-' + nl + 'cnty=Santa_Cruz or cnty=Santa_Cruz,Orange,Wake'
	print '=============================================' + nl + '-execution halted-' + nl
	sys.exit()

def slugify(t):
	return t.replace(' ','_')

def get_obs_keys(l):
	# use passed filename to retrieve observer keys, and return an array
	return_array = []
	print nl + '== retrieving obs keys ==' + nl + '- "' + l + '"'
	obscount = 1
	with open(l) as f:
		for line in f:
			r = line.split(',')
			obs = r[0].strip('\n')
			if obs != 'OBSERVER ID':
				return_array.append(obs)
				obscount += 1
	print nl + '- ' + str(obscount) + ' observer keys found -' + nl + str(return_array)
	return return_array


def load_cmd_args():
	############################################################
	#retrieve command line arguments
	#	arg 1 = program file
	#	arg 2 = file to be processed
	#other arg syntax examples:
	#	yrs=2002,2003
	#	yrs=2002-2010
	#	spp=BACS,RUGR
	#	st=NC,VA,SC

	print nl+'-looping through command line arguments'

	
	############################################################
	# DEFINE global variables - used to indicate if no criteria for variable, trumps individual booleans below
	global bstall
	global byrsall
	global bsppall
	global bcntyall
	global rootdir
	global fn #might use this later to put results in a subfolder where the this file resides

	fn = 'D:\\ebird_data\\ebd_relAug-2018\\ebd_relAug-2018.txt' #FULL DATA FILE FOR INPUT
	# fn = 'C:\\data\\@nc_birding_trail\\ebird\\sample_file\\ebd_sampling_relAug-2018.txt' #SAMPLE FILE

	#################################
	#populate scientific names in parameters
	print nl + '-populating species'
	if len(params['spp'])==0:
		params['sppsci'].append('all')
		bsppall = True
	else:
		for i in params['spp']:
			params['sppsci'].append(bird_codes.lookup_spp4(i)['SCINAME'])

	print str(params['sppsci'])

	#################################
	#populate all years in parameters
	print nl + '-populating years'
	yr_in = params['yrs']

	if len(params['yrs'])==0:
		params['yrsall'].append('all')
		byrsall = True
	elif '-' in yr_in[0]: #should represent a range of years
		print 'range detected'
		yr_range = yr_in[0].split('-')
		for y in range(int(yr_range[0]),int(yr_range[1])+1):
			params['yrsall'].append(str(y))
	else: #one year or comma separated list
		for y in yr_in:
			params['yrsall'].append(str(y))

	print str(params['yrsall'])

	#################################
	#populate all states in parameters
	print nl + '-populating states'
	if len(params['st'])==0:
		params['stall'].append('all')
		bstall = True
	else:
		for i in params['st']:
			print 'st: ' + str(i)
			if i in us_state_abbr:
				params['stall'].append(us_states[i][0])
			else:
				params['stall'].append(i)


	print str(params['stall'])

	#################################
	#populate all counties in parameters
	print nl + '-populating county'
	if len(params['cnty'])==0:
		params['cntyall'].append('all')
		bcntyall = True
	else:
		for i in params['cnty']:
			params['cntyall'].append(i.replace('_',' '))

	print str(params['cntyall'])

	
def main():
	load_cmd_args()


	##############################################################################
	# LOAD Observers

	# observers = get_obs_keys(obs_fn)

	# observers = ['obsr345959', 'obsr653947', 'obsr324648', 'obsr414347', 'obsr221242', 'obsr34276', 'obsr443134', 'obsr378451', 'obsr592285', 'obsr277612', 'obsr163269', 'obsr412839', 'obsr195764', 'obsr681642', 'obsr194026', 'obsr548917', 'obsr138514', 'obsr291983', 'obsr41940', 'obsr431133', 'obsr677114', 'obsr894235', 'obsr542151', 'obsr86766', 'obsr527452', 'obsr298867', 'obsr494951', 'obsr626362', 'obsr616689', 'obsr48106', 'obsr773300', 'obsr566320', 'obsr639424', 'obsr213564', 'obsr659344', 'obsr356212', 'obsr550951', 'obsr549133', 'obsr279034', 'obsr530386', 'obsr543005', 'obsr106010', 'obsr723168', 'obsr348767', 'obsr616682', 'obsr153726', 'obsr448261', 'obsr972774', 'obsr16673', 'obsr753385', 'obsr1002310', 'obsr33814', 'obsr123898', 'obsr943873', 'obsr526743', 'obsr644562', 'obsr333452', 'obsr2417', 'obsr527245', 'obsr236430', 'obsr20382', 'obsr344616', 'obsr791599', 'obsr307210', 'obsr563626', 'obsr577929', 'obsr291728', 'obsr168148', 'obsr542845', 'obsr537376', 'obsr682333', 'obsr666843', 'obsr334150', 'obsr442493', 'obsr547676', 'obsr899553', 'obsr299743', 'obsr385469', 'obsr125223', 'obsr1072417', 'obsr681847', 'obsr357936', 'obsr630761', 'obsr676771', 'obsr223391', 'obsr774736', 'obsr291141', 'obsr303231', 'obsr435764', 'obsr246672', 'obsr969971', 'obsr5126', 'obsr969945', 'obsr536630', 'obsr967379', 'obsr744914', 'obsr548913', 'obsr813560', 'obsr140008', 'obsr553836', 'obsr344635', 'obsr509154', 'obsr306884', 'obsr325481', 'obsr22584', 'obsr567246', 'obsr669012', 'obsr287476', 'obsr295995', 'obsr560611', 'obsr254786', 'obsr399346', 'obsr862326', 'obsr525643', 'obsr388241', 'obsr675047', 'obsr201168', 'obsr655991', 'obsr973253', 'obsr112729', 'obsr527783', 'obsr304921', 'obsr466881', 'obsr971482', 'obsr395012', 'obsr162227', 'obsr678124', 'obsr799465']
	observers = ['obsr345959', 'obsr653947', 'obsr324648', 'obsr414347', 'obsr221242', 'obsr34276', 'obsr443134']
	# observers = ['obsr345959', 'obsr653947', 'obsr324648'] # testint with fewer observers

	##############################################################################
	# TEMP - build list of NC County Codes

	# nccounties_fn = folder + '\\nc_counties.txt'
	# # nccty = open(nccounties_fn,'r')
	# nc_cty = {}

	# with open(nccounties_fn) as f:
	# 	for line in f:
	# 		r = line.split(',')
	# 		nc_cty[r[3][:-1]] = 'US-NC-' + str(r[2])
	#
	# print nc_cty
	##############################################################################

	##############################################################################
	# setup output files
	# print nl
	# print '- folder: ' + str(folder)
	# print '- Run Date: ' + str([rundt])
	# print '- years: ' + str(params['yrs'])
	# print '- species: ' + str(params['spp'])
	# print '- state: ' + str(params['st'])
	# print '- county: ' + str(params['cnty'])
	# print ['ebdobs_checklists.csv']
	results_check_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ebdobs_checklists.csv'])
	err_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ncbt_err_log.csv'])

	# DISABLE FOR NOW - bogs down computation when large # of species found
	# results_spp_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ebdobs_spp_count.csv'])
	# results_spplist_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ebdobs_spplist.csv'])

	out_check = open(results_check_fn,'w')
	err = open(err_fn,'w')

	# DISABLE FOR NOW - bogs down computation when large # of species found
	# out_spplist = open(results_spplist_fn,'w')
	# out_spp = open(results_spp_fn,'w')
	
	print nl + '-looping through source file'

	#################################
	# Set needed variables before starting loop
	count = 1
	recsfound = 0

	byrs = False
	bcnty = False
	bspp = False
	bobs = False
	bst = False

	# These will store relevant row data
	check_data = OrderedDict()
	spp_data = OrderedDict()
	spplist_data = OrderedDict()
	# obs_data = OrderedDict()
	row_data = OrderedDict()

	# keep a list of keys to reduce redundant records
	check_keys = []
	spp_keys = []
	# obs_keys = []

	###############################################################
	# OPEN source file, loop through lines
	# LOOP through observers 5 at a time? This might cut down on the bog...

	obsperloop = 3
	totobs = len(observers) #total number of observers
	numloops = math.ceil(float(totobs)/float(obsperloop))
	obsindlower = 0
	loopcounter = 1
	print nl + '== ' + str(totobs) + ' observers - ' + str(numloops) + ' loops required ==' + nl


	while loopcounter<=numloops:
		obsindupper = obsindlower + obsperloop
		print nl + '===========================================' + nl + 'Loop ' + str(loopcounter) + nl
		print nl + 'loop number ' + str(loopcounter)
		print nl + 'obsindlower ' + str(obsindlower)
		print nl + 'obsindupper ' + str(obsindupper)
		
		if loopcounter == numloops:
			print nl + str(observers[obsindlower:])
			loopobservers = observers[obsindlower:]
		else: 
			print nl + str(observers[obsindlower:obsindupper])
			loopobservers = observers[obsindlower:obsindupper]





		obsindlower = obsindupper
		loopcounter += 1

	print nl + '== END TEST LOOP =='

	start = time.clock()
	with open(fn) as f:
		for line in f:
			#remove commas
			line = line.replace(out_delim,'').replace(ebird_delim+'\n','').replace("'",'') #remove final delimiter
			# print str(line)
			r = line.split(ebird_delim) #split by EBD delimiter (tab)
			
			###############################################################
			# HEADER LINE
			if count ==1:
				
				###############################################################
				# populate header dict, write headers to output files
				# then add columns to appropriate files
				
				# populate all_fields array to hold header values
				all_fields = r

				# err.write(out_delim.join(['RECORD ADDED','GLOBAL ID','CHECKLIST ID','OBSERVER','SCIENTIFIC NAME','YEAR','STATE'])) #TESTING
				err.write(out_delim.join(all_fields)) #TESTING
				out_check.write(out_delim.join(check_fields))
				
				# DISABLE FOR NOW - bogs down computation when large # of species found
				# out_spp.write(out_delim.join(spp_fields))
				# out_spplist.write(out_delim.join(spplist_fields))


			###############################################################
			# ALL OTHER LINES
			# elif count<150000000: pass #testing
			# elif len(line)<1:
			# 	pass #skip blank lines #UNNECESSARY?
			else:
				# print nl + 'line len: ' + str(len(line)) #TESTING

				#booleans for evaluating three criteria - if 'all', trumps individual criteria
				byrs = byrsall
				bspp = bsppall 
				bst = bstall
				bcnty = bcntyall

				# print nl + 'yrs: ' + str(byrs) + ' : ' + str(byrsall) + ' : ' + str(r[date_col][:4]) #TESTING
				if str(r[obs_col]) in observers:
					bobs = True
					# print '++yrs: ' + str(byrs) + ' : ' + str(byrsall) + ' : ' + str(r[date_col][:4]) #TESTING
					
					#if observer found, perform other tests...
					# print nl + 'yrs: ' + str(byrs) + ' : ' + str(byrsall) + ' : ' + str(r[date_col][:4]) #TESTING
					if str(r[date_col][:4]) in params['yrsall']:
						byrs = True
						# print '++yrs: ' + str(byrs) + ' : ' + str(byrsall) + ' : ' + str(r[date_col][:4]) #TESTING
					
					# print nl + 'spp: ' + str(bspp) + ' : ' + str(bsppall) +  ' : ' + str(r[sciname_col]) #TESTING
					if r[sciname_col] in params['sppsci']:
						bspp = True
						# print '++spp: ' + str(bspp) + ' : ' + str(bsppall) +  ' : ' + str(r[sciname_col]) #TESTING
					
					# print nl + 'state: ' + str(bst) + ' : ' + str(bstall) +  ' : ' + str(r[state_col]) #TESTING
					if r[state_col] in params['stall']:
						bst = True
						# print '++state: ' + str(bst) + ' : ' + str(bstall) +  ' : ' + str(r[state_col]) #TESTING
					
					# print nl + 'county: ' + str(bcnty) + ' : ' + str(bcntyall) +  ' : ' + str(r[state_col]) #TESTING
					if r[cnty_col] in params['cntyall']:
						bcnty = True
						# print '++county: ' + str(bcnty) + ' : ' + str(bcntyall) +  ' : ' + str(r[cnty_col]) #TESTING
				

				#####################################################################################
				## Create dictionaries with header fields as keys and current row data as values
				if byrs and bst and bcnty and bspp and bobs: #are all criteria met?

					row_data = OrderedDict([(all_fields[i],d) for i,d in enumerate(r)])

					err.write(nl + '== ' + str(r[id_col]) + ' Record Saved ==' + nl)

					# assemble subset of full data row for each output file
					for i,d in row_data.items():
						if i in check_fields:
							check_data[i] = d
	
						# DISABLE FOR NOW - bogs down computation when large # of species found
						# if i in spp_fields:
						# 	spp_data[i] = d
	
						# DISABLE FOR NOW - bogs down computation when large # of species found
						# if i in spplist_fields:
						# 	spplist_data[i] = d

					#####################################################################################
					## check to see if row contains duplicate data, output to relevant file if not
					# print str(row_data)
					if row_data[check_key] not in check_keys: #new checklist
						out_check.write(nl+out_delim.join(check_data.values()))
						check_keys.append(row_data[check_key])

					# DISABLE FOR NOW - bogs down computation when large # of species found
					# if row_data[spp_key] not in spp_keys:
					# 	out_spp.write(nl+out_delim.join(spp_data.values()))
					# 	spp_keys.append(row_data[spp_key])
					
					# DISABLE FOR NOW - bogs down computation when large # of species found
					# ALWAYS OUTPUT SPECIES ID
					# out_spplist.write(nl+out_delim.join(spplist_data.values()))
					
					## found records counter
					recsfound += 1
					
			#Keeping count
			count +=1
			count_prop = count % 100000
			count_prop2 = count % 1000000
			if count_prop==0: 
				nowt = time.clock()
				print nl + '== ' + str(count) + ' records evaluated - ' + str(recsfound) +' found =='
				# print '== ' + 'check_key count: ' + str(len(check_keys)) + ' - ' + 'obs_key count: ' + str(len(obs_keys)) + ' - ' + 'spp_key count: ' + str(len(spp_keys)) + ' =='
				print '== ' + 'check_key count: ' + str(len(check_keys)) + ' - ' + 'spp_key count: ' + str(len(spp_keys)) + ' =='
				print '== ' + 'loop time: ' + str(round(nowt - start,1)) + 's ==' + nl
				# print '== ' + nl + str(r) + nl + '==' + nl
			 # 	print '== ' + ' | '.join([str(r[uid_col]), r[date_col], r[date_col][:4], r[state_col], r[cnty_col], r[sciname_col]] ) + ' ==' + nl
			 	# if count >1:
					# print nl + 'yrs: ' + str(byrs) + ' : ' + str(byrsall) + ' : ' + str(r[date_col][:4]) #TESTING
					# print nl + 'spp: ' + str(bspp) + ' : ' + str(bsppall) +  ' : ' + str(r[sciname_col]) #TESTING
					# print nl + 'state: ' + str(bst) + ' : ' + str(bstall) +  ' : ' + str(r[state_col]) #TESTING
					# print nl + 'county: ' + str(bcnty) + ' : ' + str(bcntyall) +  ' : ' + str(r[state_col]) #TESTING
				start = nowt
								
			if count>50000: break #TESTING
			# if count<10000000:
			# 	# err.write(out_delim.join(r) + nl)
			# 	break
	close(fn)		

	print 'search completed. ' + str(count) + ' lines evaluated, ' + str(recsfound) + ' matching records found.' + nl
	print '== ' + 'output file: ' + str(fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty'])) + ' ==' + nl

if __name__ == '__main__':
	main()