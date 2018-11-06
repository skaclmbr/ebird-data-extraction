#parse eBird Records to link with NCBT sites
#Scott Anderson Nov 6, 2018
#NC Wildlife Resources Commission
#scott.anderson@ncwildlife.org
#python 2.7

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
# 		PREFIX_ebd_obs.csv (unique list of observers)
# 		PREFIX_ebd_spplist.csv (unique list of species)
#		
import os
import sys
import datetime
import bird_codes
import time
from collections import OrderedDict
params = {'yrs':[],'yrsall':[],'spp':[],'sppsci':[], 'st':[], 'stall':[], 'cnty':[], 'cntyall':[], 'mon':[], 'monall':[]}
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
lat_col = 25 #LATITUDE
lon_col = 26 #LONGITUDE
checklist_col = 30 #SAMPLING EVENT IDENTIFIER
date_col = 27 #DATE column

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
# CNTY EXAMPLES
# params['cnty'] = ['Dare'] #Dare county only (note that there may be more than one Dare County in the US - should restrict by state)
# params['cnty'] = ['Dare','Tyrell'] #Dare OR Tyrell county records
# MON EXAMPLES
# params['mon'] = ['10'] #Only records from October
# params['mon'] = ['10', '11'] #Records from October OR November
#
#PARAMETERS
#==============================
params['st'] = ['NC'] #no state limits
params['cnty'] = ['Wake'] #no county limits
params['mon'] = [] #no month limits
params['spp'] = []
params['yrs'] = ['2013-2017']
#
########################################################################################################
########################################################################################################

# to be used to determine rough distance from point using simple trig
lat_dist = 0.003
lon_dist = 0.004

# These fields are the keys for the relevant files
check_key = 'SAMPLING EVENT IDENTIFIER'
spp_key = 'SCIENTIFIC NAME'
obs_key = 'OBSERVER ID'
folder = 'C:\\data\\@nc_birding_trail\\ebird\\'

#These arrays determine which fields are saved to their respective output files
check_fields = ['GLOBAL UNIQUE IDENTIFIER','LAST EDITED DATE','COUNTRY','COUNTRY CODE','STATE','STATE CODE','COUNTY','COUNTY CODE','IBA CODE','BCR CODE','USFWS CODE','ATLAS BLOCK','LOCALITY','LOCALITY ID',' LOCALITY TYPE','LATITUDE','LONGITUDE','OBSERVATION DATE','TIME OBSERVATIONS STARTED','OBSERVER ID','SAMPLING EVENT IDENTIFIER','PROTOCOL TYPE','PROTOCOL CODE','PROJECT CODE','DURATION MINUTES','EFFORT DISTANCE KM','EFFORT AREA HA','NUMBER OBSERVERS','ALL SPECIES REPORTED','GROUP IDENTIFIER','HAS MEDIA','APPROVED','REVIEWED','REASON','TRIP COMMENTS']
obs_fields = ['OBSERVER ID'] #list of observer column headers
spplist_fields = ['GLOBAL UNIQUE IDENTIFIER','SCIENTIFIC NAME','OBSERVATION COUNT','BREEDING BIRD ATLAS CODE','BREEDING BIRD ATLAS CATEGORY','AGE/SEX','SAMPLING EVENT IDENTIFIER'] #list of species count column headers
spp_fields = ['TAXONOMIC ORDER','CATEGORY','COMMON NAME','SCIENTIFIC NAME','SUBSPECIES COMMON NAME','SUBSPECIES SCIENTIFIC NAME','LATITUDE','LONGITUDE','SAMPLING EVENT IDENTIFIER','OBSERVATION COUNT'] #list of unique species column headers
all_fields = []
ebird_rec_ids = []


# boolean markers for identifying if no criteria set
bstall = False
bcntyall = False
byrsall = False
bsppall = False
bmonall = False

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

def load_cmd_args():
	#retrieve arguments, populate apprpriate arrays

	print nl+'-looping through command line arguments'
	# DEFINE global variables - used to indicate if no criteria for variable, trumps individual booleans below
	global bstall
	global byrsall
	global bmonall
	global bsppall
	global bcntyall
	global rootdir
	global fn #might use this later to put results in a subfolder where the this file resides

	##########################################################################
	##########################################################################
	# eBird Data File Location
	fn = 'D:\\ebird_data\\ebd_relAug-2018\\ebd_relAug-2018.txt' #FULL DATA FILE
	# fn = 'C:\\data\\@nc_birding_trail\\ebird\\sample_file\\ebd_sampling_relAug-2018.txt' #SAMPLE FILE
	# fn = 'C:\\data\\@nc_birding_trail\\ebird\\sample_file\\ebd_US-AL-101_201801_201801_relMay-2018.txt' #sample file
	# fn = 'D:\\ebird_data\\ebd_relMay-2018\\ebd_relMay-2018.txt'
	##########################################################################
	##########################################################################

	#populate scientific names in parameters
	print nl + '-populating species'
	if len(params['spp'])==0:
		params['sppsci'].append('all')
		bsppall = True
	else:
		for i in params['spp']:
			params['sppsci'].append(bird_codes.lookup_spp4(i)['SCINAME'])
	print str(params['sppsci'])

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
	
	#populate counties in parameters
	print nl + '-populating county'
	if len(params['cnty'])==0:
		params['cntyall'].append('all')
		bcntyall = True
	else:
		for i in params['cnty']:
			params['cntyall'].append(i.replace('_',' '))
	print str(params['cntyall'])

	#populate all months in parameters
	print nl + '-populating months'
	if len(params['mon'])==0:
		params['monall'].append('all')
		bmonall = True
	else:	
		for i in params['mon']:
			params['monall'].append(i)
	print str(params['monall'])
	#check to see if minimum parameters present
	if bstall and byrsall and bsppall and bcntyall and bmonall:
		print '----------------------------------'+nl+'You must specify at least one criteria to produce results.'+nl+'----------------------------------'
		error_text()

def main():
	load_cmd_args()

	# setup output files

	###############################################################
	###############################################################
	# folder for output files
	source_folder = 'D:\\ebird_data\\ebd_relMay-2018\\'

	###############################################################
	###############################################################
	
	#filename includes all parameters and run date
	fn_prefix = fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+params['mon'])
	results_check_fn = folder + 'results\\' + fn_delim.join([fn_prefix]+['ebd_checklists.csv'])
	results_spp_fn = folder + 'results\\' + fn_delim.join([fn_prefix]+['ebd_spp_count.csv'])
	results_obs_fn = folder + 'results\\' + fn_delim.join([fn_prefix]+['ebd_obs.csv'])
	results_spplist_fn = folder + 'results\\' + fn_delim.join([fn_prefix]+['ebd_spplist.csv'])
	err_fn = folder + 'results\\' + fn_delim.join([fn_prefix]+['err_log.csv'])
	out_check = open(results_check_fn,'w')
	out_spp = open(results_spp_fn,'w')
	out_obs = open(results_obs_fn,'w')
	out_spplist = open(results_spplist_fn,'w')
	err = open(err_fn,'w')


	print nl + '-looping through source file'
	# Set needed variables before starting loop
	count = 1
	hitcount = 1 #TESTING
	recsfound = 0
	byrs = False
	bcnty = False
	bspp = False
	bst = False
	# These will store relevant row data
	check_data = OrderedDict()
	spp_data = OrderedDict()
	spplist_data = OrderedDict()
	obs_data = OrderedDict()
	row_data = OrderedDict()
	# keep a list of keys to reduce redundant records
	check_keys = []
	spp_keys = []
	obs_keys = []

	###############################################################
	# OPEN source file, loop through lines
	start = time.clock()
	with open(fn) as f:
		for line in f:
			#remove commas
			line = line.replace(out_delim,'').replace(ebird_delim+'\n','').replace("'",'') #remove final delimiter

			r = line.split(ebird_delim) #split by EBD delimiter (tab)
			
			###############################################################
			# HEADER LINE
			if count ==1:
				
				###############################################################
				# populate header dict, write headers to output files
				# then add columns to appropriate files
				# populate all_fields array to hold header values
				all_fields = r

				out_check.write(out_delim.join(check_fields))
				out_obs.write(out_delim.join(obs_fields))
				out_spp.write(out_delim.join(spp_fields))
				out_spplist.write(out_delim.join(spplist_fields))

			# ALL OTHER LINES
			else:
				# print nl + 'line len: ' + str(len(line)) #TESTING

				#booleans for evaluating three criteria - if 'all', trumps individual criteria
				byrs = byrsall
				bspp = bsppall 
				bst = bstall
				bcnty = bcntyall
				bmon = bmonall

				#####################################################################################
				# Test criteria for this record
				if str(r[date_col][:4]) in params['yrsall']:
					byrs = True
					hitcount +=1

				if r[state_col] in params['stall']:
					bst = True

				if r[sciname_col] in params['sppsci']:
					bspp = True

				if r[cnty_col] in params['cntyall']:
					bcnty = True

				if str(r[date_col][5:7]) in params['monall']:
					bmon = True

				#####################################################################################
				## Create dictionaries with header fields as keys and current row data as values
				if byrs and bst and bcnty and bspp and bmon: #are all criteria met?

					row_data = OrderedDict([(all_fields[i],d) for i,d in enumerate(r)])

					# populate lists containing file data
					for i,d in row_data.items():
						if i in check_fields:
							check_data[i] = d
						if i in obs_fields:
							obs_data[i] = d
						if i in spp_fields:
							spp_data[i] = d
						if i in spplist_fields:
							spplist_data[i] = d

					#####################################################################################
					## check to see if row contains duplicate data, output to relevant file if not

					if row_data[check_key] not in check_keys: #new checklist
						out_check.write(nl+out_delim.join(check_data.values()))
						check_keys.append(row_data[check_key])
					if row_data[obs_key] not in obs_keys:
						out_obs.write(nl+out_delim.join(obs_data.values()))
						obs_keys.append(row_data[obs_key])
					
					if row_data[spp_key] not in spp_keys:
						out_spp.write(nl+out_delim.join(spp_data.values()))
						spp_keys.append(row_data[spp_key])

					# ALWAYS OUTPUT SPECIES ID
					out_spplist.write(nl+out_delim.join(spplist_data.values()))
					## found records counter
					recsfound += 1

			#Keeping count, pop up message every million records to show we're still working!
			count +=1
			count_prop = count % 1000000

			if count_prop==0: 
				nowt = time.clock()
				print nl + '== ' + str(count) + ' records evaluated - ' + str(recsfound) +' found =='
				print '== ' + 'check_key count: ' + str(len(check_keys)) + ' - ' + 'obs_key count: ' + str(len(obs_keys)) + ' - ' + 'spp_key count: ' + str(len(spp_keys)) + ' =='
				print '== ' + 'loop time: ' + str(round(nowt - start,1)) + 's ==' + nl

				start = nowt

	print 'search completed. ' + str(count) + ' lines evaluated, ' + str(recsfound) + ' matching records found.' + nl
	print '== ' + 'output file: ' + str(fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty'])) + ' ==' + nl
if __name__ == '__main__':
	main()