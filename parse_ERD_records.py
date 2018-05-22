#parse eBird Records to link with NCBT sites
#Scott Anderson Jun 6, 2017
#NC Wildlife Resources Commission
#scott.anderson@ncwildlife.org
#python 2.7

# Parse through eBird Basic Dataset records and retrieve specified records
# inputs:
#		spp = species list
#		yr = year list
#		st = 2-letter state code list

import os
import sys
import datetime
import bird_codes

params = {'yrs':[],'yrsall':[],'spp':[],'sppsci':[], 'st':[], 'stall':[], 'cnty':[], 'cntyall':[]}
rundt = str(datetime.datetime.now().strftime('%Y%m%d'))

year = '2016' #ERD has separate file and folder for year
erd_file_path = 'E:\\ebird_data\\ERD2016SS\\' + year + '\\checklists.csv'

ebird_delim = ',' 
delim = '~'
out_delim = ','
fn_delim = '_' #delim for filename
nl = '\n'
rootdir = ''
fn = ''
state_col = 9 #STATE CODE column
#sciname_col = 4 #SCIENTIFIC NAME column - EBD
county_col = 10 #COUNTY NAME column
id_col = 0 #GLOBAL UNIQUE IDENTIFIER column
lat_col = 2 #LATITUDE
lon_col = 3 #LONGITUDE
ncbt_lat_col = 2 #LATITUDE - NCBT
ncbt_lon_col = 3 #LONGITUDE - NCBT
lat_dist = 0.003
lon_dist = 0.004
date_col = 23 #DATE column - EBD
year_col = 4 #DATE column - ERD
folder = 'C:\\data\\@nc_birding_trail\\ebird\\' #for NCBT work - output folder

ebird_rec_ids = []

# boolean markers for identifying if no criteria set
bstall = False
byrsall = False
bsppall = False
bsppallnc = False
bcntyall = False

#eBird EBD Header Fields for reference
ncbt_fields = ['TITLE','SITESLUG','LAT','LON','REGION','GROUPSLUG','CATEGORY','GROUP']
# Run through first row, gather species list


#ebird_fields = ['GLOBAL UNIQUE IDENTIFIER','TAXONOMIC ORDER','CATEGORY','COMMON NAME','SCIENTIFIC NAME','SUBSPECIES COMMON NAME','SUBSPECIES SCIENTIFIC NAME','OBSERVATION COUNT','BREEDING BIRD ATLAS CODE','AGE/SEX','COUNTRY','COUNTRY CODE','STATE','STATE CODE','COUNTY','COUNTY CODE','IBA CODE','BCR CODE','LOCALITY','LOCALITY ID',' LOCALITY TYPE','LATITUDE','LONGITUDE','OBSERVATION DATE','TIME OBSERVATIONS STARTED','TRIP COMMENTS','SPECIES COMMENTS','OBSERVER ID','FIRST NAME','LAST NAME','SAMPLING EVENT IDENTIFIER','PROTOCOL TYPE','PROJECT CODE','DURATION MINUTES','EFFORT DISTANCE KM','EFFORT AREA HA','NUMBER OBSERVERS','ALL SPECIES REPORTED','GROUP IDENTIFIER','APPROVED','REVIEWED','REASON']

# these are the basic ERD fields, one colum for each species after
erd_fields = ['SAMPLING_EVENT_ID','LOC_ID','LATITUDE','LONGITUDE','YEAR','MONTH','DAY','TIME','COUNTRY','STATE_PROVINCE','COUNTY','COUNT_TYPE','EFFORT_HRS','EFFORT_DISTANCE_KM','EFFORT_AREA_HA','OBSERVER_ID','NUMBER_OBSERVERS','GROUP_ID','PRIMARY_CHECKLIST_FLAG']

us_states = {'AK':['US-AK','United States','US','Alaska'],'AL':['US-AL','United States','US','Alabama'],'AR':['US-AR','United States','US','Arkansas'],'AZ':['US-AZ','United States','US','Arizona'],'CA':['US-CA','United States','US','California'],'CO':['US-CO','United States','US','Colorado'],'CT':['US-CT','United States','US','Connecticut'],'DC':['US-DC','United States','US','District of Columbia'],'DE':['US-DE','United States','US','Delaware'],'FL':['US-FL','United States','US','Florida'],'GA':['US-GA','United States','US','Georgia'],'HI':['US-HI','United States','US','Hawaii'],'IA':['US-IA','United States','US','Iowa'],'ID':['US-ID','United States','US','Idaho'],'IL':['US-IL','United States','US','Illinois'],'IN':['US-IN','United States','US','Indiana'],'KS':['US-KS','United States','US','Kansas'],'KY':['US-KY','United States','US','Kentucky'],'LA':['US-LA','United States','US','Louisiana'],'MA':['US-MA','United States','US','Massachusetts'],'MD':['US-MD','United States','US','Maryland'],'ME':['US-ME','United States','US','Maine'],'MI':['US-MI','United States','US','Michigan'],'MN':['US-MN','United States','US','Minnesota'],'MO':['US-MO','United States','US','Missouri'],'MS':['US-MS','United States','US','Mississippi'],'MT':['US-MT','United States','US','Montana'],'NC':['US-NC','United States','US','North Carolina'],'ND':['US-ND','United States','US','North Dakota'],'NE':['US-NE','United States','US','Nebraska'],'NH':['US-NH','United States','US','New Hampshire'],'NJ':['US-NJ','United States','US','New Jersey'],'NM':['US-NM','United States','US','New Mexico'],'NV':['US-NV','United States','US','Nevada'],'NY':['US-NY','United States','US','New York'],'OH':['US-OH','United States','US','Ohio'],'OK':['US-OK','United States','US','Oklahoma'],'OR':['US-OR','United States','US','Oregon'],'PA':['US-PA','United States','US','Pennsylvania'],'RI':['US-RI','United States','US','Rhode Island'],'SC':['US-SC','United States','US','South Carolina'],'SD':['US-SD','United States','US','South Dakota'],'TN':['US-TN','United States','US','Tennessee'],'TX':['US-TX','United States','US','Texas'],'UT':['US-UT','United States','US','Utah'],'VA':['US-VA','United States','US','Virginia'],'VT':['US-VT','United States','US','Vermont'],'WA':['US-WA','United States','US','Washington'],'WI':['US-WI','United States','US','Wisconsin'],'WV':['US-WV','United States','US','West Virginia'],'WY':['US-WY','United States','US','Wyoming']}
us_state_abbr = us_states.iterkeys()
# 'OBSERVATION DATE' (23), 'SCIENTIFIC NAME' (4), 'STATE' (13)

print '-parameters set'
#  
#retrieve command line arguments
#	arg 1 = program file
#	arg 2 = file to be processed
#other arg syntax examples:
#	yrs=2002,2003
#	yrs=2002-2010
#	spp=BACS,RUGR
#	st=NC,VA,SC

def error_text():
	print nl + '=============================================' + nl + 'please enter variables in the following format:' + nl
	#print '-years-' + nl + 'yrs=2002 or yrs=2003,2005 or yrs=2003-2010' + nl
	print '-species-' + nl + 'spp=BACS or spp=BACS,WOTH,AMRE' + nl
	print '-state-' + nl + 'st=NC or st=NC,VA,SC'
	print '-county-' + nl + 'cnty=Wake or cnty=Wake,Durham,Chatham'
	print '=============================================' + nl + '-execution halted-' + nl
	sys.exit()

def slugify(t):
	return t.replace(' ','_')

def load_cmd_args():
	print nl+'-looping through command line arguments'
	############################
	#TEMPORARY - use to simulate command line arguments
	#ERD puts files in separate folders, one for each year, will have to direct to the correct year
	######INPUT DATA HERE#############################


	temp_argv = 'C:\\data\\@nc_birding_trail\\ebird\\parse_ebird_records.py ' + erd_file_path + ' st=NC spp=NC' #FIX THIS!!!!
	# temp_argv = 'C:\\data\\@nc_birding_trail\\ebird\\parse_ebird_records.py ' + erd_file_path + ' st=NC cnty=Burke,Edgecombe,Lenoir,Madison,Pasquotank,Robeson' #FIX THIS!!!!
	#temp_argv = 'C:\\data\\@nc_birding_trail\\ebird\\parse_ebird_records.py ' + erd_file_path + ' st=NC' #FIX THIS!!!!
	# temp_argv = 'C:\\data\\@projects\\eBird_obs\\20150825_parse_ebird_records.py J:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt spp=LOSH st=NC'
	temp_argv = temp_argv.split(' ')

	global bstall
	global byrsall
	global bsppall
	global bsppallnc
	global bcntyall
	global rootdir
	global fn
	global ncbt
	
	#need this?
	#ncbt = folder + '20170606_ncbt_site_info_ebird.txt'

	# try:
	for argn, arg in enumerate(temp_argv): #for testing
	# for argn, arg in enumerate(sys.argv): 
		if argn>=1: #parse values
			if "=" in arg:
				#format should be: argname=[val1,val2,val3]
				flag = arg.split('=')
				params[flag[0]]= flag[1].split(',')
			else: #assume it is a filename
				fn = arg
		elif argn==0:
			dirfilesep = arg.rfind('\\')+1
			rootdir = arg[:dirfilesep]
			sourcefile = arg[dirfilesep:] 

	if len(fn)==0: #no filename passed, use default
		# fn = 'E:\\ebird_data\\ebd_relFeb-2018\\ebd_relFeb-2018.txt' #FIX THIS!!!
		fn = erd_file_path #FIX THIS!!!

	#################################
	#check if all species to be collected
	#ERD - create array of species column #
	print nl + '-year'
	print str(year)

	#################################
	#check if all species to be collected
	print nl + '-populating species'

	print params['spp']
	if len(params['spp'])==0:
		params['sppsci'].append('all')
		params['spp'].append('all')
		bsppall = True
	elif params['spp'][0]=='NC': #if NC species chosen, only select these species
		#look for all NC Species
		bspallnc = True
		params['spp'] = bird_codes.nc_birds

	#################################
	#populate all years in parameters
	#modify this to change folders where files are located
	# print nl + '-populating years'
	# yr_in = params['yrs']

	# if len(params['yrs'])==0:
	# 	params['yrsall'].append('all')
	# 	byrsall = True
	# elif '-' in yr_in[0]: #should represent a range of years
	# 	print 'range detected'
	# 	yr_range = yr_in[0].split('-')
	# 	for y in range(int(yr_range[0]),int(yr_range[1])+1):
	# 		params['yrsall'].append(y)
	# else: #one year or comma separated list
	# 	for y in yr_in:
	# 		params['yrsall'].append(y)

	# print str(params['yrsall'])

	#################################
	#populate all counties in parameters
	print nl + '-populating counties'

	if len(params['cnty'])==0:
		params['cntyall'].append('all')
		bcntyall = True
	else:
		for i in params['cnty']:
			params['cntyall'].append(i)
	print str(params['cntyall'])
	
	#################################
	#populate all states in parameters
	print nl + '-populating states'
	if len(params['st'])==0:
		params['stall'].append('all')
		bstall = True
	else:
		for i in params['st']:
			if i in us_state_abbr:
				#params['stall'].append(us_states[i][0]) #This version for EBD
				params['stall'].append(us_states[i][3]) #This version for ERD
			else:
				params['stall'].append(i)

	print str(params['stall'])

	###########################################
	#check to see if minimum parameters present

	if bstall and bcntyall and bsppall:
		print '----------------------------------'+nl+'You must specify at least one criteria to produce results.'+nl+'----------------------------------'
		error_text()

def main():
	# parse through command arguments, set parameters
	load_cmd_args()

	###########################################
	# If species list is long, use MultNCSpp in filename
	spp_fn = params['spp']
	if bsppallnc:
		spp_fn = ['NCSpp']
	elif len(params['spp'])>5:
		spp_fn = ['MultNCSpp']

	# Set up files for results
	results_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+spp_fn+params['st']+['erd_checklists.csv']) #main checklist file
	outspp_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+spp_fn+params['st']+['erd_species_counts.csv']) #list of species detected per checklist
	outspplookup_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+spp_fn+params['st']+['erd_spp_lookup.csv']) #list of 
	err_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+spp_fn+params['st']+['erd_err_log.csv'])

	cntylist = []	

	out = open(results_fn,'w')
	outspp = open(outspp_fn,'w')
	outspplookup = open(outspplookup_fn,'w')
	err = open(err_fn,'w')

	outspp.write(out_delim.join(['SAMPLING_EVENT_ID','SPP4','COUNT']))
	outspplookup.write(out_delim.join(['SCINAME_SLUG','SPP4','COMMNAME','SCINAME','NC']))

	count = 1
	recsfound = 0
	spp_cols = {} #array of columns to watch for non-zero values

	###########################################
	print nl + '-looping through source file'

	with open(fn) as f:
		for line in f:
			r = line.split(ebird_delim) #split by erd delimiter (comma)
			
			if count ==1: #header line

				##############################################################################
				# add header row to checklist file
				out.write(out_delim.join(r[:len(erd_fields)]))

				##############################################################################
				#calculate species columns to watch based on spp list passed and matches to column headers
				if bsppall:
					for x in xrange(len(erd_fields),len(r)): #count to maintain column #s
						spp_cols[x] = r[x] #capture all species
				else:
					for i in params['spp']:

						bird_info = bird_codes.lookup_spp4(i) #get bird info list from bird_codes based on 4 digit code
						if bird_info: #if a record returned...
							params['sppsci'].append(bird_info['SCINAME_SLUG']) #translate 4 letter codes to ERD formatted scinames

							for x in xrange(len(erd_fields),len(r)): #loop through species and find column numbers
								if r[x] == bird_info['SCINAME_SLUG']:
									spp_cols[x] = r[x] #create dict: key=column #, value = sci slug
									outspplookup.write(nl+out_delim.join(bird_info.values()))
			else:
				if len(line)>1: #skip blank lines

					##############################################################################
					# RESET booleans for evaluating three criteria
					#byrs = byrsall
					bspp = bsppall
					bst = bstall
					bcnty = bcntyall

					##############################################################################
					## EVALUATE YEARS
					#if int(r[date_col][:4]) in params['yrsall']: byrs = True #EBD
					#if r[year_col] in params['yrsall']: byrs = True #ERD

					
					##############################################################################
					## EVALUATE SPP
					## if not all species or not all NC species, only collect rows where chosen species are found
					if not(bsppall or bsppallnc):
						# print '-checking for non-zero counts'
						for i in spp_cols: #loop through spp columns, return true if value >0
							temp_count = r[i]
							try: #try to evaluate if number is greater than 0
								if int(temp_count) > 0: bspp = True
							except: #check to see if value is 'x'
								if temp_count == 'x': bspp = True

					##############################################################################
					## EVALUATE STATE
					if r[state_col] in params['stall']: bst = True

					##############################################################################
					## EVALUATE COUNTY
					if r[county_col] in params['cntyall']: bst = True

					##############################################################################
					## populate output files
					if bst and bspp and bcnty: #years not relevant, already defined
						#print str(r[county_col])
						out.write(nl+out_delim.join(r[:len(erd_fields)])) #ERD output of first columns to checklist table (not spp columns)
						
						#populate separate table with spp records
						# print str(r[len(erd_fields):])
						for s in spp_cols.keys():
							##############################################################################
							# FOR TESTING
							# print str(n) + ': ' + str(s)
							# print str(s) + ' : ' + spp_cols[s] + ' : ' + r[s]
							# print str(out_delim.join([r[0],spp_cols[s],r[s]]))
							##############################################################################
							outspp.write(nl+out_delim.join([r[0],spp_cols[s],r[s]]))

						recsfound +=1
					
					##############################################################################
					## Turn off all indicators
					bspp = False
					bst = False
					bcnty = False

			count +=1
			count_prop = float(count) / 10000

			if int(str((count_prop)-int(count_prop)).split('.')[1][:10])==0: print '== ' + str(count) + ' records evaluated - ' + str(recsfound) +' found =='
			# if count>100: break #for testing

	# print nl + 'counties found: ' + out_delim.join(cntylist)
	# err.write(out_delim.join(cntylist))
	print nl + 'search completed. ' + str(count) + ' lines evaluated, ' + str(recsfound) + ' matching records found.'

if __name__ == '__main__':
	main()