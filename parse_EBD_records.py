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
from collections import OrderedDict

params = {'yrs':[],'yrsall':[],'spp':[],'sppsci':[], 'st':[], 'stall':[], 'cnty':[], 'cntyall':[]}
rundt = str(datetime.datetime.now().strftime('%Y%m%d'))

ebird_delim = '\t' 
delim = '~'
out_delim = ','
fn_delim = '_' #delim for filename
nl = '\n'
rootdir = ''
fn = ''
state_col = 15 #STATE CODE column
sciname_col = 5 #SCIENTIFIC NAME column
cnty_col = 16 #COUNTY NAME column
id_col = 0 #GLOBAL UNIQUE IDENTIFIER column
lat_col = 21 #LATITUDE
lon_col = 22 #LONGITUDE
# ncbt_lat_col = 2 #LATITUDE
# ncbt_lon_col = 3 #LONGITUDE
checklist_col = 32 #SAMPLING EVENT IDENTIFIER
lat_dist = 0.003
lon_dist = 0.004
date_col = 27 #DATE column
folder = 'C:\\data\\@nc_birding_trail\\ebird\\'

check_fields = ['COUNTRY','COUNTRY CODE','STATE','STATE CODE','COUNTY','COUNTY CODE','IBA CODE','BCR CODE','USFWS CODE','ATLAS BLOCK','LOCALITY','LOCALITY ID',' LOCALITY TYPE','LATITUDE','LONGITUDE','OBSERVATION DATE','TIME OBSERVATIONS STARTED','OBSERVER ID','SAMPLING EVENT IDENTIFIER','PROTOCOL TYPE','PROTOCOL CODE','PROJECT CODE','DURATION MINUTES','EFFORT DISTANCE KM','EFFORT AREA HA','NUMBER OBSERVERS','ALL SPECIES REPORTED','GROUP IDENTIFIER','HAS MEDIA','APPROVED','REVIEWED','REASON','TRIP COMMENTS','SPECIES COMMENTS'] #list of checklist column headers
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

#eBird EBD Header Fields for reference
ncbt_fields = ['TITLE','SITESLUG','LAT','LON','REGION','GROUPSLUG','CATEGORY','GROUP']

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
	print '-years-' + nl + 'yrs=2002 or yrs=2003,2005 or yrs=2003-2010' + nl
	print '-species-' + nl + 'spp=BACS or yrs=BACS,WOTH,AMRE' + nl
	print '-state-' + nl + 'st=NC or st=NC,VA,SC'
	print '-county-' + nl + 'cnty=Santa_Cruz or cnty=Santa_Cruz,Orange,Wake'
	print '=============================================' + nl + '-execution halted-' + nl
	sys.exit()

def slugify(t):
	return t.replace(' ','_')

def load_cmd_args():
	print nl+'-looping through command line arguments'
	############################
	#TEMPORARY - use to simulate command line arguments
	######INPUT DATA HERE#############################

	temp_argv = 'C:\\data\\@nc_birding_trail\\ebird\\parse_ebird_records.py E:\\ebird_data\\ebd_relFeb-2018\\ebd_relFeb-2018.txt spp=HOSP'
	# temp_argv = 'C:\\data\\@projects\\eBird_obs\\20150825_parse_ebird_records.py J:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt spp=LOSH st=NC'
	# temp_argv = 'C:\\data\\@projects\\eBird_obs\\20150825_parse_ebird_records.py E:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt spp=NOBO st=NC,SC,VA,GA,TN'
	# temp_argv = 'C:\\data\\@projects\\eBird_obs\\20150825_parse_ebird_records.py E:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt spp=KEWA,SWWA,CERW,WOTH,YBCU st=NC'
	# temp_argv = 'C:\\data\\@projects\\eBird\\20150825_parse_ebird_records.py E:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt spp=YCPA'
	# temp_argv = 'C:\\data\\@projects\\eBird\\20150825_parse_ebird_records.py E:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt yrs=2003,2004 spp=YCPA'
	# temp_argv = 'C:\\data\\@projects\\eBird\\20150825_parse_ebird_records.py E:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt yrs=2003-2015 spp=BACS,YCPA'
	# temp_argv = 'C:\\data\\@projects\\eBird\\20150825_parse_ebird_records.py E:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt yrs=2010-2015 spp=RUBL'
	# temp_argv = 'C:\\data\\@projects\\eBird\\20150825_parse_ebird_records.py E:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt yrs=2003,2004 spp=BACS,YCPA'
	# temp_argv = 'C:\\data\\@projects\\eBird\\20150825_parse_ebird_records.py E:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt yrs=2003,2004 spp=BACS,YCPA st=NC'
	temp_argv = temp_argv.split(' ')

	global bstall
	global byrsall
	global bsppall
	global bcntyall
	global rootdir
	global fn
	# global ncbt
	
	# ncbt = folder + '20170606_ncbt_site_info_ebird.txt'

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
		fn = 'E:\\ebird_data\\ebd_relFeb-2018\\ebd_relFeb-2018.txt'

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
			params['yrsall'].append(y)
	else: #one year or comma separated list
		for y in yr_in:
			params['yrsall'].append(y)

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
	#populate county names in parameters
	print nl + '-populating county'
	if len(params['cnty'])==0:
		params['cntyall'].append('all')
		bcntyall = True
	else:
		for i in params['cnty']:
			params['cntyall'].append(i.replace('_',' '))

	print str(params['cntyall'])

	#################################
	#check to see if minimum parameters present

	if bstall and byrsall and bsppall and bcntyall:
		print '----------------------------------'+nl+'You must specify at least one criteria to produce results.'+nl+'----------------------------------'
		error_text()

def find_bird4(sppsci, spplist):
	#pass scientific name, list of requested 4-letter codes - cycle through spplist and return matching 4-letter bird code
	result = ''
	for n, b in enumerate(spplist): #loop through passed bird4 list
		if sppsci == bird_codes.lookup_bird(b)['SCINAME']: #check if sciname matches passed value
			result = b
	return result

 
def main():
	load_cmd_args()

	# outname = str(fn_delim.join(params.values()))
	# print str(params['yrs'])
	# print str(fn_delim.join(params['yrs']))
	results_check_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ebd_checklists.csv'])
	results_spp_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ebd_spp_count.csv'])
	results_obs_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ebd_obs.csv'])
	results_spplist_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ebd_spplist.csv'])
	# results_loc_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ebd_loc.csv'])
	err_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+params['cnty']+['ncbt_err_log.csv'])

	##############################################################################
	# setup output files
	out_check = open(results_check_fn,'w')
	out_spp = open(results_spp_fn,'w')
	out_obs = open(results_obs_fn,'w')
	# out_loc = open(results_loc_fn,'w')
	out_spplist = open(results_spplist_fn,'w')
	err = open(err_fn,'w')
	
	print nl + '-looping through source file'
	count = 1
	recsfound = 0

	check_data = OrderedDict()
	spp_data = OrderedDict()
	spplist_data = OrderedDict()
	obs_data = OrderedDict()
	row_data = OrderedDict()

	# keep a list of keys to reduce redundant records
	check_keys = []
	spp_keys = []
	# spplist_keys = [] # UNNEEDED
	obs_keys = []

	with open(fn) as f:
	# with open('E:\ebird_data\ebd_relMay-2015\ebd_relMay-2015.txt') as f:
	# with open('ebird_sample.txt') as f:
		for line in f:
			#remove commas
			line = line.replace(out_delim,'').replace(ebird_delim+'\n','').replace("'",'') #remove final delimiter
			# print str(line)
			r = line.split(ebird_delim) #split by EBD delimiter (tab)
			
			###############################################################
			# HEADER LINE
			if count ==1: #header line, skip
				
				###############################################################
				# add routine here to populate header dicts with column numbers
				# then add columns to appropriate files
				all_fields = r

				err.write(out_delim.join(['RECORD ADDED','GLOBAL ID','CHECKLIST ID','OBSERVER','SCIENTIFIC NAME','YEAR','STATE']))
				out_check.write(out_delim.join(check_fields))
				out_obs.write(out_delim.join(obs_fields))
				out_spp.write(out_delim.join(spp_fields))
				out_spplist.write(out_delim.join(spplist_fields))

				# print all_fields

			###############################################################
			# ALL OTHER LINES
			else:

				# out_check.write(nl+out_delim.join(r))
				if len(line)>1: #skip blank lines
					print nl + 'line len: ' + str(len(line))

					#booleans for evaluating three criteria
					byrs = byrsall
					bspp = bsppall 
					bst = bstall
					bcnty = bcntyall



					#print str(r[date_col][:4]) + ' - ' + str(params['yrsall'])
					print nl + 'yrs: ' + str(byrs) + ' : ' + str(byrsall) + ' : ' + str(r[date_col][:4])
					if str(r[date_col][:4]) in params['yrsall']:
						byrs = True
						print '++yrs: ' + str(byrs) + ' : ' + str(byrsall) + ' : ' + str(r[date_col][:4])
					
					print nl + 'spp: ' + str(bspp) + ' : ' + str(bsppall) +  ' : ' + str(r[sciname_col])
					if r[sciname_col] in params['sppsci']:
						bspp = True
						print '++spp: ' + str(bspp) + ' : ' + str(bsppall) +  ' : ' + str(r[sciname_col])
					
					print nl + 'state: ' + str(bst) + ' : ' + str(bstall) +  ' : ' + str(r[state_col])
					if r[state_col] in params['stall']:
						bst = True
						print '++state: ' + str(bst) + ' : ' + str(bstall) +  ' : ' + str(r[state_col])
					
					print nl + 'county: ' + str(bcnty) + ' : ' + str(bcntyall) +  ' : ' + str(r[state_col])
					if r[cnty_col] in params['cntyall']:
						bcnty = True
						print '++county: ' + str(bcnty) + ' : ' + str(bcntyall) +  ' : ' + str(r[cnty_col])
					

					#####################################################################################
					## TESTING - move below criteria when working

					# if byrs and bst and bcnty and bspp: #are all criteria met?
					if byrs and bst and bcnty and bspp: #are all criteria met?
						#####################################################################################
						## Create dictionary with headers as keys and current row as values
						row_data = OrderedDict([(all_fields[i],d) for i,d in enumerate(r)])

						for i,d in row_data.items():
							if i in check_fields:
								check_data[i] = d
							if i in obs_fields:
								obs_data[i] = d
							if i in spp_fields:
								spp_data[i] = d
							if i in spplist_fields:
								spplist_data[i] = d

						if row_data['SAMPLING EVENT IDENTIFIER'] not in check_keys: #new checklist
							out_check.write(nl+out_delim.join(check_data.values()))
							check_keys.append(row_data['SAMPLING EVENT IDENTIFIER'])
							# spp_keys = [] #clear out species list for the checklist - -UNEEDED?

						if row_data['OBSERVER ID'] not in obs_keys:
							out_obs.write(nl+out_delim.join(obs_data.values()))
							obs_keys.append(row_data['OBSERVER ID'])
						
						if row_data['GLOBAL UNIQUE IDENTIFIER'] not in spp_keys:
							out_spp.write(nl+out_delim.join(spp_data.values()))
							spp_keys.append(row_data['GLOBAL UNIQUE IDENTIFIER'])
						
						# ALWAYS OUTPUT SPECIES ID
						out_spplist.write(nl+out_delim.join(spplist_data.values()))
						
						# SHOULDN'T NEED THIS
						# if row_data['SCIENTIFIC NAME'] not in spplist_keys:
						# 	out_spplist.write(nl+out_delim.join(spplist_data.values()))
						# 	spplist_keys.append(row_data['SCIENTIFIC NAME'])

						## TESTING
						err.write(nl+out_delim.join(['True',r[0],r[32],r[30] + ' ' + r[31],r[5],r[date_col][:4],r[15]]))
					
					else: #criteria not met
						## TESTING
						err.write(nl+out_delim.join(['False',r[0],r[32],r[30] + ' ' + r[31],r[5],r[date_col][:4],r[15]]))
						pass

					##
					#####################################################################################

					#####################################################################################
					## COMMENTED OUT FOR TESTING
					# 	# EXAMPLE
					# 	# a = [-2,1,5,3,8,5,6]
					# 	# b = [1,2,5]
					# 	# c = [ a[i] for i in b]

					# 	print 'check: ' + str([r[i] for i in check_cols.keys()])
					# 	print 'obs: ' + str([r[i] for i in obs_cols.keys()])
					# 	print 'loc: ' + str([r[i] for i in loc_cols.keys()])
					# 	print 'spp: ' + str([r[i] for i in spp_cols.keys()])
					# 	print 'spplist: ' + str([r[i] for i in spplist_cols.keys()])

					# 	if r[checklist_col] != currChecklist: #save new checklist information
					# 		currChecklist = r[checklist_col]
					# 		# out_check.write(out_delim.join([find_bird4(r[sciname_col],params['spp'])] + r))
					# 		out_check.write(out_delim.join([bird_codes.lookup_sciname(slugify(r[sciname_col]))] + r))
					#	recsfound +=1
					##
					#####################################################################################



						################################################################################
						#this code checks to parse down to one record per checklist
						# if r[id_col] in ebird_rec_ids: #this list has already been added or checked
						# 	pass
						# else:
						# 	# list has not been evaluated
						# 	ebird_rec_ids.append(r[id_col]) #add to evaluated list

						# 	#check to see if lat/lon within distance of an NCBT Site
						# 	with open(ncbt) as ts: #loop through trail sites
						# 		for ncbt_line in ts:
						# 			ncbt_line = ncbt_line.replace(out_delim,'')
						# 			ncbt_r = ncbt_line.split(ebird_delim)
						# 			la = float(ncbt_r[ncbt_lat_col])
						# 			lo = float(ncbt_r[ncbt_lon_col])
						# 			#if lat_dist+la >= r[lat_col] >= la-lat_dist and lon_dist+lo >= r[lon_col] >= lo-lon_dist
						# 			if lat_dist+la >= r[lat_col] >= la-lat_dist and lon_dist+lo >= r[lon_col] >= lo-lon_dist:

						# 				err.write(out_delim.join([str(la),str(lo),str(r[lat_col]),str(r[lon_col]),str(lat_dist+la >= r[lat_col] >= la-lat_dist),str(lon_dist+lo >= r[lon_col] >= lo-lon_dist)]+r)+nl)
						# 				out.write(out_delim.join([r[id_col],r[lat_col],r[lon_col]] + ncbt_r)+nl)
						# 				recsfound +=1

			count +=1
			count_prop = float(count) / 10000
			#print str((count_prop)-int(count_prop)).split('.')
			if int(str((count_prop)-int(count_prop)).split('.')[1][:10])==0: print '== ' + str(count) + ' records evaluated - ' + str(recsfound) +' found =='
			if count>200: break #for testing
	print 'search completed. ' + str(count) + ' lines evaluated, ' + str(recsfound) + ' matching records found.'

if __name__ == '__main__':
	main()