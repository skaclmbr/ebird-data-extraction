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

params = {'yrs':[],'yrsall':[],'spp':[],'sppsci':[], 'st':[], 'stall':[]}
rundt = str(datetime.datetime.now().strftime('%Y%m%d'))

ebird_delim = '\t' 
delim = '~'
out_delim = ','
fn_delim = '_' #delim for filename
nl = '\n'
rootdir = ''
fn = ''
state_col = 13 #STATE CODE column
sciname_col = 4 #SCIENTIFIC NAME column
county_col = 14 #COUNTY NAME column
id_col = 0 #GLOBAL UNIQUE IDENTIFIER column
lat_col = 21 #LATITUDE
lon_col = 22 #LONGITUDE
ncbt_lat_col = 2 #LATITUDE
ncbt_lon_col = 3 #LONGITUDE
lat_dist = 0.003
lon_dist = 0.004
date_col = 23 #DATE column
folder = 'C:\\data\\@nc_birding_trail\\ebird\\'

ebird_rec_ids = []

# boolean markers for identifying if no criteria set
bstall = False
byrsall = False
bsppall = False

#eBird EBD Header Fields for reference
ncbt_fields = ['TITLE','SITESLUG','LAT','LON','REGION','GROUPSLUG','CATEGORY','GROUP']
ebird_fields = ['GLOBAL UNIQUE IDENTIFIER','TAXONOMIC ORDER','CATEGORY','COMMON NAME','SCIENTIFIC NAME','SUBSPECIES COMMON NAME','SUBSPECIES SCIENTIFIC NAME','OBSERVATION COUNT','BREEDING BIRD ATLAS CODE','AGE/SEX','COUNTRY','COUNTRY CODE','STATE','STATE CODE','COUNTY','COUNTY CODE','IBA CODE','BCR CODE','LOCALITY','LOCALITY ID',' LOCALITY TYPE','LATITUDE','LONGITUDE','OBSERVATION DATE','TIME OBSERVATIONS STARTED','TRIP COMMENTS','SPECIES COMMENTS','OBSERVER ID','FIRST NAME','LAST NAME','SAMPLING EVENT IDENTIFIER','PROTOCOL TYPE','PROJECT CODE','DURATION MINUTES','EFFORT DISTANCE KM','EFFORT AREA HA','NUMBER OBSERVERS','ALL SPECIES REPORTED','GROUP IDENTIFIER','APPROVED','REVIEWED','REASON']
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
	print '=============================================' + nl + '-execution halted-' + nl
	sys.exit()

def load_cmd_args():
	print nl+'-looping through command line arguments'
	############################
	#TEMPORARY - use to simulate command line arguments
	######INPUT DATA HERE#############################

	temp_argv = 'C:\\data\\@nc_birding_trail\\ebird\\parse_ebird_records.py J:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt yrs=1975-2015 st=NC'
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
	global rootdir
	global fn
	global ncbt
	
	ncbt = folder + '20170606_ncbt_site_info_ebird.txt'

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
		fn = 'J:\\ebird_data\\ebd_relMay-2015\\ebd_relMay-2015.txt'

	#################################
	#populate scientific names in parameters
	print nl + '-populating species'
	if len(params['spp'])==0:
		params['sppsci'].append('all')
		bsppall = True
	else:
		for i in params['spp']:
			params['sppsci'].append(bird_codes.lookup_bird(i)['SCINAME'])

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
			if i in us_state_abbr:
				params['stall'].append(us_states[i][0])
			else:
				params['stall'].append(i)


	print str(params['stall'])

	#################################
	#check to see if minimum parameters present

	if bstall and byrsall and bsppall:
		print '----------------------------------'+nl+'You must specify at least one criteria to produce results.'+nl+'----------------------------------'
		error_text()

	# except:
	# 	print 'exception during criteria building'
	# 	error_text()

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
	results_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+['ncbt_ebird.csv'])
	err_fn = folder + 'results\\' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+['ncbt_err_log.csv'])
	

	##############################################################################
	#this restricts to a subset of counties
	#only for ad hoc analysis
	counties = ['Burke','Edgecombe','Lenoir','Madison','Pasquotank','Robeson']
	##############################################################################

	# print nl + 'results file:' + results_fn #testing
	

	out = open(results_fn,'w')
	err = open(err_fn,'w')
	# out = open(rootdir + '/results/' + fn_delim.join([rundt]+params['yrs']+params['spp']+params['st']+['ebd.csv']),'w')
	count = 1
	recsfound = 0
	print nl + '-looping through source file'
	with open(fn) as f:
	# with open('E:\ebird_data\ebd_relMay-2015\ebd_relMay-2015.txt') as f:
	# with open('ebird_sample.txt') as f:
		for line in f:
			#remove commas
			line = line.replace(out_delim,'') #remove final delimiter
			r = line.split(ebird_delim) #split by ebird delimiter (tab)
			
			if count ==1: #header line, skip
				out.write(str(out_delim.join(['SPPCODE']+r)))
			else:
				if len(line)>1: #skip blank lines

					#booleans for evaluating three criteria
					byrs = byrsall
					bspp = bsppall 
					bst = bstall

					#print str(r[date_col][:4]) + ' - ' + str(params['yrsall'])
					if int(r[date_col][:4]) in params['yrsall']: byrs = True
					#if r[sciname_col] in params['sppsci']: bspp = True
					if r[state_col] in params['stall']: bst = True
					#print str(byrs)

					if byrs and bst:
						#print str(r[county_col])
						if r[county_col] in counties: # retrieve only chatham county right now 
							out.write(out_delim.join([find_bird4(r[sciname_col],params['spp'])] + r))
							recsfound +=1
						#out.write(out_delim.join([find_bird4(r[sciname_col],params['spp'])] + r))

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
			#if count>100000: break #for testing
	print 'search completed. ' + str(count) + ' lines evaluated, ' + str(recsfound) + ' matching records found.'

if __name__ == '__main__':
	main()