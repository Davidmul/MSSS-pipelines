#pipeline version 0.1 12.10.2016
#Basic structure done
#pipeline version 0.2 26.12.2017
#dummy skymodel matches BBS input exactly
#parser options for input field


from __future__ import division
import time
import os
import glob
import sys
import pyrap.tables as pt,pyrap.quanta as qa,pyrap.measures as pm
import numpy as np
import subprocess
import optparse
import lofar.parmdb as pdb
from multiprocessing import Pool

# Master copy of the original NDPPP and BBS parsets
# These locations need to be changed by the user
#Additionally, the location of the createRMparmdb program needs to be changed as seen further below

master_copy_parset = './NDPPP_copy.parset'
master_bbs_rmparset = './BBS_RMcorrect.parset'
master_bbs_skymodel = './dummyskymodel_master.skymodel'



# Function to copy all MS in a list

def copyNDPPP(input_list,type):
        f = open(master_copy_parset,'r')
        master_copy_data = f.read()
        f.close()
        for i in range(len(input_list)):
                print i
                newdata = master_copy_data.replace('input',input_list[i])
                newdata = newdata.replace('output',str(input_list[i])+'.'+str(type))
                nf = open(input_list[i]+'.'+str(type)+'.parset','w')
                nf.write(newdata)
                nf.close()
                os.system('NDPPP '+input_list[i]+'.'+str(type)+'.parset')
                os.system('rm '+input_list[i]+'.'+str(type)+'.parset')


# Function to run BBS on a list of MS files
# One function for each snapshot
#Enabling parallelisation here to speed things up

def testcal1(ms):
        subprocess.call('calibrate-stand-alone -f --replace-parmdb --replace-sourcedb --parmdb '+str(input)+'/RMPARMDB.1SNAPSHOT '+str(ms)+' '+str(master_bbs_rmparset)+' '+str(input)+'/dummyphasecentre.skymodel'.format(ms), shell=True)

def testcal2(ms):
	subprocess.call('calibrate-stand-alone -f --replace-parmdb --replace-sourcedb --parmdb '+str(input)+'/RMPARMDB.2SNAPSHOT '+str(ms)+' '+str(master_bbs_rmparset)+' '+str(input)+'/dummyphasecentre.skymodel'.format(ms), shell=True)


#Function to create a dummy skymodel for RM correction
#pyrap is needed here to extract coordinates from MS itself and input said coordinates into the dummy skymodel

def createdummyskymodel(inputmslist):
        t = pt.table(inputmslist[0])
        tb = pt.table(t.getkeyword('FIELD'))
        direction = np.squeeze(tb.getcol('PHASE_DIR'))
        RA = np.rad2deg(direction[0]);DEC = np.rad2deg(direction[1]) #RA directly from MS #RA from MS is in radians
        RAhr = int(RA/15);RAarcmin = int(((RA/15)-RAhr)/(1/60));RAarcsec=(((RA/15)-RAhr)/(1/60)-RAarcmin)/(1/60) #converting RA degrees to hrs.mins.secs
        DECdeg = int(DEC);DECarcmin = int((DEC-DECdeg)*60);DECarcsec=((DEC-DECdeg)*60-DECarcmin)*60 #converting DEC degrees to degs.mins.secs
        f = open(master_bbs_skymodel,'r')
        master_copy_skymodel = f.read()
        f.close()
        newdata = master_copy_skymodel.replace('RAhr',str(RAhr).zfill(2)) #inputting data to new skymodel,filling with a zero if required
        newdata = newdata.replace('RAarcmin',str(RAarcmin).zfill(2))
        newdata = newdata.replace('RAarcsec',str(RAarcsec).zfill(2))
        newdata = newdata.replace('DECdeg','+'+str(DECdeg).zfill(2))
        newdata = newdata.replace('DECarcmin',str(DECarcmin).zfill(2))
        newdata = newdata.replace('DECarcsec',str(DECarcsec).zfill(2))
        nf = open('dummyphasecentre.skymodel','w')
        nf.write(newdata)
        nf.close()

#Function to run parmdb
# Using CODE TEC values, commissioning tests show that CODE is more reliable than ROB TEC values
#THE LOCATION OF THE PROGRAM  NEEDS TO BE CHANGED!

def createRMparm(inputmslist,snapshotnum):
        os.system("python /home/mulcahy/bin/createRMParmdb "+inputmslist[0]+" -a -o RMPARMDB."+str(snapshotnum)+"SNAPSHOT --IONprefix='CODG' --IONpath='./../IONEX/'")



######################PARSER OPTIONS##################################


parser = optparse.OptionParser()

required = optparse.OptionGroup(parser, 'Required Attributes')

required.add_option('--i', help='Input Field directory, eg. H033+41', type='string', dest='input')

parser.add_option_group(required)

options, arguments = parser.parse_args()

input = options.input

########################################################

start = time.time()

#check for any final calibrated files

print 'checking directories for completed files'

dircheck = glob.glob(str(input)+'/*finalsnapver/*')
for i in range(len(dircheck)):
	if os.path.isdir(dircheck[i])==True:
		print 'Final MS file already exists,calibration already performed'
		print 'Exiting'
		sys.exit()
	else:
		continue

currentdir = os.getcwd()

#create snapshot directories

os.system('mkdir '+str(input)+'/1stsnap')
os.system('mkdir '+str(input)+'/2ndsnap')

#get the list of MS files for each seperate snapshot

snapshot1_file_list = glob.glob(str(input)+'/'+os.listdir(str(input))[0]+'/BAND*/*.MS')
snapshot2_file_list = glob.glob(str(input)+'/'+os.listdir(str(input))[1]+'/BAND*/*.MS')

#run copying function

print 'Copying CORRECTED to DATA'

copyNDPPP(snapshot1_file_list,'copy') 
os.system('mv '+str(input)+'/'+os.listdir(str(input))[0]+'/BAND*/*.MS.copy '+str(input)+'/1stsnap')
copyNDPPP(snapshot2_file_list,'copy') 
os.system('mv '+str(input)+'/'+os.listdir(str(input))[1]+'/BAND*/*.MS.copy '+str(input)+'/2ndsnap')

#get list of copied MS for both snapshots

snapshot1_copy_list = glob.glob(str(input)+'/1stsnap/*.MS.copy')
snapshot2_copy_list = glob.glob(str(input)+'/2ndsnap/*.MS.copy')

#run RMextract on dataset, once for each snapshot

print 'Running RM extract'

createRMparm(snapshot1_copy_list,1)
os.system('mv RMPARMDB.1SNAPSHOT '+str(input))
createRMparm(snapshot2_copy_list,2)
os.system('mv RMPARMDB.2SNAPSHOT '+str(input))


#output RM extract info

inputparmdb1 = pdb.parmdb(str(input)+'/RMPARMDB.1SNAPSHOT') 
inputparmdb2 = pdb.parmdb(str(input)+'/RMPARMDB.2SNAPSHOT')

length1=len(inputparmdb1.getNames())
length2=len(inputparmdb2.getNames())

#some code here to find the average RM from the parmdb files and output it onto a text file.

fullavgval1 = np.array([])
for i in range(length1):
	value = np.mean(inputparmdb1.getValuesGrid(inputparmdb1.getNames()[i])[inputparmdb1.getNames()[i]]['values']) #find the mean RM correction of a single station for the snapshot 
	fullavgval1 = np.append(fullavgval1,value)
finalavg1 = np.mean(fullavgval1)

fullavgval2 = np.array([])
for i in range(length2):
        value = np.mean(inputparmdb2.getValuesGrid(inputparmdb2.getNames()[i])[inputparmdb2.getNames()[i]]['values']) #find the mean RM correction of a single station for the	snapshot
        fullavgval2 = np.append(fullavgval2,value)
finalavg2 = np.mean(fullavgval2)

finalavg = (finalavg1+finalavg2)/2

outfile = open(str(input)+'/AVERAGERM_'+str(input)+'.txt', 'w')
outfile.write('Average RM correction for Field '+str(input)+' is '+str(finalavg)+' rad per meter squared')
outfile.close()

#create dummy skymodel needed

createdummyskymodel(snapshot1_copy_list)
os.system('mv dummyphasecentre.skymodel '+str(input)+'/')

# run BBS to apply RM calibration
# At the moment runs 3 threads for each snapshot
#Can easily be changed from the code below 

workers = Pool(processes=3)
workers.map(testcal1,snapshot1_copy_list)
workers = Pool(processes=3)
workers.map(testcal2,snapshot2_copy_list)

# copy out from corrected to data (save data)

os.system('mkdir '+str(input)+'/1stfinalsnapver')
os.system('mkdir '+str(input)+'/2ndfinalsnapver')

copyNDPPP(snapshot1_copy_list,'final')
os.system('mv '+str(input)+'/1stsnap/*.final '+str(input)+'/1stfinalsnapver') #ok
copyNDPPP(snapshot2_copy_list,'final')
os.system('mv '+str(input)+'/2ndsnap/*.final '+str(input)+'/2ndfinalsnapver') #ok

os.system('rm -rf '+str(input)+'/*snap')

print 'FINISHING'

end = time.time()

print'process took '+str(end - start)+' seconds to complete'
