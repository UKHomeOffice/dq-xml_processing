#!/usr/bin/env python

##############################################################################################################################
# DQ_IL2_Prep_XML_files.py
#
# AUTHOR:      Stewart Lee
# DATE:        2016/05/11
#
# Loads files source folder to target folder based on a max batch size parameter
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
import os, re, time, sys, shutil, fileinput, datetime, ConfigParser, multiprocessing
import itertools
from multiprocessing import Pool, freeze_support

### GLOBAL VARIABLES #########################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
##############################################################################################################################

def move_file(multiprocessing_pool_vars):
    
    filename=multiprocessing_pool_vars[0]
    from_dir=multiprocessing_pool_vars[1]
    to_dir=multiprocessing_pool_vars[2]

    from_filename=os.path.join(from_dir,filename)
    to_filename=os.path.join(to_dir,filename)
    
    shutil.move(from_filename,to_filename)

    return [True, os.path.basename(from_filename)]

# Takes a list of list objects e.g. [[False, <error msg>],[True, <error msg>]] and outputs to log when an error has been encountered
def check_multiprocessing_errors(results_list):
    NO_OF_ERRORS=0
    for result in results_list:
        ERROR=result[0]
        DETAILS=result[1]
        if not ERROR:
            NO_OF_ERRORS+=1
            add_log_entry('MULTIPROCESSING ERROR CHECKING', 'Error: ' + str(DETAILS))
    add_log_entry('MULTIPROCESSING ERROR CHECKING', str(NO_OF_ERRORS) + ' error(s)')

def add_log_entry(log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    if DEBUG: print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    LOGFILE.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def main(argv):

    ### GLOBAL DEBUG VARIABLE### #################################################################################################
    global DEBUG, LOGFILE
    DEBUG=1
    
    ### OTHER VARIABLES ##########################################################################################################
    STARTTIME = datetime.datetime.now()

    CONFIG_FILE='DQ_IL2_Config.ini'
    DEFAULT_SECTION='DEFAULT'
    CUSTOM_SECTION='DQ_IL2_Prep_XML_files'
    
    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)
    
    ROOT_DIR            = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))          # Root folder for all scripts, logs, temp folder, etc.
    SOURCE_FILE_DIR     = re.sub("/*$","/",config.get(CUSTOM_SECTION,'SOURCE_FILE_DIR'))    # Location of the source xml
    XML_DIR             = re.sub("/*$","/",config.get(CUSTOM_SECTION,'XML_DIR'))            # Location of the source xml
    MAX_XML_BATCH_SIZE  = int(config.get(CUSTOM_SECTION,'MAX_XML_BATCH_SIZE'))              # Controls max batch size - e.g. if set to 100, a max of 100 files will be processed
    NO_OF_PROCESSES     = int(config.get(CUSTOM_SECTION,'NO_OF_PROCESSES'))                 # No. of processes
    DEBUG               = int(config.get(CUSTOM_SECTION,'DEBUG'))                           # Used to control output to the console (Default=1, i.e. output)

    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    
    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_Prep_XML_files_' + YYYYMMDDSTR + '.log'
    LOGFILE = open(LOGFILENAME, 'a')   
    LOGFILE.write('\n--------------------------------------------------------------------\n')
    add_log_entry('*** RUN START ***', os.path.basename(sys.argv[0]))
    LOGFILE.write('--------------------------------------------------------------------\n')

    ##############################################################################################################################
    # Move files
    ##############################################################################################################################
    print '\n*** Move files to inprocess folder'

    source_dir_list =  [f for f in os.listdir(SOURCE_FILE_DIR) if f.endswith('.xml')]
    source_dir_list.sort()
         
    batch_dir_list = [f for f in os.listdir(XML_DIR) if f.endswith('.xml')]
    CURRENT_BATCH_SIZE = len(batch_dir_list)
    BATCH_DIFF=MAX_XML_BATCH_SIZE-CURRENT_BATCH_SIZE
    pool = multiprocessing.Pool(NO_OF_PROCESSES)
    if BATCH_DIFF<=0:
        add_log_entry('PREPARING BATCH', str(CURRENT_BATCH_SIZE) + ' xml file(s) present in ' + XML_DIR + ' - no files added')
        
    elif source_dir_list: # If files exist for this filetype in the FTP_LANDING_ZONE
        
        results=pool.map(move_file, itertools.izip(source_dir_list[:BATCH_DIFF],itertools.repeat(SOURCE_FILE_DIR),
                                                                                itertools.repeat(XML_DIR)))
        add_log_entry('MOVED FILES', 'Processed ' + str(len(results)) + ' file(s)')
        # Check multiprocessing results
        check_multiprocessing_errors(results)
    else:
        add_log_entry('PREPARING BATCH', 'No files available')

    ##############################################################################################################################
    # SCRIPT END
    ##############################################################################################################################
    ENDTIME = datetime.datetime.now()
    delta = ENDTIME - STARTTIME
    LOGFILE.write('--------------------------------------------------------------------\n')
    add_log_entry('*** RUN COMPLETE ***', time.strftime("%Y%m%d%H%M%S") + ' (ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s))')
    LOGFILE.write('--------------------------------------------------------------------\n')
    LOGFILE.close()

    
# Run the main function
if __name__ == "__main__":
	main(sys.argv[1:])
