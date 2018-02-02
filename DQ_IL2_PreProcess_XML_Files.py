#!/usr/bin/env python

##############################################################################################################################
# DQ_IL2_PreProcess_XML_Files.py
#
# AUTHOR:       Stewart Lee
# DATE:         2016/05/20
# LAST UPDATED: 2016/12/05
#
# DESCRIPTION:  
#
# This script does the following:
#
# - Removes previously generated .xml.MOD files
# - PreProcesses xmls (concats files, i.e. in the format <xml>|<filename>) in multiprocessing
#
# UPDATES:
#
# 20161205      Due to "gpfdist" failing (not in this script) due to files of more than 1MB in size being received, this
#               script has been updated to reject any files more than 1MB (configurable) in size.  The reject folder
#               is also configurable.
#                   
##############################################################################################################################

### IMPORT PYTHON MODULES 
import os, zipfile, re, time, sys, shutil, getopt, ConfigParser, glob, ntpath, multiprocessing
from datetime import datetime
import itertools
from multiprocessing import Pool, freeze_support

##############################################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
YYYYMMDDHHSTR = time.strftime("%Y%m%d%H")
##############################################################################################################################

def batch_list(l, n):
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]

def concat_xml_files(multiprocessing_pool_vars):
    filename=multiprocessing_pool_vars[0]
    XML_DIR=multiprocessing_pool_vars[1]
    XML_INPROCESS_DIR=multiprocessing_pool_vars[2]
    MAX_FILESIZE_BYTES=multiprocessing_pool_vars[3]
    REJECT_DIR=multiprocessing_pool_vars[4]

    concat=None
    full_filename=os.path.join(XML_DIR,filename)
    filesize=int(os.stat(full_filename).st_size)
    if filesize < MAX_FILESIZE_BYTES:
        with open(full_filename) as f:
            concat = f.read() + '|' + ntpath.basename(filename)
    else:
        shutil.move(full_filename,os.path.join(REJECT_DIR,filename))
    
    return concat

# Takes a list of list objects e.g. [[False, <error msg>],[True, <error msg>]] and outputs to log when an error has been encountered
def check_multiprocessing_errors(results_list):
    NO_OF_ERRORS=0
    for result in results_list:
        SUCCESS=result[0]
        DETAILS=result[1]
        if not SUCCESS:
            NO_OF_ERRORS+=1
            add_log_entry('MULTIPROCESSING ERROR CHECKING', 'Error: ' + str(DETAILS))
    add_log_entry('MULTIPROCESSING ERROR CHECKING', str(NO_OF_ERRORS) + ' error(s)')

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def add_log_entry(log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    if DEBUG: print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    LOGFILE.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def main(argv):
    ### GLOBAL DEBUG VARIABLE #################################################################################################
    global DEBUG, LOGFILE
    DEBUG = 1
   
    STARTTIME = datetime.now()

    ### THESE FOLDERS MUST EXIST #################################################################################################
    ROOT_DIR=''
    ### OTHER VARIABLES ##########################################################################################################
    STARTTIME = datetime.now()

    CONFIG_FILE='DQ_IL2_Config.ini'
    DEFAULT_SECTION='DEFAULT'
    CUSTOM_SECTION='DQ_IL2_PreProcess_XML_files'
    
    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)
    
    ROOT_DIR            = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))          # Root folder for all scripts, logs, temp folder, etc.
    XML_DIR             = re.sub("/*$","/",config.get(CUSTOM_SECTION,'XML_DIR'))            # Location of the source xml
    XML_INPROCESS_DIR   = re.sub("/*$","/",config.get(CUSTOM_SECTION,'XML_INPROCESS_DIR'))  # Location of the source xml
    NO_OF_PROCESSES     = int(config.get(CUSTOM_SECTION,'NO_OF_PROCESSES'))                 # No. of processes
    BUFFER_LIMIT        = int(config.get(CUSTOM_SECTION,'BUFFER_LIMIT'))                    # Max no. of files to hold in memory before writing to file
    MAX_FILESIZE_BYTES  = int(config.get(CUSTOM_SECTION,'MAX_FILESIZE_BYTES'))              # Max filesize in bytes to accept, anything over this will be rejected
    REJECT_DIR          = config.get(CUSTOM_SECTION,'REJECT_DIR')                          # Max no. of files to hold in memory before writing to file
    DEBUG               = int(config.get(DEFAULT_SECTION,'DEBUG'))                          # Used to control output to the console (Default=1, i.e. output)
   
    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    OUTPUT_MOD_FILENAME=os.path.join(XML_INPROCESS_DIR,'PARSED_CONCAT_X.xml.MOD')
    
    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_PREPROCESS_XML_FILES_LOGFILE_' + YYYYMMDDSTR + '.log' # records general script output
    LOGFILE = open(LOGFILENAME, 'a')

    LOGFILE.write('\n--------------------------------------------------------------------\n')
    add_log_entry('*** RUN START ***',os.path.basename(sys.argv[0]))
    LOGFILE.write('--------------------------------------------------------------------\n')

    ##############################################################################################################################
    # Concat files
    ##############################################################################################################################

    print '\n*** Concat xml files'
    xml_inprocess_dir_list = [os.path.join(XML_INPROCESS_DIR,f) for f in os.listdir(XML_INPROCESS_DIR) if f.endswith(".xml.MOD")]

    xml_dir_list = [f for f in os.listdir(XML_DIR) if f.lower().endswith(".xml")]   

    open(OUTPUT_MOD_FILENAME, 'wb').close()
    
    BATCH_COUNTER=1
    RESULT_COUNTER=0

    BATCHES=batch_list(xml_dir_list, BUFFER_LIMIT)
    BATCHES_LENGTH=len(BATCHES)
    
    for batch in BATCHES:
        pool = multiprocessing.Pool(NO_OF_PROCESSES)
        
        if batch:
            with open(OUTPUT_MOD_FILENAME, 'ab') as f:
                for result in pool.map(concat_xml_files,    itertools.izip(batch,                            # ARG 1 (Filename)
                                                            itertools.repeat(XML_DIR),                       # ARG 2 (XML_DIR - input)
                                                            itertools.repeat(XML_INPROCESS_DIR),             # ARG 3 (XML_INPROCESS - output)
                                                            itertools.repeat(MAX_FILESIZE_BYTES),            # ARG 4 (MAX_FILESIZE_BYTES - filesize reject threshold)
                                                            itertools.repeat(REJECT_DIR)                     # ARG 5 (REJECT_DIR - output)
                                                        )):
                    # (filename, count) tuples from worker
                    if result is not None:
                        f.write(result + '\n')
                        RESULT_COUNTER+=1

            add_log_entry('CONCAT XML FILES', 'Batch %s of %s complete (%s file(s) processed)' % (BATCH_COUNTER,BATCHES_LENGTH,RESULT_COUNTER))
            
        BATCH_COUNTER+=1
        pool.close()
        pool.join()

    ##############################################################################################################################
    # SCRIPT END 
    ##############################################################################################################################
    ENDTIME = datetime.now()
    delta = ENDTIME - STARTTIME
    LOGFILE.write('--------------------------------------------------------------------\n')
    add_log_entry('*** RUN COMPLETE ***','ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s)')
    LOGFILE.write('--------------------------------------------------------------------\n')
    LOGFILE.close()

# Run the main function
if __name__ == "__main__":
    main(sys.argv[1:])
