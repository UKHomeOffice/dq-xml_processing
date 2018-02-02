#!/usr/bin/env python

##############################################################################################################################
# DQ_IL2_DB_GP_Load.py
#
# AUTHOR:      Stewart Lee
# DATE:        2015/06/01
#
# This script moves files to an inprocess folder, runs a batch file and archives files
#
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
import os, re, time, sys, shutil, fileinput, getopt, datetime
#from datetime import datetime

### GLOBAL VARIABLES #########################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
##############################################################################################################################

def add_log_entry(log_obj, log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    if DEBUG: print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    log_obj.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def main(argv):

    ### GLOBAL DEBUG VARIABLE### #################################################################################################
    global DEBUG
    DEBUG=1
    
    ### THESE FOLDERS MUST EXIST #################################################################################################
    ROOT_DIR=''
    DOS_BATCH_FILE=''

    MAX_GPLOAD_RETRIES=3
    SLEEPTIME=3
    ### OTHER VARIABLES ##########################################################################################################
    STARTTIME = datetime.datetime.now()

    try:
          opts, args = getopt.getopt(argv,"dr:b:s:t:")
    except getopt.GetoptError:
        print 'Opt error'
        sys.exit(2)
    for opt, arg in opts:
          if opt == '-d':
             DEBUG=True
          elif opt in ("-r"):
             ROOT_DIR = re.sub("/*$","/",arg)
          elif opt in ("-b"):
             DOS_BATCH_FILE = arg
          elif opt in ("-t"):
             SLEEPTIME = int(arg)
    
    TARGET_FILE_DIR=os.path.join(ROOT_DIR, 'tmp/')
    ARCHIVE_FILE_DIR=os.path.join(ROOT_DIR, 'archive/')
    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    SOURCE_FILE_DIR=os.path.join(ROOT_DIR, 'csv/')
    INPROCESS_FILE_DIR=os.path.join(ROOT_DIR, 'xml_inprocess/')
    
    FILE_COUNTER=0

    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_DB_GP_LOAD_XML_' + YYYYMMDDSTR + '.log'
    LOGFILE = open(LOGFILENAME, 'a')   
    LOGFILE.write('\n--------------------------------------------------------------------\n')
    add_log_entry(LOGFILE,'*** RUN START ***', os.path.basename(sys.argv[0]))
    LOGFILE.write('--------------------------------------------------------------------\n')

    ##############################################################################################################################
    # Exit if no files
    ##############################################################################################################################
    print '\n*** Exit if no files'
    source_dir_list = [f for f in os.listdir(INPROCESS_FILE_DIR) if f.endswith('.xml.MOD')]
    if not source_dir_list:
       add_log_entry(LOGFILE,'EXIT IF NO FILES', 'No source files')
       LOGFILE.write('--------------------------------------------------------------------\n')
       add_log_entry(LOGFILE,'*** RUN COMPLETE ***', time.strftime("%Y%m%d%H%M%S"))
       LOGFILE.write('--------------------------------------------------------------------\n')
       LOGFILE.close()
       sys.exit(0)

    ##############################################################################################################################
    # Run batch file
    ##############################################################################################################################
    print '\n*** Run the batch file: ' + DOS_BATCH_FILE

    RETRY_COUNT=0
    
    while True:

        os.system("cd " + ROOT_DIR + "/scripts")
        RETURN_CODE=os.system(DOS_BATCH_FILE)
        
        # GPLOAD RETURN CODES:
        # 0 No Error
        # 1 Warning
        # 2 Failure
        if RETURN_CODE not in (0,1):
            add_log_entry(LOGFILE,'GPLOAD BATCH FILE FAILED', DOS_BATCH_FILE + ' failed: ' + str(RETURN_CODE))

            if RETRY_COUNT < MAX_GPLOAD_RETRIES:
                RETRY_COUNT += 1
                add_log_entry(LOGFILE,'GPLOAD', 'Retry attempt: ' + str(RETRY_COUNT))
                time.sleep(SLEEPTIME)
            else:
                ENDTIME = datetime.datetime.now()
                delta = ENDTIME - STARTTIME
                LOGFILE.write('--------------------------------------------------------------------\n')
                add_log_entry(LOGFILE,'*** RUN FAILED ***', time.strftime("%Y%m%d%H%M%S") + ' (ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s))')
                LOGFILE.write('--------------------------------------------------------------------\n')
                LOGFILE.close()
                sys.exit(1)
        else:
            break

    add_log_entry(LOGFILE,'GPLOAD BATCH FILE RUN', 'COMPLETED SUCCESSFULLY')
    
    ##############################################################################################################################
    # SCRIPT END
    ##############################################################################################################################
    ENDTIME = datetime.datetime.now()
    delta = ENDTIME - STARTTIME
    LOGFILE.write('--------------------------------------------------------------------\n')
    add_log_entry(LOGFILE,'*** RUN COMPLETE ***', time.strftime("%Y%m%d%H%M%S") + ' (ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s))')
    LOGFILE.write('--------------------------------------------------------------------\n')
    LOGFILE.close()

    
# Run the main function
if __name__ == "__main__":
	main(sys.argv[1:])
