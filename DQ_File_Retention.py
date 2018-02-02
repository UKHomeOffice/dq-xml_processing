#!/usr/bin/env python

##############################################################################################################################
# 
#
# AUTHOR:      Stewart Lee
# DATE:        20160520
#
# DESCRIPTION:
#
# This script does the following:
#
# - Retains/Archives files based on input given in the config file
# - This only works with zip files and log files! (e.g. FILETYPE_YYYYMMDD_HHMM_NNNN.zip or LOG_NAME_YYYYMMDD[HH].zip)
#
# - Options:
#
#   -h display this message
#   -c the config file
#   -l the log folder (folder path with forward slashes)
#   -f the logging frequency
#   -t to test (no files will be moved/removed)
#   -v for verbose logging 
#
# e.g. DQ_File_Retention.py -c DQ_File_Retention.config -l C:\log -f 100 
#
# Reads from the config file DQ_File_Retention.config, logs to C:\log, and logs every 100 rows of output.
# 
# Sample config file:
# 
#   ACTION          REGEX                                DAYS    TIME/FILE SOURCE FOLDER                           TARGET FOLDER                               ACTIVE
#   =============== ==================================== ======= ========= ======================================= =========================================== ======
#   DELETE_FILE	    |PARSED_.*\.zip		         |10	 |t        |C:\archive	                           |N/A                                        |Y    
#   DELETE_FOLDER   |[0-9]{8}                            |365    |f        |\\00.00.000.00\raw_filestore           |N/A                                        |Y    
#   ARCHIVE_FILE    |RAW_.*\.zip			 |10	 |f        |C:\archive 	                           |\\development\dq\                          |Y
#   MOVE_FILE       |DQ_CC_DETAILS_[0-9]{8}\.log	 |1	 |t        |C:\log	                           |C:\Users\lees\Desktop\env\il2\log          |Y
#   
# Comma separated file (whitespace is ignored).
#
# 
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
from __future__ import with_statement
import os, zipfile, re, time, sys, shutil, fileinput, getopt, datetime
from datetime import timedelta

#from datetime import datetime

### GLOBAL VARIABLES #########################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
##############################################################################################################################

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def add_log_entry(log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    print curr_time + '\t' + log_summary.ljust(50,' ') + '\t' + log_msg
    LOGFILE.write(curr_time + '\t' + log_summary.ljust(50,' ') + '\t' + log_msg + '\n')

def print_usage():
    print 'Correct usage: ' + os.path.basename(sys.argv[0])
    print 'Enter pathnames separated with forward slashes ("/")'
    print 'Options: -h display this message'
    print '         -c the config file'
    print '         -l the log folder (folder path with forward slashes)'
    print '         -f the logging frequency'
    print '         -t to test (no files will be moved/removed)'
    sys.exit(1)

def handle_exception(e):
    add_log_entry('','ERROR: %s' % (str(e)))
    sys.exit(1)

def main(argv):
    ### VARIABLES ################################################################################################################
    STARTTIME = datetime.datetime.now()
    TIME_BASED_DELETE=False
    DAYS_TO_KEEP=''
    LOGFILE_DIR=''
    CONFIG_FILE=''
    CONFIG_FILE_DELIM='|'
    LOGGING_FREQUENCY=100
    TEST=False
    VERBOSE_LOGGING=False
    TEST_MESSAGE=''

    try:
        opts, args = getopt.getopt(argv,"hc:l:f:d:tv")
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit(1)
        elif opt in ("-c"):
            CONFIG_FILE = arg
        elif opt in ("-d"):
            CONFIG_FILE_DELIM = arg
        elif opt in ("-l"):
            LOGFILE_DIR = re.sub("/*$","/",arg)
        elif opt in ("-f"):
            LOGGING_FREQUENCY = int(arg)
        elif opt in ("-t"):
            TEST = True
        elif opt in ("-v"):
            VERBOSE_LOGGING = True
             
    if CONFIG_FILE == '':
        print 'Missing configuration file argument (-c)'
        sys.exit(1)
    if LOGFILE_DIR == '':
        print 'Missing log file dir argument (-l)'
        sys.exit(1)
    if TEST:
        TEST_MESSAGE='######## TEST ########'
    
    
    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_File_Retention_' + YYYYMMDDSTR + '.log'
    global LOGFILE
    LOGFILE = open(LOGFILENAME, 'a')  

    add_log_entry('*** RUN START ***', os.path.basename(sys.argv[0]))

    ##############################################################################################################################
    # Cleanup
    ##############################################################################################################################
    print '\n*** Moving files'

    CONFIG_FILE_LEN=file_len(CONFIG_FILE)
    COUNTER=1
    
    for line in open(CONFIG_FILE,'rb').readlines():

        line_split=line.split(CONFIG_FILE_DELIM)
        ACTION=line_split[0].strip()
        FILENAME_REGEX=line_split[1].strip()
        try:
            DAYS_TO_KEEP=int(line_split[2].strip())
        except Exception, e:
            handle_exception(e)
        METHOD=line_split[3].strip()
        SOURCE_FILE_DIR=line_split[4].strip()
        TARGET_FILE_DIR=line_split[5].strip()
        ACTIVE=line_split[6].strip()

        TIME_BASED_CHECK=METHOD.upper()=='T'

        if ACTIVE.upper()=='Y':
            EARLIEST_DATE_TO_KEEP = int(datetime.datetime.strftime(datetime.datetime.now() - timedelta(days=DAYS_TO_KEEP),'%Y%m%d')) # YYYYMMDD (string)

            try:
                source_dir_list = [f for f in os.listdir(SOURCE_FILE_DIR) if (re.match(FILENAME_REGEX, f))]
            except Exception, e:
                handle_exception(e)
            
            TOTAL_FILES_TO_ARCHIVE=len(source_dir_list)

            add_log_entry(FILENAME_REGEX, '(%s/%s) %s item(s) from "%s" older than %s day(s) old ' % (COUNTER,CONFIG_FILE_LEN, ACTION, os.path.basename(SOURCE_FILE_DIR),DAYS_TO_KEEP ))
            
            if source_dir_list:
                FILECOUNT=0
                OLDFILECOUNT=0
                for f in source_dir_list:
                    FILE_IS_OLD = False
                    filename_YYYYMMDD=None
                    full_filepath=os.path.join(SOURCE_FILE_DIR,f)

                    if TIME_BASED_CHECK:
                        filename_YYYYMMDD=datetime.datetime.strftime(datetime.datetime.fromtimestamp(os.path.getmtime(full_filepath)),'%Y%m%d')
                    else:
                        if re.search('[0-9]{8}',f): # Match the first YYYYMMDD of the filename/folder
                            filedate_match=re.search('[0-9]{8}',f)
                            filename_YYYYMMDD = f[filedate_match.start():filedate_match.end()]
                        elif re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}',f): # Handle YYYY-MM-DD formats
                            filedate_match=re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}',f)
                            filename_YYYYMMDD = f[filedate_match.start():filedate_match.end()].replace('-','')
                        elif re.search('[0-9]{4}_[0-9]{2}_[0-9]{2}',f): # Handle YYYY_MM_DD formats
                            filedate_match=re.search('[0-9]{4}_[0-9]{2}_[0-9]{2}',f)
                            filename_YYYYMMDD = f[filedate_match.start():filedate_match.end()].replace('_','')

                    try:
                        datetime.datetime.strptime(filename_YYYYMMDD, "%Y%m%d").date()
                    except Exception,e:
                        add_log_entry('','Failed converting date for: %s'%(f))
                        handle_exception(e)
                        
                    FILE_IS_OLD = int(filename_YYYYMMDD) < EARLIEST_DATE_TO_KEEP
                    
                    if FILECOUNT % LOGGING_FREQUENCY == 0:
                        add_log_entry('PROCESSED', str(FILECOUNT) + ' item(s) of ' + str(TOTAL_FILES_TO_ARCHIVE))
                        
                    if FILE_IS_OLD:

                        # Handle the deletion of files
                        if  ACTION == 'DELETE_FILE':
                            if not TEST:
                                try:
                                    os.remove(full_filepath)
                                except Exception, e:
                                    handle_exception(e)
                            if VERBOSE_LOGGING: add_log_entry(TEST_MESSAGE,'Deleted file %s' % (full_filepath)) 
                                
                        # Handle the deletion of folders        
                        elif ACTION == 'DELETE_FOLDER':
                            if not TEST:
                                try:
                                    shutil.rmtree(full_filepath)
                                except Exception, e:
                                    handle_exception(e)
                            if VERBOSE_LOGGING: add_log_entry(TEST_MESSAGE,'Deleted folder %s' % (full_filepath))
                                
                        # Handle the moving of files
                        elif ACTION == 'MOVE_FILE':
                            if not TEST:
                                try:
                                    shutil.move(full_filepath, os.path.join(TARGET_FILE_DIR,f))
                                except Exception, e:
                                    handle_exception(e)
                            if VERBOSE_LOGGING: add_log_entry(TEST_MESSAGE,'Moved file %s' % (full_filepath))

                        # Handle the archiving of files
                        # Create a folder YYYYMMDD if it doesn't already exist and move files into it
                        elif ACTION == 'ARCHIVE_FILE':
                            if not TEST:
                                try:
                                    if not os.path.exists(os.path.join(TARGET_FILE_DIR,filename_YYYYMMDD)):
                                        os.mkdir(os.path.join(TARGET_FILE_DIR,filename_YYYYMMDD))
                                    shutil.move(full_filepath, os.path.join(TARGET_FILE_DIR,filename_YYYYMMDD,f))
                                except Exception, e:
                                    handle_exception(e)
                            if VERBOSE_LOGGING: add_log_entry(TEST_MESSAGE,'Archived %s to %s' % (full_filepath, os.path.join(TARGET_FILE_DIR,filename_YYYYMMDD)))
                                
                        else:
                            add_log_entry(TEST_MESSAGE, 'Unknown action: "%s"' % (ACTION))
                            sys.exit(1)

                        OLDFILECOUNT+=1
                            
                    FILECOUNT+=1
                        
                # END FOR LOOP
                add_log_entry(TEST_MESSAGE,str(OLDFILECOUNT) + ' item(s) actioned')
            else: # if not source_dir_list
                add_log_entry('','No items')
               
        else: # if not ACTIVE.upper()=='Y'
            add_log_entry(FILENAME_REGEX, '(%s/%s) %s item(s) older than %s day(s) old (INACTIVE)' % (COUNTER,CONFIG_FILE_LEN, ACTION, DAYS_TO_KEEP ))
              
        COUNTER+=1
        
    # END FOR LOOP   

    ##############################################################################################################################
    # SCRIPT END
    ##############################################################################################################################
    ENDTIME = datetime.datetime.now()
    delta = ENDTIME - STARTTIME
    add_log_entry('*** RUN COMPLETE ***', time.strftime("%Y%m%d%H%M%S") + ' (ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s))\n')
    LOGFILE.close()
    
# Run the main function
if __name__ == "__main__":
    main(sys.argv[1:])
