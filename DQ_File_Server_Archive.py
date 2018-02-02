#!/usr/bin/env python

##############################################################################################################################
# 
#
# AUTHOR:      Stewart Lee
# DATE:        2015/12/21
#
# DESCRIPTION:
#
# This script does the following:
#
# DQ_File_Server_Archive.py -s C:\Users\lees\Desktop\env\archive -r ".*_[0-9]{8}_[0-9]{4}_[0-9]{4}\.zip$" -l C:\Users\lees\Desktop\env\log -d 7
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

def overwrite_file(from_file, to_file):
    if os.path.exists(to_file):
       os.remove(to_file)
    shutil.move(from_file,to_file)

def add_log_entry(log_obj, log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    log_obj.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def print_usage():
    print 'Correct usage: ' + os.path.basename(sys.argv[0])
    print 'Enter pathnames separated with forward slashes ("/")'
    print 'Options: -h display this message'
    print '         -d number of days to keep (e.g. 30 is keep the last 30 days)'
    print '         -s source folder (the folder containing the files to delete)'
    print '         -r the regex pattern to search for files to delete (e.g. .*\.zip)'
    print '         -l the log folder (folder path with forward slashes)'
    print '         -w the path to the WFLS archive folder (e.g. \\10.000.000.10\my_archived_files)'
    print '         -t time based delete flag (delete based on file last modified date rather than filename date)'
    sys.exit(1)
    
def main(argv):
    
    ### THESE FOLDERS MUST EXIST #################################################################################################
    SOURCE_FILE_DIR=''
    ### OTHER VARIABLES ##########################################################################################################
    STARTTIME = datetime.datetime.now()
    TIME_BASED_DELETE=False
    DAYS_TO_KEEP=''
    FILENAME_REGEX=''
    LOGFILE_DIR=''
    WFLS_PATH=''

    try:
          opts, args = getopt.getopt(argv,"hd:s:r:l:w:t")
    except getopt.GetoptError:
          print_usage()
          sys.exit(2)
    for opt, arg in opts:
          if opt == '-h':
             print_usage()
             sys.exit(1)
          elif opt == '-d':
             DAYS_TO_KEEP = int(arg)
          elif opt in ("-s"):
             SOURCE_FILE_DIR = re.sub("/*$","/",arg)
          elif opt in ("-r"):
             FILENAME_REGEX = arg
          elif opt in ("-l"):
             LOGFILE_DIR = re.sub("/*$","/",arg)
          elif opt in ("-w"):
             WFLS_PATH = re.sub("/*$","/",arg)
          elif opt in ("-t"):
             TIME_BASED_DELETE=True

    if DAYS_TO_KEEP=='' or FILENAME_REGEX=='' or LOGFILE_DIR=='' or WFLS_PATH=='':
        print 'Required arguments blank'
        print_usage()
    
    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_File_Server_Archive_' + YYYYMMDDSTR + '.log'
    LOGFILE = open(LOGFILENAME, 'a')  

    add_log_entry(LOGFILE,'*** RUN START ***', os.path.basename(sys.argv[0]))

    ##############################################################################################################################
    # Cleanup
    ##############################################################################################################################
    print '\n*** Moving files'

    EARLIEST_DATE_TO_KEEP = int(datetime.datetime.strftime(datetime.datetime.now() - timedelta(days=DAYS_TO_KEEP),'%Y%m%d')) # YYYYMMDD (string)
    
    source_dir_list = [f for f in os.listdir(SOURCE_FILE_DIR) if (re.match(FILENAME_REGEX, f))]

    if source_dir_list:
       FILECOUNT=0
       for filename in source_dir_list:
             FILE_IS_OLD = False

             if not TIME_BASED_DELETE:
                 filedate = re.split('_',filename)[1]
                 filename_split = filename.split('_')
                 filename_YYYYMMDD = int(filename_split[1])

                 FILE_IS_OLD= filename_YYYYMMDD < EARLIEST_DATE_TO_KEEP
             else:
                 FILE_IS_OLD=os.stat(SOURCE_FILE_DIR + filename).st_mtime < time.time() - (int(DAYS_TO_KEEP) * 86400)
                     
             if FILE_IS_OLD:
                 if not os.path.exists(WFLS_PATH + filedate):
                     os.mkdir(WFLS_PATH + filedate + '/')
                 print 'Moving: ' + filename

                 #shutil.move(full_filepath, WFLS_PATH + filedate + '/' + filename)
                 FILECOUNT+=1

         
       add_log_entry(LOGFILE,'FILES MOVED', str(FILECOUNT))

       
    ##############################################################################################################################
    # SCRIPT END
    ##############################################################################################################################
    ENDTIME = datetime.datetime.now()
    delta = ENDTIME - STARTTIME
    add_log_entry(LOGFILE,'*** RUN COMPLETE ***', time.strftime("%Y%m%d%H%M%S") + ' (ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s))')
    LOGFILE.close()
    
# Run the main function
if __name__ == "__main__":
	main(sys.argv[1:])
