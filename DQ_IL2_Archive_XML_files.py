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

def overwrite_file(from_file, to_file):
    if os.path.exists(to_file):
       os.remove(to_file)
    os.rename(from_file,to_file)

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

    ### OTHER VARIABLES ##########################################################################################################
    STARTTIME = datetime.datetime.now()

    try:
          opts, args = getopt.getopt(argv,"dr:s:")
    except getopt.GetoptError:
        print 'Opt error'
        sys.exit(2)
    for opt, arg in opts:
          if opt == '-d':
             DEBUG=True
          elif opt in ("-r"):
             ROOT_DIR = re.sub("/*$","/",arg)
    
    ARCHIVE_FILE_DIR=os.path.join(ROOT_DIR, 'archive/')
    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    XML_DIR=os.path.join(ROOT_DIR, 'xml/')
    XML_INPROCESS_DIR=os.path.join(ROOT_DIR, 'xml_inprocess/')
    
    FILE_COUNTER=0

    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_Archive_XML_files_' + YYYYMMDDSTR + '.log'
    LOGFILE = open(LOGFILENAME, 'a')   
    LOGFILE.write('\n--------------------------------------------------------------------\n')
    add_log_entry(LOGFILE,'*** RUN START ***', os.path.basename(sys.argv[0]))
    LOGFILE.write('--------------------------------------------------------------------\n')

    ##############################################################################################################################
    # Cleanup xml files
    ##############################################################################################################################
    print '\n*** Cleanup xml files'
    xml_dir_list = [f for f in os.listdir(XML_DIR) if f.endswith('.xml')]
    if xml_dir_list:

       for file in xml_dir_list:
           os.remove(XML_DIR + file)
           FILE_COUNTER+=1
       add_log_entry(LOGFILE,'CLEANUP XML FILES', str(FILE_COUNTER) + ' files cleaned up')
    else:
       add_log_entry(LOGFILE,'CLEANUP XML FILES', 'No source files')

    


    ##############################################################################################################################
    # Cleanup xml mod files
    ##############################################################################################################################
    print '\n*** Cleanup inprocess files'
    xml_dir_list = [f for f in os.listdir(XML_INPROCESS_DIR) if f.endswith('.xml.MOD')]
    if xml_dir_list:
       FILE_COUNTER=0
       for file in xml_dir_list:
           os.remove(XML_INPROCESS_DIR + file)
           FILE_COUNTER+=1
       add_log_entry(LOGFILE,'CLEANUP INPROCESS FILES', str(FILE_COUNTER) + ' files cleaned up')
    else:
       add_log_entry(LOGFILE,'CLEANUP INPROCESS FILES', 'No source files')

    


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
