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
import os, re, time, sys, shutil, fileinput, datetime, ConfigParser
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

    CONFIG_FILE='DQ_IL2_Config.ini'
    DEFAULT_SECTION='DEFAULT'
    CUSTOM_SECTION='DQ_IL2_Archive_GA_files'

    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)
   
    ROOT_DIR                = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))
    GA_FILE_DIR             = re.sub("/*$","/",config.get(CUSTOM_SECTION,'GA_FILE_DIR'))
    GA_CONCAT_OUTPUT_DIR    = re.sub("/*$","/",config.get(CUSTOM_SECTION,'GA_CONCAT_OUTPUT'))
    GA_XML_ARCHIVE_DIR      = re.sub("/*$","/",config.get(CUSTOM_SECTION,'GA_XML_ARCHIVE'))
    DEBUG                   = int(config.get(CUSTOM_SECTION,'DEBUG'))
    
    ARCHIVE_FILE_DIR=os.path.join(ROOT_DIR, 'archive/')
    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    
    FILE_COUNTER=0

    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_Archive_GA_files_' + YYYYMMDDSTR + '.log'
    LOGFILE = open(LOGFILENAME, 'a')   
    LOGFILE.write('\n--------------------------------------------------------------------\n')
    add_log_entry(LOGFILE,'*** RUN START ***', os.path.basename(sys.argv[0]))
    LOGFILE.write('--------------------------------------------------------------------\n')

    ##############################################################################################################################
    # Cleanup xml files
    ##############################################################################################################################
    print '\n*** Cleanup xml files'
    ga_dir_list = [f for f in os.listdir(GA_FILE_DIR) if f.endswith('.xml')]
    if ga_dir_list:

       for filename in ga_dir_list:
           print GA_FILE_DIR + filename
           overwrite_file(GA_FILE_DIR + filename, GA_XML_ARCHIVE_DIR + filename)
           FILE_COUNTER+=1
       add_log_entry(LOGFILE,'CLEANUP XML FILES', str(FILE_COUNTER) + ' files archived')
    else:
       add_log_entry(LOGFILE,'CLEANUP XML FILES', 'No source files')

    ##############################################################################################################################
    # Cleanup xml mod files
    ##############################################################################################################################
    print '\n*** Cleanup inprocess files'
    ga_concat_dir_list = [f for f in os.listdir(GA_CONCAT_OUTPUT_DIR) if f.endswith('.xml.MOD')]
    if ga_concat_dir_list:
       FILE_COUNTER=0
       for filename in ga_concat_dir_list:
           os.remove(GA_CONCAT_OUTPUT_DIR + filename)
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
