#!/usr/bin/env python

##############################################################################################################################
# DQ_IL2_GA_Archive_JSON
#
# AUTHOR:      Stewart Lee
# DATE:        2016/08/15
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
import os, re, time, sys, shutil, datetime, ConfigParser

### GLOBAL VARIABLES #########################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
##############################################################################################################################

def add_log_entry(log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    if DEBUG: print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    LOGFILE.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def move_files(SOURCE_DIR, TARGET_DIR, FILE_EXT):
    file_list = [f for f in os.listdir(SOURCE_DIR) if f.endswith(FILE_EXT)]
    if file_list:
       FILE_COUNTER=0
       for fname in file_list:
           shutil.move(SOURCE_DIR + fname, TARGET_DIR + fname)
           FILE_COUNTER+=1
       add_log_entry('MOVE FILES', str(FILE_COUNTER) + ' files moved')
    else:
       add_log_entry('MOVE FILES', 'No source files')
    return True

def remove_files(SOURCE_DIR, FILE_EXT):
    file_list = [f for f in os.listdir(SOURCE_DIR) if f.endswith(FILE_EXT)]
    if file_list:
       FILE_COUNTER=0
       for fname in file_list:
           os.remove(SOURCE_DIR + fname)
           FILE_COUNTER+=1
       add_log_entry('REMOVE FILES', str(FILE_COUNTER) + ' files removed')
    else:
       add_log_entry('REMOVE FILES', 'No source files')
    return True

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
    CUSTOM_SECTION='DQ_IL2_GA_Archive_Files'

    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)
   
    ROOT_DIR                = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))
    GA_SOURCE_DIR           = re.sub("/*$","/",config.get(CUSTOM_SECTION,'GA_SOURCE_DIR'))
    GA_OUTPUT_DIR           = re.sub("/*$","/",config.get(CUSTOM_SECTION,'GA_OUTPUT_DIR'))
    GA_ARCHIVE_DIR          = re.sub("/*$","/",config.get(CUSTOM_SECTION,'GA_ARCHIVE_DIR'))
    FPL_SOURCE_DIR          = re.sub("/*$","/",config.get(CUSTOM_SECTION,'FPL_SOURCE_DIR'))
    FPL_OUTPUT_DIR          = re.sub("/*$","/",config.get(CUSTOM_SECTION,'FPL_OUTPUT_DIR'))
    FPL_ARCHIVE_DIR         = re.sub("/*$","/",config.get(CUSTOM_SECTION,'FPL_ARCHIVE_DIR'))
    DEBUG                   = int(config.get(CUSTOM_SECTION,'DEBUG'))
    
    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    
    FILE_COUNTER=0

    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_GA_Archive_Files_' + YYYYMMDDSTR + '.log'
    global LOGFILE
    LOGFILE = open(LOGFILENAME, 'a')   
    add_log_entry('*** RUN START ***', os.path.basename(sys.argv[0]))

    ##############################################################################################################################
    # Cleanup files
    ##############################################################################################################################
    print '\n*** Cleanup files'
    move_files(GA_SOURCE_DIR, GA_ARCHIVE_DIR, '.xml')
    remove_files(GA_OUTPUT_DIR, '.xml.MOD')



    ##############################################################################################################################
    # SCRIPT END
    ##############################################################################################################################
    ENDTIME = datetime.datetime.now()
    delta = ENDTIME - STARTTIME
    add_log_entry('*** RUN COMPLETE ***', time.strftime("%Y%m%d%H%M%S") + ' (ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s))')
    LOGFILE.close()

    
# Run the main function
if __name__ == "__main__":
	main(sys.argv[1:])
