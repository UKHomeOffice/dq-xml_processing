#!/usr/bin/env python

##############################################################################################################################
# DQ_IL2_index_raw_msg.py
#
# AUTHOR:      Stewart Lee
# DATE:        2016/06/27
#
# DESCRIPTION:
#
# This script does the following:
#
# - Indexes raw files in a csv (guid, filepath, zipfile, filename)
# - GPLOADs the files into the load_raw_message_guid table in GP
# - Due to an issue with the GPLOAD command intermittently hanging, logic was introduced to have an upper limit on the
#   subprocess (gpload.py).  This subprocess time limit can be configured DQ_IL2_Config.ini.  Parameters are as follows:
#
#    LOG_DIR                     <log directory for gpload and subprocess scripts>
#    SCRIPTS_DIR                 <.py scripts directory>
#    SOURCE_FILE_DIR             <RAW zipfile directory>
#    RAW_MESSAGE_GUID_CSV_DIR    <Directory to generate the csv>
#    RAW_DONE_PATH               <File index done full file path>
#    LOG_FREQUENCY               <Frequency to log the polling of the process to find if it has completed>
#    SLEEPTIME                   <Time to sleep in sec(s) when polling the success/failure of gpload.py>
#    GPLOAD_MAX_RUNTIME_SECS     <gpload.py threshold>
#    GPLOAD_RETRIES              <no. of retries for running the gpload.py script>
#    DOS_BATCH_FILE              <the batch file which runs gpload.py>
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
from __future__ import with_statement
import os, zipfile, re, time, sys, shutil, fileinput, datetime, ConfigParser, subprocess

#from datetime import datetime

### GLOBAL VARIABLES #########################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
##############################################################################################################################

def add_raw_index_entry(log_obj, filepath, guid, zipfile, filename):
    log_obj.write(guid + ',' + filepath + ',' + zipfile + ',' + filename + '\n')

def process_is_running(pid, name):
    a = os.popen("tasklist").readlines()
    for line in a:
          try:
              TASKLIST_PID = int(line[29:34])
          except:
              TASKLIST_PID = None
          try:
              TASKLIST_NAME = line[0:7]
          except:
              TASKLIST_NAME = None
          if TASKLIST_PID is not None and TASKLIST_NAME is not None:
              if int(TASKLIST_PID) == int(pid) and TASKLIST_NAME.strip() == name.strip():
                  return True
    return False

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
    CUSTOM_SECTION='DQ_IL2_index_raw_msg'
    GPLOAD_PROCESS_NAME='cmd.exe' # The tasklist name which runs the gpload.py process

    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)

    LOG_DIR                     = re.sub("/*$","/",config.get(CUSTOM_SECTION,'LOG_DIR'))
    SCRIPTS_DIR                 = re.sub("/*$","/",config.get(CUSTOM_SECTION,'SCRIPTS_DIR'))
    SOURCE_FILE_DIR             = re.sub("/*$","/",config.get(CUSTOM_SECTION,'SOURCE_FILE_DIR'))
    RAW_MESSAGE_GUID_CSV_DIR    = re.sub("/*$","/",config.get(CUSTOM_SECTION,'RAW_MESSAGE_GUID_CSV_DIR'))
    RAW_DONE_PATH               = re.sub("/*$","/",config.get(CUSTOM_SECTION,'RAW_DONE_PATH'))
    SLEEPTIME                   = int(config.get(CUSTOM_SECTION,'SLEEPTIME'))
    LOG_FREQUENCY               = int(config.get(CUSTOM_SECTION,'LOG_FREQUENCY'))
    GPLOAD_MAX_RUNTIME_SECS     = int(config.get(CUSTOM_SECTION,'GPLOAD_MAX_RUNTIME_SECS'))
    GPLOAD_RETRIES              = int(config.get(CUSTOM_SECTION,'GPLOAD_RETRIES'))
    DOS_BATCH_FILE              = config.get(CUSTOM_SECTION,'DOS_BATCH_FILE')

    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOG_DIR + 'DQ_IL2_index_raw_msg_' + YYYYMMDDSTR + '.log' # records general script output
    LOGFILE = open(LOGFILENAME, 'a')

    LOGFILE.write('\n--------------------------------------------------------------------\n')
    add_log_entry('*** RUN START ***',os.path.basename(sys.argv[0]))
    LOGFILE.write('--------------------------------------------------------------------\n')
    RAW_FILE_INDEX_NAME=RAW_MESSAGE_GUID_CSV_DIR + 'raw_message_guid.csv'
    RAW_FILE_INDEX_LOGFILE = open(RAW_FILE_INDEX_NAME, 'wb')

    ##############################################################################################################################
    # Index RAW zip files
    ##############################################################################################################################
    print '\n*** Indexing RAW files'
    add_log_entry('INDEXING','RAW files')
    source_dir_list =  [f for f in os.listdir(SOURCE_FILE_DIR) if re.match("RAW.*\.zip$",f)]

    if source_dir_list:
       for filename in source_dir_list:
           filedate = re.split('_',filename)[1]
           full_filepath=SOURCE_FILE_DIR + filename
           if os.path.isfile(full_filepath) and filename.upper().startswith('RAW'):
               zip = zipfile.ZipFile(full_filepath)
               FIRST_BAD_FILE= zip.testzip()

               if FIRST_BAD_FILE == None: # Carry on if there are no errors
                        FILE_COUNT=0
                        for compressed_file in zip.namelist():
                            if compressed_file.upper().endswith('.TXT'):
                               manifest_guid=re.split("_",os.path.basename(compressed_file))[1]
                               FILE_COUNT+=1
                               add_raw_index_entry(RAW_FILE_INDEX_LOGFILE,RAW_DONE_PATH + filedate,manifest_guid,filename,compressed_file)
                        add_log_entry('INDEXING','INDEXED ' + str(FILE_COUNT) + ' files from ' + filename )
                        zip.close()
               else:
                  add_log_entry('INDEXING','Test zip failed for ' + filename)

    else:
       add_log_entry('INDEXING','No source files')

    ##############################################################################################################################
    # Run batch file
    ##############################################################################################################################
    print '\n*** Run the batch file: ' + DOS_BATCH_FILE
    add_log_entry('GPLOAD',DOS_BATCH_FILE)
    LAST_RUN_LOGFILE_NAME=os.path.join(LOG_DIR,'gpload_raw_msg_guid_ext_ctrl_doc_last_run.log')

    RAW_FILE_INDEX_LOGFILE.close()
    os.system("cd " + SCRIPTS_DIR)


    ATTEMPT_NO=1
    GPLOAD_SUCCEEDED=False
    GPLOAD_FAILED=False

    for i in range(GPLOAD_RETRIES):

      add_log_entry('GPLOAD (Run ' + str(ATTEMPT_NO)+')','Starting GPLOAD')
      open(LAST_RUN_LOGFILE_NAME,'wb').close() # open and clear the last run log file
      PROCESS_STARTTIME=datetime.datetime.now()

      # Start the GPLOAD process
      try:
          s=subprocess.Popen(DOS_BATCH_FILE, shell=False, stderr=False, stdout=False)
      except Exception, e:
          add_log_entry('ERROR RUNNING BATCH FILE',str(e))
          sys.exit(1)

      PID=s.pid

      add_log_entry('','PID ('+str(PID)+') has been started')

      ELAPSED_TIME=None
      COUNTER=0

      # Loop until:
      # - The script succeeds
      # - The script fails
      # - The script exceeds the max threshold parameter
      while True:

            if process_is_running(PID, GPLOAD_PROCESS_NAME) and COUNTER % LOG_FREQUENCY == 0:
                add_log_entry('','PID ('+str(PID)+') is running')

            ########################################################
            # Handle over-running jobs
            ########################################################
            ELAPSED_TIME = datetime.datetime.now() - PROCESS_STARTTIME

            if ELAPSED_TIME.seconds > GPLOAD_MAX_RUNTIME_SECS:
                add_log_entry('', 'Threshold exceeded (' + str(ELAPSED_TIME.seconds) + ' sec(s))')

                if process_is_running(PID, GPLOAD_PROCESS_NAME):
                    SUCCESSFULLY_KILLED_PROCESS=False
                    try:
                        TASKKILL_RETURN_CODE=subprocess.call(['taskkill', '/F', '/T', '/PID', str(s.pid)])
                    except:
                        add_log_entry('TASKKILL','Failed to kill process ('+str(TASKKILL_RETURN_CODE)+')')
                        pass

                    if TASKKILL_RETURN_CODE == 0:
                        add_log_entry('TASKKILL','Successfully killed process ('+str(TASKKILL_RETURN_CODE)+')')
                        break # The script has OVERRAN and we have successfully killed the process
                else:
                    add_log_entry('TASKKILL','PID is not running')

            if not process_is_running(PID, GPLOAD_PROCESS_NAME):
                add_log_entry('','PID ('+str(PID)+') is not running')
                shutil.copy(LAST_RUN_LOGFILE_NAME,LAST_RUN_LOGFILE_NAME + '.tmp')

                LAST_RUN_LOGFILE=open(LAST_RUN_LOGFILE_NAME + '.tmp','rb')
                LAST_RUN_LOGFILE_LINES=LAST_RUN_LOGFILE.readlines()
                LAST_RUN_LOGFILE.close()
                LAST_RUN_LOGFILE_LENGTH=len(LAST_RUN_LOGFILE_LINES)
                LAST_LINE=''

                if LAST_RUN_LOGFILE_LENGTH > 1:
                    LAST_LINE = LAST_RUN_LOGFILE_LINES[LAST_RUN_LOGFILE_LENGTH-1]

                    if re.search('\|INFO\|gpload succeeded',LAST_LINE):
                        GPLOAD_SUCCEEDED=True
                        break # The script has SUCCEEDED
                    elif re.search('\|INFO\|gpload failed',LAST_LINE):
                        GPLOAD_FAILED=True
                        break # The script has FAILED

            time.sleep(SLEEPTIME)
            COUNTER+=1

      # END WHILE LOOP

      if GPLOAD_SUCCEEDED:
          add_log_entry('','gpload.py completed successfully')
          break
      elif GPLOAD_FAILED:
          add_log_entry('','gpload failed')
          for line in LAST_RUN_LOGFILE_LINES:
              if re.search('\|ERROR\|',line):
                  add_log_entry('',line.replace('\n',''))

      if ATTEMPT_NO>=GPLOAD_RETRIES:
          add_log_entry('FAILED',DOS_BATCH_FILE + ' failed after ' + str(ATTEMPT_NO) + ' attempt(s)')
          ENDTIME = datetime.datetime.now()
          delta = ENDTIME - STARTTIME
          add_log_entry('SCRIPT FAILED','ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s)')
          sys.exit(1)

      ATTEMPT_NO+=1

    ##############################################################################################################################
    # Move files to Local archive folder
    ##############################################################################################################################

    print '\n*** Moving files to local archive folder'

    if source_dir_list:
       for filename in source_dir_list:
           full_filepath=SOURCE_FILE_DIR + filename
           if os.path.isfile(full_filepath) and filename.upper().startswith('RAW'):
                if os.path.exists(RAW_DONE_PATH):
                     shutil.move(full_filepath, RAW_DONE_PATH + filename)

    else:
       add_log_entry('MOVING FILES TO LOCAL ARCHIVE','No source files')

    ##############################################################################################################################
    # SCRIPT END
    ##############################################################################################################################
    ENDTIME = datetime.datetime.now()
    delta = ENDTIME - STARTTIME
    add_log_entry('SCRIPT COMPLETE','ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s)')


# Run the main function
if __name__ == "__main__":
	main(sys.argv[1:])
