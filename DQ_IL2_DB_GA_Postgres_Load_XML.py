#!/usr/bin/env python

##############################################################################################################################
# DQ_IL2_DB_GA_Postgres_Load_XML
#
# AUTHOR:      Stewart Lee
# DATE:        2016/02/12
#
# This script loads GA xml files to the GA database:
# - Preprocesses files
# - Loads files to the PG database (retries on failure x times as configured in parameter file)
#
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
import os, re, time, sys, shutil, fileinput, datetime, ConfigParser
import psycopg2

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
    if DEBUG: print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    LOGFILE.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def main(argv):

    ### GLOBAL DEBUG VARIABLE### #################################################################################################
    global DEBUG
    DEBUG=1

    ### OTHER VARIABLES ##########################################################################################################
    STARTTIME = datetime.datetime.now()

    ### READ CONFIG FILE #######################################################################################################
    CONFIG_FILE='DQ_IL2_Config.ini'
    DEFAULT_SECTION='DEFAULT'
    CUSTOM_SECTION='DQ_IL2_DB_GA_Postgres_Load_XML'

    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)
   
    ROOT_DIR                = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))
    SOURCE_FILE_DIR         = re.sub("/*$","/",config.get(CUSTOM_SECTION,'SOURCE_FILE_DIR'))
    INPROCESS_FILE_DIR      = re.sub("/*$","/",config.get(CUSTOM_SECTION,'INPROCESS_FILE_DIR'))
    GA_CONCAT_OUTPUT_DIR    = re.sub("/*$","/",config.get(CUSTOM_SECTION,'GA_CONCAT_OUTPUT'))
    PG_HOST                 = config.get(DEFAULT_SECTION,'PG_HOST')
    PG_USER                 = config.get(DEFAULT_SECTION,'PG_USER')
    PG_DB                   = config.get(DEFAULT_SECTION,'PG_DB')
    PG_LOAD_TABLE           = config.get(CUSTOM_SECTION,'PG_LOAD_TABLE')
    SLEEPTIME               = int(config.get(CUSTOM_SECTION,'SLEEPTIME'))
    DB_CONNECT_RETRIES      = int(config.get(DEFAULT_SECTION,'DB_CONNECT_RETRIES'))
    DB_CONNECT_RETRY_DELAY  = int(config.get(DEFAULT_SECTION,'DB_CONNECT_RETRY_DELAY'))
    DEBUG                   = int(config.get(DEFAULT_SECTION,'DEBUG'))
    
    REJECT_FILE_DIR=os.path.join(ROOT_DIR, 'reject/')
    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    OUTPUT_MOD_FILENAME=GA_CONCAT_OUTPUT_DIR + 'GA_PARSED_CONCAT_SOURCE.xml.MOD'
    
    FILE_COUNTER=0

    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_DB_GA_POSTGRES_LOAD_XML_' + YYYYMMDDSTR + '.log'
    global LOGFILE
    LOGFILE = open(LOGFILENAME, 'a')   
    add_log_entry('*** RUN START ***', os.path.basename(sys.argv[0]))

    ##############################################################################################################################
    # Move files
    ##############################################################################################################################

    print '\n*** Move xml files'
    source_dir_list = [f for f in os.listdir(SOURCE_FILE_DIR) if f.lower().endswith(".xml")]
    if source_dir_list:
       FILE_COUNT=0
       for fname in source_dir_list:
          fname=SOURCE_FILE_DIR + fname
          shutil.move(fname,INPROCESS_FILE_DIR + os.path.basename(fname))
          FILE_COUNT+=1
       add_log_entry('MOVE XML FILES', 'Moved ' + str(FILE_COUNT) + ' files')
    else:
       add_log_entry('MOVE XML FILES', 'No xml files')
       
    ##############################################################################################################################
    # Preprocess files
    ##############################################################################################################################

    print '\n*** Concat xml files'
    OUTPUT_MOD_FILE = open(OUTPUT_MOD_FILENAME, 'wb')
    inprocess_dir_list = [f for f in os.listdir(INPROCESS_FILE_DIR) if f.lower().endswith(".xml")]
    if inprocess_dir_list:
       FILE_COUNT=0
       for fname in inprocess_dir_list:
          fname=INPROCESS_FILE_DIR + fname
          concat=''
          concat += os.path.basename(fname) + '|' + open(fname).read() + '\r\n'

          FILE_LENGTH=file_len(fname)
          f = open(fname, 'r')
          if FILE_LENGTH==1:
              for line in f.readlines():
                  OUTPUT_MOD_FILE.write(concat)
              f.close()
          else: # reject any files which have more than 1 line
              f.close()
              shutil.move(fname,REJECT_FILE_DIR + os.path.basename(fname))
          
          FILE_COUNT+=1
       add_log_entry('CONCAT XML FILES', 'Concatenated ' + str(FILE_COUNT) + ' files')
    else:
       add_log_entry('CONCAT XML FILES', 'No xml files')
       
    OUTPUT_MOD_FILE.close()
    
    ##############################################################################################################################
    # Run POSTGRES load
    ##############################################################################################################################
    print '\n*** Load POSTGRES DB'

    RETRY_COUNT=0
    concat_inprocess_dir_list = [f for f in os.listdir(GA_CONCAT_OUTPUT_DIR) if f.endswith('.xml.MOD')]
    if concat_inprocess_dir_list:
        CONN_STRING='host='+PG_HOST+' dbname='+PG_DB+' user='+PG_USER
        add_log_entry('CONNECTING TO DATABASE', 'Connecting to '+PG_HOST+' as '+PG_USER)
        
        while True: # connect to the database a retry 
            try:
                conn = psycopg2.connect(CONN_STRING)
                break
            except Exception, e:
                if RETRY_COUNT<DB_CONNECT_RETRIES:
                    RETRY_COUNT+=1
                    add_log_entry('CONNECTING TO DATABASE', 'Connection failed... retrying attempt ' + str(RETRY_COUNT) + ' of ' + str(DB_CONNECT_RETRIES) )
                    time.sleep(DB_CONNECT_RETRY_DELAY) # Delay for n seconds between connection retries
                else:
                    add_log_entry('CONNECTING TO DATABASE', 'ERROR: ' + str(e))
                    sys.exit(1)            
                        
        add_log_entry('CONNECTING TO DATABASE', 'Connection successful')
        cur = conn.cursor()
        for file_name in concat_inprocess_dir_list:
            f = open(GA_CONCAT_OUTPUT_DIR + file_name, 'r')
            add_log_entry('RUNNING SQL...','TRUNCATING AND LOADING TABLE '+PG_LOAD_TABLE)
            try:
                cur.execute('TRUNCATE TABLE ' + PG_LOAD_TABLE + ';')
                conn.commit()
                cur.copy_from(f, PG_LOAD_TABLE, sep='|')
            except Exception, e:
                f.close()
                cur.close()
                add_log_entry('SQL FAILED', str(e))
                sys.exit(1)
            #rowcount=cur.rowcount
            conn.commit()
            f.close()
            add_log_entry('GA FILE LOADED ', file_name )
        cur.close()
        conn.close()
    else:
        add_log_entry('NO FILES TO PROCESS', 'No source files or nothing to process')
        add_log_entry('*** RUN COMPLETE ***', time.strftime("%Y%m%d%H%M%S"))
        LOGFILE.close()
        sys.exit(0)

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
