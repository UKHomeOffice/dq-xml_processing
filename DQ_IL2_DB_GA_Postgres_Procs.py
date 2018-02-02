#!/usr/bin/env python

##############################################################################################################################
# DQ_IL2_DB_GA_Postgres_Procs.py
#
# AUTHOR:      Stewart Lee
# DATE:        2016/02/12
#
# This script runs SQL commands from an input .sql file
#
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
import os, re, time, sys, shutil, fileinput, getopt, datetime, ConfigParser
import psycopg2

### GLOBAL VARIABLES #########################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
##############################################################################################################################

# This function moves a file to a folder, overwriting if another file of the same name already exists
def overwrite_file(from_file, to_file):
    if os.path.exists(to_file):
       os.remove(to_file)
    os.rename(from_file,to_file)

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def fetchone_cursor_output(curs):
    DEFAULT_OUTPUT=None
    outputmsg=DEFAULT_OUTPUT
    try: # get first output message if it exists
        outputmsg = curs.fetchone() #  only get one row of output
    except Exception, e:
        pass
    
    if curs.rowcount != 0:
        return outputmsg
    else:
        return DEFAULT_OUTPUT
        

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
    SQL_INPUT_FILENAME='GA_PG_Custom_Stored_Proc_List.sql'
    DEFAULT_SECTION='DEFAULT'
    CUSTOM_SECTION='DQ_IL2_DB_GA_Postgres_Load_Procs'

    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)
   
    ROOT_DIR                = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))
    PG_HOST                 = config.get(DEFAULT_SECTION,'PG_HOST')
    PG_USER                 = config.get(DEFAULT_SECTION,'PG_USER')
    PG_DB                   = config.get(DEFAULT_SECTION,'PG_DB')
    DB_CONNECT_RETRIES      = int(config.get(DEFAULT_SECTION,'DB_CONNECT_RETRIES'))
    DB_CONNECT_RETRY_DELAY  = int(config.get(DEFAULT_SECTION,'DB_CONNECT_RETRY_DELAY'))
    DEBUG                   = int(config.get(DEFAULT_SECTION,'DEBUG'))
    
    ARCHIVE_FILE_DIR=os.path.join(ROOT_DIR, 'archive/')
    REJECT_FILE_DIR=os.path.join(ROOT_DIR, 'reject/')
    SCRIPTS_FILE_DIR=os.path.join(ROOT_DIR, 'scripts/')
    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    SQL_INPUT_FILENAME=os.path.join(SCRIPTS_FILE_DIR, SQL_INPUT_FILENAME)

    FILE_COUNTER=0

    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_DB_GP_LOAD_PROCS_' + YYYYMMDDSTR + '.log'
    global LOGFILE
    LOGFILE = open(LOGFILENAME, 'a')   
    LOGFILE.write('\n--------------------------------------------------------------------\n')
    add_log_entry('*** RUN START ***', os.path.basename(sys.argv[0]))
    LOGFILE.write('--------------------------------------------------------------------\n')

   
    ##############################################################################################################################
    # Run POSTGRES load
    ##############################################################################################################################
    print '\n*** Load POSTGRES DB'

    RETRY_COUNT=0

    CONN_STRING='host='+PG_HOST+' dbname='+PG_DB+' user='+PG_USER
    add_log_entry('CONNECTING TO DATABASE', 'Connecting to '+PG_HOST+' as '+PG_USER)
    
    while True: # connect to the database  
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
    
    if os.path.exists(SQL_INPUT_FILENAME):
        SQL_INPUT_FILE = open(SQL_INPUT_FILENAME, 'r')

        ##########################################################################################################################
        # Run custom stored procs (This will only work with CUSTOM STORED PROCS which produce output as a result set:
        # (RETURN CODE, OUTPUT MESSAGE, ERROR MESSAGE)
        ##########################################################################################################################
        #cur.execute("SET CLIENT_ENCODING TO 'LATIN1';")
        add_log_entry('RUNNING SQL USING INPUT FILE', os.path.basename(SQL_INPUT_FILENAME).replace('\n',''))
        COUNTER=1
        TOTAL_COMMANDS=file_len(SQL_INPUT_FILENAME)
        
        for SQL_STATEMENT in SQL_INPUT_FILE.readlines():
            
            cur = conn.cursor()
            
            add_log_entry('*** EXECUTING COMMAND ('+str(COUNTER)+'/'+str(TOTAL_COMMANDS)+')', SQL_STATEMENT.replace('\n',''))
            
            try:
                cur.execute(SQL_STATEMENT)  
            except Exception, e:
                SQL_INPUT_FILE.close()
                cur.close()
                add_log_entry('*** FAILED', str(e))
                sys.exit(1)
            
            STOREDPROC_OUTPUT=fetchone_cursor_output(cur)

            if len(STOREDPROC_OUTPUT) >= 3:
                RETURN_CODE=STOREDPROC_OUTPUT[0]
                OUTPUT_MSG=str(STOREDPROC_OUTPUT[1])
                ERROR_MSG=str(STOREDPROC_OUTPUT[2])
            else:
                SQL_INPUT_FILE.close()
                cur.close()
                add_log_entry('*** FAILED', 'Invalid stored proc output')
                sys.exit(1)
            
            if RETURN_CODE <0:
                SQL_INPUT_FILE.close()
                cur.close()
                add_log_entry('*** FAILED ('+str(RETURN_CODE)+')', str(OUTPUT_MSG+', '+ERROR_MSG))
                sys.exit(1)
            add_log_entry('*** SUCCESS ('+str(RETURN_CODE)+')', str(OUTPUT_MSG+', '+ERROR_MSG) )
            
            conn.commit() 
            SQL_INPUT_FILE.close()            
            cur.close()
            COUNTER+=1
        
    else:
        add_log_entry('NO SQL INPUT FILE', SQL_INPUT_FILENAME+' must exist - copy file and rerun')
        sys.exit(1)

    conn.close()


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
