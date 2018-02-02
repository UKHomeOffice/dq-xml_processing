#!/usr/bin/env python

#######################################################################################################################
# DQ_IL2_DB_GA_Postgres_Load_FPL
#
# AUTHOR:      Phil Hibbs
# DATE:        2016/06/30
#
# This script loads GA NATS files to the GA database
#
# UPDATE:   Stewart Lee
# DATE:     2016/08/19
#
#           - Added json_key_exists/get_json_key_value functions
#           - Updated for new file formats to include new messgetypes (FLIGHTPLAN, ARRIVAL, DEPARTURE, etc.)
#
#
#######################################################################################################################

### IMPORT PYTHON MODULES #############################################################################################
import os, re, time, sys, shutil, datetime, ConfigParser, pyodbc
import json, csv
import psycopg2

### GLOBAL VARIABLES ##################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
#######################################################################################################################

def json_key_exists(json_obj, key):
    try:
        json_obj[key]
    except Exception, e:
        return False
    return True

def get_json_key_value(json_obj, key):
    try:
        json_obj[key]
    except Exception, e:
        return None
    return json_obj[key]

def add_log_entry(log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    if DEBUG: print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    LOGFILE.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def main(argv):

    ### GLOBAL DEBUG VARIABLE### ######################################################################################
    global DEBUG
    DEBUG=1

    ### OTHER VARIABLES ###############################################################################################
    STARTTIME = datetime.datetime.now()

    ### READ CONFIG FILE ##############################################################################################
    CONFIG_FILE='DQ_IL2_Config.ini'
    DEFAULT_SECTION='DEFAULT'
    CUSTOM_SECTION='DQ_IL2_DB_GA_Postgres_Load_FPL'

    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)
   
    ROOT_DIR                = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))
    SOURCE_FILE_DIR         = re.sub("/*$","/",config.get(CUSTOM_SECTION,'SOURCE_FILE_DIR'))
    INPROCESS_FILE_DIR      = re.sub("/*$","/",config.get(CUSTOM_SECTION,'INPROCESS_FILE_DIR'))
    TARGET_FILE_DIR         = re.sub("/*$","/",config.get(CUSTOM_SECTION,'TARGET_FILE_DIR'))
    CSV_NAME                = config.get(CUSTOM_SECTION,'NATS_CSV_FILE')
    PG_LOAD_TABLE           = config.get(CUSTOM_SECTION,'PG_LOAD_TABLE')
    PG_HOST                 = config.get(DEFAULT_SECTION,'PG_HOST')
    PG_USER                 = config.get(DEFAULT_SECTION,'PG_USER')
    PG_DB                   = config.get(DEFAULT_SECTION,'PG_DB')
    DB_CONNECT_RETRIES      = int(config.get(DEFAULT_SECTION,'DB_CONNECT_RETRIES'))
    DB_CONNECT_RETRY_DELAY  = int(config.get(DEFAULT_SECTION,'DB_CONNECT_RETRY_DELAY'))                     
    DEBUG                   = int(config.get(DEFAULT_SECTION,'DEBUG'))
    
    ### REGEX/MATCHING VARIABLES ######################################################################################
    NATS_JSON_REGEX=          r'^.*\.json$'

    REJECT_FILE_DIR=os.path.join(ROOT_DIR, 'reject/')
    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    NATS_CSV_PATH=os.path.join(TARGET_FILE_DIR,CSV_NAME)
    MESSAGETYPES=['ARRIVAL','CHANGE','CANCELLATION','DEPARTURE','DELAY','FLIGHTPLAN','MESSAGE']
                             
    ### NATS FILE/TABLE VARIABLES #####################################################################################
    #             FILENAME    DELIM  TABLENAME                  PRE-LOAD SQL                 
    PARAM_LIST=   [CSV_NAME,  ',',   PG_LOAD_TABLE,  'TRUNCATE TABLE ' + PG_LOAD_TABLE + ';' ]   
    
    ### LOG FILE VARIABLES ############################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_DB_GA_Postgres_Load_FPL_' + YYYYMMDDSTR + '.log'
    global LOGFILE
    LOGFILE = open(LOGFILENAME, 'a')   
    add_log_entry('*** RUN START ***', os.path.basename(sys.argv[0]))

    ##############################################################################################################################
    # Move files
    ##############################################################################################################################

    print '\n*** Move xml files'
    source_dir_list = [f for f in os.listdir(SOURCE_FILE_DIR) if f.lower().endswith(".json")]
    if source_dir_list:
       FILE_COUNT=0
       for fname in source_dir_list:
          fname=SOURCE_FILE_DIR + fname
          shutil.move(fname,INPROCESS_FILE_DIR + os.path.basename(fname))
          FILE_COUNT+=1
       add_log_entry('MOVE JSON FILES', 'Moved ' + str(FILE_COUNT) + ' files')
    else:
       add_log_entry('MOVE JSON FILES', 'No JSON files')

    ###################################################################################################################
    # Parse JSON file and write CSV
    ###################################################################################################################
    print '\n*** Parse JSON file and write CSV'

    inprocess_dir_list = [f for f in os.listdir(INPROCESS_FILE_DIR) if (re.match(NATS_JSON_REGEX, f))]
    CURRENT_BATCH_SIZE = len(inprocess_dir_list)

    with open(NATS_CSV_PATH,'wb') as CSV_FILE:
        FIELD_NAMES = ['MESSAGETYPE','ADEP','CALLSIGN','DOF','EOBT','MTYPE','ADES','IFPLID','REG','TITLE','EOBD','MESSAGE_CONTENT','FILENAME','SUBMISSION_TIME','MESSAGE_DELIVERY_TIME' ]
        CSV_OUT = csv.DictWriter(CSV_FILE, fieldnames=FIELD_NAMES)
        # CSV_OUT.writeheader()
        if inprocess_dir_list:
           for filename in inprocess_dir_list:
               #filedate = datetime.datetime.strptime(filename.split("_")[1], '%Y%m%d')
               # This section reads all json files (sorted by filename)
               # and writes the contents out as CSV
               VALID_JSON=True
               if os.path.isfile(INPROCESS_FILE_DIR + filename):
                   json_data = open(INPROCESS_FILE_DIR + filename)
                   try: 
                       data = json.load(json_data)
                   except Exception, e:
                       VALID_JSON=False
                       break

                   if VALID_JSON:
                       json_data.close

                       # Initialise variables
                       ADEP,CALLSIGN,DOF,EOBT,MTYPE,ADES=None,None,None,None,None,None
                       IFPLID,REG,TITLE,EOBD=None,None,None,None
                       MESSAGE_CONTENT=None
                       MESSAGETYPE=None
                       SUBMISSION_TIME=None
                       MESSAGE_DELIVERY_TIME=None
                       
                       if json_key_exists(data,'X400Message'):
                           data_x400=json.loads(json.dumps(data['X400Message']))
                           if json_key_exists(data_x400,'envelope'):
                               data_envelope=json.loads(json.dumps(data_x400['envelope']))
                               SUBMISSION_TIME=get_json_key_value(data_envelope,'submissionTime')
                               MESSAGE_DELIVERY_TIME=get_json_key_value(data_envelope,'messageDeliveryTime')
                       
                           for CURR_MESSAGETYPE in MESSAGETYPES:
                               if json_key_exists(data,CURR_MESSAGETYPE):
                                   MESSAGETYPE=CURR_MESSAGETYPE
                                   data_curr_mt=json.loads(json.dumps(data[MESSAGETYPE]))
                                   ADEP=     get_json_key_value(data_curr_mt,'ADEP')
                                   CALLSIGN= get_json_key_value(data_curr_mt,'CALLSIGN')
                                   DOF=      get_json_key_value(data_curr_mt,'DOF')
                                   EOBT=     get_json_key_value(data_curr_mt,'EOBT')
                                   MTYPE=     get_json_key_value(data_curr_mt,'TYPE')
                                   ADES=     get_json_key_value(data_curr_mt,'ADES')
                                   IFPLID=   get_json_key_value(data_curr_mt,'IFPLID')
                                   REG=      get_json_key_value(data_curr_mt,'REG')
                                   TITLE=    get_json_key_value(data_curr_mt,'TITLE')
                                   EOBD=     get_json_key_value(data_curr_mt,'EOBD')
                                   MESSAGE_CONTENT=  get_json_key_value(data_curr_mt,'CONTENT')
                                   MESSAGE_CONTENT=  MESSAGE_CONTENT.replace('\r','\\r').replace('\n','\\n').replace(',','\,') if MESSAGE_CONTENT else None
                                   break;
                              
                       CSV_OUT.writerow({
                             'TITLE'                    : TITLE
                             ,'MESSAGETYPE'             : MESSAGETYPE
                             ,'CALLSIGN'                : CALLSIGN
                             ,'REG'                     : REG
                             ,'ADEP'                    : ADEP
                             ,'ADES'                    : ADES
                             ,'MTYPE'                   : MTYPE
                             ,'EOBT'                    : EOBT
                             ,'EOBD'                    : EOBD
                             ,'DOF'                     : DOF
                             ,'IFPLID'                  : IFPLID
                             ,'MESSAGE_CONTENT'         : MESSAGE_CONTENT
                             ,'FILENAME'                : filename
                             ,'SUBMISSION_TIME'         : SUBMISSION_TIME
                             ,'MESSAGE_DELIVERY_TIME'   : MESSAGE_DELIVERY_TIME
                             })
                   else:    
                       add_log_entry('ERROR PARSING JSON', filename) 
    add_log_entry('PARSE JSON AND WRITE CSV', 'Done')
    ###################################################################################################################
    # Run POSTGRES load
    ###################################################################################################################
    print '\n*** Load POSTGRES DB'

    RETRY_COUNT=0

    ###################################################################################################################
    # Connect to DB
    ###################################################################################################################

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
    

    ###################################################################################################################
    # LOAD NATS
    ###################################################################################################################

    SOURCE_FILE_EXTRACT=PARAM_LIST[0]
    SOURCE_FILE_EXTRACT_DELIM=PARAM_LIST[1]
    TARGET_TABLE_NAME=PARAM_LIST[2]
    PRE_LOAD_SQL=PARAM_LIST[3]
    cur = conn.cursor()
    SOURCE_FILE_EXTRACT=os.path.join(TARGET_FILE_DIR,SOURCE_FILE_EXTRACT)
    
    ###################################################################################################################
    # Load NATS
    ###################################################################################################################
    cur.execute("SET CLIENT_ENCODING TO 'LATIN1';") 

    # RUN PRE_LOAD_SQL ################################################################################################
    add_log_entry('*** RUNNING PRE_LOAD SQL...', PRE_LOAD_SQL)
    
    try:
        cur.execute(PRE_LOAD_SQL)
        conn.commit()
    except Exception, e:
        add_log_entry('ERROR', str(e))
        sys.exit(1)
    add_log_entry('*** SUCCESS', '')

    if os.path.exists(SOURCE_FILE_EXTRACT) and os.stat(SOURCE_FILE_EXTRACT).st_size != 0: # if file exists and is not empty
    
        SOURCE_FILE_EXTRACT_FILE = open(SOURCE_FILE_EXTRACT, 'r')
        
        # RUN LOAD_SQL ################################################################################################
        add_log_entry('*** RUNNING FILE LOAD...', os.path.basename(SOURCE_FILE_EXTRACT) + ' into ' + TARGET_TABLE_NAME)
        try:
            cur.copy_from(SOURCE_FILE_EXTRACT_FILE, TARGET_TABLE_NAME, sep=SOURCE_FILE_EXTRACT_DELIM)
            conn.commit()
        except Exception, e:
            SOURCE_FILE_EXTRACT_FILE.close()
            add_log_entry('NATS FILE LOAD ERROR', 'Error loading file: '+os.path.basename(SOURCE_FILE_EXTRACT)+': Table: '+TARGET_TABLE_NAME+': '+str(e))
            sys.exit(1)
        SOURCE_FILE_EXTRACT_FILE.close()
        
        add_log_entry('*** SUCCESS', '')
        cur.close()            
    else:
        add_log_entry('NO DATA TO LOAD', os.path.basename(SOURCE_FILE_EXTRACT))

    conn.close()

    ###################################################################################################################
    # SCRIPT END
    ###################################################################################################################
    ENDTIME = datetime.datetime.now()
    delta = ENDTIME - STARTTIME
    add_log_entry('*** RUN COMPLETE ***', time.strftime("%Y%m%d%H%M%S") + ' (ELAPSED TIME: ' + str(delta.seconds) + '.' + str(delta.microseconds) + ' sec(s))')
    LOGFILE.close()
    
# Run the main function
if __name__ == "__main__":
	main(sys.argv[1:])
