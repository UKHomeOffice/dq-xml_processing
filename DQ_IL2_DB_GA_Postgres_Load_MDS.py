#!/usr/bin/env python

##############################################################################################################################
# DQ_IL2_DB_GA_Postgres_Load_MDS
#
# AUTHOR:      Stewart Lee
# DATE:        2016/02/12
#
# This script loads GA MDS files to the GA database:
#
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
import os, re, time, sys, shutil, fileinput, getopt, datetime, ConfigParser, pyodbc, getpass
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

def add_log_entry(log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    if DEBUG: print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    LOGFILE.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def fetchone_cursor_output(curs):
    outputmsg='No output'
    try: # get first output message if it exists
        outputmsg = curs.fetchone()
    except Exception, e:
        pass
    return outputmsg

def main(argv):

    ### GLOBAL DEBUG VARIABLE### #################################################################################################
    global DEBUG
    DEBUG=1

    ### OTHER VARIABLES ##########################################################################################################
    STARTTIME = datetime.datetime.now()

    ### READ CONFIG FILE #######################################################################################################
    CONFIG_FILE='DQ_IL2_Config.ini'
    DEFAULT_SECTION='DEFAULT'
    CUSTOM_SECTION='DQ_IL2_DB_GA_Postgres_Load_MDS'

    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)
   
    ROOT_DIR                = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))
    SOURCE_FILE_DIR         = re.sub("/*$","/",config.get(CUSTOM_SECTION,'SOURCE_FILE_DIR'))
    PG_HOST                 = config.get(DEFAULT_SECTION,'PG_HOST')
    PG_USER                 = config.get(DEFAULT_SECTION,'PG_USER')
    PG_DB                   = config.get(DEFAULT_SECTION,'PG_DB')
    SLEEPTIME               = int(config.get(CUSTOM_SECTION,'SLEEPTIME'))
    DB_CONNECT_RETRIES      = int(config.get(DEFAULT_SECTION,'DB_CONNECT_RETRIES'))
    DB_CONNECT_RETRY_DELAY  = int(config.get(DEFAULT_SECTION,'DB_CONNECT_RETRY_DELAY'))
    MDS_DB_HOST             = re.sub("/*$","/",config.get(DEFAULT_SECTION,'MDS_DB_HOST'))                      # MDS Extract File name for MDS
    DEBUG                   = int(config.get(DEFAULT_SECTION,'DEBUG'))
    
    ARCHIVE_FILE_DIR=os.path.join(ROOT_DIR, 'archive/')
    REJECT_FILE_DIR=os.path.join(ROOT_DIR, 'reject/')
    LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
    MDS_EXTRACT_DIR=os.path.join(ROOT_DIR, 'mds/')                                          # The folder containing the extracted MDS file (the results of the query MDS_DB_SQL)

    FILE_COUNTER=0
    
    ### MDS FILE/TABLE VARIABLES #################################################################################################
    MDS_SCHEMA_NAME='ga_master_data'
                                    # FILENAME                                  # DELIM     # TABLENAME                                     # PRE-LOAD SQL                 
    MDS_PORT_VALUES_LIST=           ['ga_mds_port_values_extract.csv',          '|',        MDS_SCHEMA_NAME+'.tbl_mds_port_values',         'TRUNCATE TABLE ' + MDS_SCHEMA_NAME+'.tbl_mds_port_values'          + ';' ]
    MDS_PORTS_LIST=                 ['ga_mds_ports_extract.csv',                '|',        MDS_SCHEMA_NAME+'.tbl_mds_ports',               'TRUNCATE TABLE ' + MDS_SCHEMA_NAME+'.tbl_mds_ports'                + ';' ]
    MDS_BF_OFFICES_LIST=            ['ga_mds_bf_offices_extract.csv',           '|',        MDS_SCHEMA_NAME+'.tbl_mds_bf_offices',          'TRUNCATE TABLE ' + MDS_SCHEMA_NAME+'.tbl_mds_bf_offices'           + ';' ]
    MDS_BF_REGIONS_LIST=            ['ga_mds_bf_regions_extract.csv',           '|',        MDS_SCHEMA_NAME+'.tbl_mds_bf_regions',          'TRUNCATE TABLE ' + MDS_SCHEMA_NAME+'.tbl_mds_bf_regions'           + ';' ]
    MDS_PERSON_TYPES_LIST=          ['ga_mds_person_types_extract.csv',         '|',        MDS_SCHEMA_NAME+'.tbl_mds_passengertype',       'TRUNCATE TABLE ' + MDS_SCHEMA_NAME+'.tbl_mds_passengertype'        + ';' ]
    MDS_NON_ICAO_PORT_VALUES_LIST=  ['tbl_ga_non_icao_port_value.csv',          ',',        MDS_SCHEMA_NAME+'.tbl_ga_non_icao_port_values', 'DELETE FROM '    + MDS_SCHEMA_NAME+'.tbl_ga_non_icao_port_values' \
                                                                                                                                             + ' where added_by_user_flag = \'N\'' + ';' ]
    MDS_NON_ICAO_PORTS_LIST=        ['tbl_ga_non_icao_ports.csv',               ',',        MDS_SCHEMA_NAME+'.tbl_ga_non_icao_ports',       'DELETE FROM '    + MDS_SCHEMA_NAME+'.tbl_ga_non_icao_ports' \
                                                                                                                                             + ' where added_by_user_flag = \'N\'' + ';' ]      
    MDS_AD_GROUP_TO_REGION_LIST=    ['tbl_ad_group_to_region_links.csv',        ',',        MDS_SCHEMA_NAME+'.tbl_ad_group_to_region_links','TRUNCATE TABLE ' + MDS_SCHEMA_NAME+'.tbl_ad_group_to_region_links' + ';' ]
    MDS_AD_GROUPS_LIST=             ['tbl_ad_groups.csv',                       ',',        MDS_SCHEMA_NAME+'.tbl_ad_groups',               'TRUNCATE TABLE ' + MDS_SCHEMA_NAME+'.tbl_ad_groups'                + ';' ]
    MDS_ROLES_LIST=                 ['tbl_roles.csv',                           ',',        MDS_SCHEMA_NAME+'.tbl_roles',                   'TRUNCATE TABLE ' + MDS_SCHEMA_NAME+'.tbl_roles'                    + ';' ]
    ### LOG FILE VARIABLES #######################################################################################################
    LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_DB_GA_Postgres_Load_MDS_' + YYYYMMDDSTR + '.log'
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

    ##############################################################################################################################
    # Connect to DB
    ##############################################################################################################################

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
    

    ##############################################################################################################################
    # LOAD MDS
    ##############################################################################################################################

    for FILE_TABLE_LIST in (MDS_PORT_VALUES_LIST, \
                            MDS_PORTS_LIST, \
                            MDS_BF_OFFICES_LIST, \
                            MDS_BF_REGIONS_LIST, \
                            MDS_PERSON_TYPES_LIST, \
                            MDS_NON_ICAO_PORT_VALUES_LIST, \
                            MDS_NON_ICAO_PORTS_LIST, \
                            MDS_AD_GROUP_TO_REGION_LIST, \
                            MDS_AD_GROUPS_LIST, \
                            MDS_ROLES_LIST \
                            ):
        MDS_EXTRACT=FILE_TABLE_LIST[0]
        MDS_EXTRACT_DELIM=FILE_TABLE_LIST[1]
        MDS_TABLE=FILE_TABLE_LIST[2]
        MDS_PRE_LOAD_SQL=FILE_TABLE_LIST[3]
        cur = conn.cursor()
        MDS_EXTRACT=os.path.join(MDS_EXTRACT_DIR,MDS_EXTRACT)


        ##########################################################################################################################
        # Load PORTS
        ##########################################################################################################################
        cur.execute("SET CLIENT_ENCODING TO 'LATIN1';") 

        # RUN PRE_LOAD_SQL #######################################################################################################
        add_log_entry('*** RUNNING PRE_LOAD SQL...', MDS_PRE_LOAD_SQL)
        
        try:
            cur.execute(MDS_PRE_LOAD_SQL)
            conn.commit() 
        except Exception, e:
            add_log_entry('ERROR', str(e))
            sys.exit(1)
        add_log_entry('*** SUCCESS', '')

        if os.path.exists(MDS_EXTRACT) and os.stat(MDS_EXTRACT).st_size != 0: # if file exists and is not empty
        
            MDS_EXTRACT_FILE = open(MDS_EXTRACT, 'r')
            
            # RUN LOAD_SQL ###########################################################################################################
            add_log_entry('*** RUNNING FILE LOAD...', os.path.basename(MDS_EXTRACT) + ' into ' + MDS_TABLE)
            try:
                cur.copy_from(MDS_EXTRACT_FILE, MDS_TABLE, sep=MDS_EXTRACT_DELIM)
                conn.commit()
            except Exception, e:
                MDS_EXTRACT_FILE.close()
                add_log_entry('MDS FILE LOAD ERROR', 'Error loading file: '+os.path.basename(MDS_EXTRACT)+': Table: '+MDS_TABLE+': '+str(e))
                sys.exit(1)
            MDS_EXTRACT_FILE.close()
            
            add_log_entry('*** SUCCESS', '')
            cur.close()            
        else:
            add_log_entry('NO DATA TO LOAD', os.path.basename(MDS_EXTRACT))

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
