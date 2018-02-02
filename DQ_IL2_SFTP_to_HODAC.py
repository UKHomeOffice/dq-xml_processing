#!/usr/bin/python

##############################################################################################################################
# DQ_IL2_SFTP_to_HODAC
#
# AUTHOR:      Stewart Lee
# DATE:        2016/07/11
#
# DESCRIPTION:
#
# This script does the following:
#
# - Takes config variables from the ./DQ_IL2_Config.ini file
# - SSH into the target host and opens SFTP
# - Reads any file matching "ACCEPTED_REGEX" from the hodac source folder
# - SFTPs the files with a .tmp filename
# - Once transfer is complete, renames the file back to the original (no .tmp suffix)
# - Deletes the file from the hodac source folder
#
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
import datetime, re, time, sys, os
import logging, ConfigParser
import paramiko # reqd for SSH/SFTP "yum install python-paramiko"
### GLOBAL VARIABLES #########################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
##############################################################################################################################


# C:\Users\lees\Desktop\_backups\_keys\drt\openssh

def add_log_entry(log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    if DEBUG: print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    LOGFILE.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def ssh_login(in_host, in_user, in_keyfile):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy()) ## This line can be removed when the host is added to the known_hosts file
    privkey = paramiko.RSAKey.from_private_key_file (in_keyfile)
    try:
        ssh.connect(in_host, username=in_user,pkey=privkey )
    except Exception, e:
        add_log_entry('SSH LOGIN FAILED',str(e))
        sys.exit(1)
    return ssh
# end def ssh_login

def sftp_remove_ignore_io_error(sftp,file_path):
        try:
                sftp.remove(file_path)
        except IOError, e:
                pass

def main():
        ### OTHER VARIABLES ##########################################################################################################
        global DEBUG
        DEBUG=1
        STARTTIME = datetime.datetime.now()
        ACCEPTED_REGEX=r'^PARSED_.*\.zip$' + '|' + r'^STORED_.*\.zip$' + '|' + r'^FAILED_.*\.zip$' + '|' + r'^RAW_.*\.zip$'
        ### READ CONFIG FILE #######################################################################################################
        CONFIG_FILE='DQ_IL2_Config.ini'
        DEFAULT_SECTION='DEFAULT'
        CUSTOM_SECTION='DQ_IL2_SFTP_to_HODAC'

        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILE)

        ROOT_DIR                = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))
        SOURCE_DIR              = re.sub("/*$","/",config.get(CUSTOM_SECTION,'SOURCE_DIR'))
        TARGET_DIR              = config.get(CUSTOM_SECTION,'TARGET_DIR')
        HODAC_HOST              = config.get(CUSTOM_SECTION,'HODAC_HOST')
        HODAC_USER              = config.get(CUSTOM_SECTION,'HODAC_USER')
        SSH_PRIVATE_KEY         = config.get(CUSTOM_SECTION,'SSH_PRIVATE_KEY')
        DEBUG                   = int(config.get(DEFAULT_SECTION,'DEBUG'))
        
        LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
        
        ### LOG FILE VARIABLES #######################################################################################################
        global LOGFILE
        LOGFILENAME=LOGFILE_DIR + CUSTOM_SECTION + '_' + YYYYMMDDSTR + '.log'
        LOGFILE=open(LOGFILENAME,'a')

        add_log_entry('*** STARTING ***', os.path.basename(sys.argv[0]))
        add_log_entry('SSH LOGIN','Connecting to ' + HODAC_HOST + ' as ' + HODAC_USER + ' using ' + SSH_PRIVATE_KEY)

        ssh=ssh_login(HODAC_HOST, HODAC_USER, SSH_PRIVATE_KEY)
        
        add_log_entry('SSH LOGIN','Connection successful')
        add_log_entry('SSH LOGIN','Opening SFTP...')

        sftp = ssh.open_sftp()
        
        add_log_entry('SSH LOGIN','SFTP opened')

        SOURCE_DIR_LIST = [os.path.join(SOURCE_DIR,f) for f in os.listdir(SOURCE_DIR) if re.match(ACCEPTED_REGEX, f)]

        if SOURCE_DIR_LIST: 
            for file_name in SOURCE_DIR_LIST:
                
                TMP_FILENAME=os.path.basename(file_name) + '.tmp'
                FULL_FILENAME=file_name
                SFTP_ERROR=False
                
                sftp_remove_ignore_io_error(sftp,TARGET_DIR+'/'+TMP_FILENAME)
                add_log_entry('SFTP','Putting file ' +FULL_FILENAME+' as ' +os.path.basename(TMP_FILENAME))
                sftp.chdir(TARGET_DIR)
                try:
                        sftp.put(FULL_FILENAME,TARGET_DIR+'/'+TMP_FILENAME)
                except Exception, e:
                        add_log_entry('SFTP','Error putting file: ' + os.path.basename(FULL_FILENAME))
                        add_log_entry('SFTP',str(e))
                        SFTP_ERROR=True
                        
                if not SFTP_ERROR:
                        filename_basename=os.path.basename(file_name)
                        
                        TMP_FILENAME=filename_basename + '.tmp'
                        FULL_FILENAME=file_name.replace(u'\\','/')
                        
                        SFTP_ERROR=False
                        
                        sftp_remove_ignore_io_error(sftp,TARGET_DIR+'/'+filename_basename)
                        add_log_entry('SFTP','Renaming file ' +TARGET_DIR+'/'+TMP_FILENAME + ' to ' + filename_basename)
                        sftp.rename(TARGET_DIR+'/'+TMP_FILENAME,TARGET_DIR+'/'+filename_basename)
                        add_log_entry('CLEANUP','Removing local file ' + filename_basename)                    
                        os.remove(FULL_FILENAME)

        else:
            add_log_entry('SFTP','No source files')

if __name__ == '__main__':
    main()
