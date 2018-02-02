#!/usr/bin/python

##############################################################################################################################
# 
# AUTHOR:      Stewart Lee
# DATE:        2016/10/16
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
import datetime, re, time, sys, os, argparse,shutil
import logging, ConfigParser
import paramiko # reqd for SSH/SFTP "yum install python-paramiko"
### GLOBAL VARIABLES #########################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
##############################################################################################################################


def add_log_entry(msg, mode):
    if DEBUG: print msg
    if mode=='info':
        LOGFILE.info(msg)
    elif mode=='exception':
        LOGFILE.exception(msg)

def ssh_login(in_host, in_user, in_keyfile):
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy()) ## This line can be removed when the host is added to the known_hosts file
	privkey = paramiko.RSAKey.from_private_key_file (in_keyfile)
	try:
            ssh.connect(in_host, username=in_user,pkey=privkey )
	except Exception, e:
                add_log_entry(str(e), EXCEPTION_MODE)
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
        global DEBUG, INFO_MODE, EXCEPTION_MODE
        DEBUG=1
        INFO_MODE='info'
        EXCEPTION_MODE='exception'
        STARTTIME = datetime.datetime.now()
        JSON_REGEX=r'^.*\.json$'
        NATS_EXPORT_FILENAME='nats.csv'
        ### READ CONFIG FILE #######################################################################################################
        CONFIG_FILE='DQ_IL2_Config.ini'
        DEFAULT_SECTION='DEFAULT'
        CUSTOM_SECTION='NATS_IL2_SFTP_Archive_FPL_Files'

        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILE)

        ROOT_DIR                = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))
        SOURCE_DIR              = re.sub("/*$","/",config.get(CUSTOM_SECTION,'SOURCE_DIR'))
        TARGET_DIR              = config.get(CUSTOM_SECTION,'TARGET_DIR')
        EXT_HOST                = config.get(DEFAULT_SECTION,'EXT_HOST')
        EXT_USER                = config.get(CUSTOM_SECTION,'EXT_USER')
        SSH_PRIVATE_KEY         = config.get(CUSTOM_SECTION,'SSH_PRIVATE_KEY')
        DEBUG                   = int(config.get(CUSTOM_SECTION,'DEBUG'))
        
        LOGFILE_DIR=os.path.join(ROOT_DIR, 'log/')
        
        ### LOG FILE VARIABLES #######################################################################################################
        global LOGFILE, AUDIT_LOG
        LOGFILENAME=LOGFILE_DIR + CUSTOM_SECTION + '_' + YYYYMMDDSTR + '.log'

        if DEBUG==1:
            logging.basicConfig(
                    filename=LOGFILENAME,
                    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG
            )
        else:
            logging.basicConfig(
                    filename=LOGFILENAME,
                    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO
            )

        LOGFILE=logging.getLogger('NATS')

        add_log_entry('\n*** STARTING ***', INFO_MODE)
        add_log_entry('Connecting to %s as %s' % (EXT_HOST,EXT_USER),INFO_MODE + '...')

        ssh=ssh_login(EXT_HOST, EXT_USER, SSH_PRIVATE_KEY)
        
        add_log_entry('Connection successful',INFO_MODE)
        add_log_entry('Opening SFTP...',INFO_MODE)

        sftp = ssh.open_sftp()
        
        add_log_entry('SFTP opened',INFO_MODE)

        SOURCE_DIR_LIST = [os.path.join(SOURCE_DIR,f) for f in sftp.listdir(SOURCE_DIR) if (re.match(JSON_REGEX, f))]
        FILE_COUNT=0
        if SOURCE_DIR_LIST: 
            for file_name in SOURCE_DIR_LIST:
                
                filename_basename=os.path.basename(file_name)
                TMP_FILENAME='%s.tmp'%(filename_basename)
                SFTP_ERROR=False
                
                if os.path.exists(os.path.join(TARGET_DIR,TMP_FILENAME)):
                    os.remove(os.path.join(TARGET_DIR,TMP_FILENAME))
                    
                add_log_entry('Processing % s' % (file_name),INFO_MODE)
                try:
                        sftp.get(file_name,os.path.join(TARGET_DIR,TMP_FILENAME))
                except Exception, e:
                        add_log_entry('Error putting file: %s' % (os.path.basename(file_name)),EXCEPTION_MODE)
                        add_log_entry(str(e),EXCEPTION_MODE)
                        SFTP_ERROR=True
                if not SFTP_ERROR:
                        
                        SFTP_ERROR=False
                        
                        if os.path.exists(os.path.join(TARGET_DIR,filename_basename)):
                            try:
                                os.remove(os.path.join(TARGET_DIR,filename_basename))
                            except Exception, e:
                                add_log_entry(str(e),EXCEPTION_MODE)

                        try:
                            shutil.move(os.path.join(TARGET_DIR,TMP_FILENAME),os.path.join(TARGET_DIR,filename_basename))
                        except Exception, e:
                            add_log_entry(str(e),EXCEPTION_MODE)
                            
                        try:
                            sftp.remove(file_name)
                        except Exception, e:
                            add_log_entry(str(e),EXCEPTION_MODE)
                FILE_COUNT+=1
            sftp.close()
            ssh.close()
            add_log_entry('TRANSFER COMPLETE',INFO_MODE)
            add_log_entry('%s file(s) processed'%(FILE_COUNT),INFO_MODE)
        else:
            add_log_entry('No source files',INFO_MODE)

if __name__ == '__main__':
    main()
