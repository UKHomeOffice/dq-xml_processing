#!/usr/bin/python

##############################################################################################################################
# 
# AUTHOR:      Stewart Lee
# DATE:        2016/10/16
#
##############################################################################################################################

### IMPORT PYTHON MODULES ####################################################################################################
import datetime, re, time, sys, os, argparse
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
        MDS_EXPORT_FILENAME='MDS_EXTRACT.csv'
        ### READ CONFIG FILE #######################################################################################################
        CONFIG_FILE='DQ_IL2_Config.ini'
        DEFAULT_SECTION='DEFAULT'
        CUSTOM_SECTION='NATS_IL2_SFTP_MDS_Extract'

        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILE)

        ROOT_DIR                = re.sub("/*$","/",config.get(DEFAULT_SECTION,'ROOT_DIR'))
        SOURCE_DIR              = re.sub("/*$","/",config.get(CUSTOM_SECTION,'SOURCE_DIR')) # The local MDS folder
        TARGET_DIR              = re.sub("/*$","/",config.get(CUSTOM_SECTION,'TARGET_DIR')) # The remote MDS folder
        EXT_HOST                = config.get(DEFAULT_SECTION,'EXT_HOST')
        EXT_USER                = config.get(CUSTOM_SECTION,'EXT_USER')
        SSH_PRIVATE_KEY         = config.get(CUSTOM_SECTION,'SSH_PRIVATE_KEY')
        DEBUG                   = int(config.get(DEFAULT_SECTION,'DEBUG'))
        
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
        add_log_entry('Connecting to ' + EXT_HOST + ' as ' + EXT_USER,INFO_MODE + '...')

        ssh=ssh_login(EXT_HOST, EXT_USER, SSH_PRIVATE_KEY)
        
        add_log_entry('Connection successful',INFO_MODE)
        add_log_entry('Opening SFTP...',INFO_MODE)

        sftp = ssh.open_sftp()
        
        add_log_entry('SFTP opened',INFO_MODE)

        #open(os.path.join(TARGET_DIR,MDS_EXPORT_FILENAME), 'a').close()
        sftp.chdir(TARGET_DIR)

        try:
            sftp_remove_ignore_io_error(sftp,os.path.join(TARGET_DIR,MDS_EXPORT_FILENAME+'.tmp'))
	    sftp.put(os.path.join(SOURCE_DIR,MDS_EXPORT_FILENAME),os.path.join(TARGET_DIR,MDS_EXPORT_FILENAME+'.tmp'))
	    sftp_remove_ignore_io_error(sftp,os.path.join(TARGET_DIR,MDS_EXPORT_FILENAME))
	    sftp.rename(os.path.join(TARGET_DIR,MDS_EXPORT_FILENAME+'.tmp'),os.path.join(TARGET_DIR,MDS_EXPORT_FILENAME))
            add_log_entry('%s SFTP\'d successfully'%(MDS_EXPORT_FILENAME),INFO_MODE)
        except Exception, e:
            add_log_entry(str(e),INFO_MODE)
       
        sftp.close()
        ssh.close()

if __name__ == '__main__':
    main()
