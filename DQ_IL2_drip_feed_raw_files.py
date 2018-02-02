#!/usr/bin/env python

#DQ_IL2_drip_feed_raw_files.py -i "C:/Users/lees.DQDEV/Desktop/env/archive/" -o "C:/Users/lees.DQDEV/Desktop/env/nrt_input/" -l "C:/Users/lees.DQDEV/Desktop/env/log/" ^
#-s "20150427_1000_0001" -t 2 -n 20 -m 60


### IMPORT PYTHON MODULES 
import os, re, time, sys, shutil, getopt
from datetime import datetime

##############################################################################################################################
YYYYMMDDSTR = time.strftime("%Y%m%d")
YYYYMMDDHHMISSSTR = time.strftime("%Y%m%d%H%M%S")
YYYYMMDDHHSTR = time.strftime("%Y%m%d%H")
##############################################################################################################################

def overwrite_file(from_file, to_file):
    if os.path.exists(to_file):
       os.remove(to_file)
    os.rename(from_file,to_file)

def add_log_entry(log_obj, log_summary, log_msg):
    curr_time = time.strftime("%Y%m%d%H%M%S")
    print curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg
    log_obj.write(curr_time + '\t' + log_summary.ljust(28,' ') + '\t' + log_msg + '\n')

def main(argv):
   
   ### REGEX VARIABLES ##########################################################################################################
   RAW_REGEX=        r'^RAW_[0-9]{8}_[0-9]{4}_[0-9]{4}.*\.zip$'

   STARTTIME = datetime.now()

   try:
          opts, args = getopt.getopt(argv,"i:o:t:n:s:l:m:",["indir=","outdir=","sleeptime=","maxfiles="])
   except getopt.GetoptError:
          print 'Problem reading options'
          print_usage()
          sys.exit(2)

   FROM_DIR='C:\\Users\\lees.DQDEV\\Desktop\\env\\archive\\'
   TO_DIR='C:\\Users\\lees.DQDEV\\Desktop\\env\\nrt_input\\'
   LOGFILE_DIR='C:\\Users\\lees.DQDEV\\Desktop\\env\\log\\'
   SLEEPTIME=3
   MAXFILES=20
   MAXBATCHSIZE=60
   START_FILEDATE_SEQ=''
   
   for opt, arg in opts:
       if opt in ("-i"):
               FROM_DIR = re.sub("/*$","/",arg)
       elif opt in ("-o"):
               TO_DIR = re.sub("/*$","/",arg)
       elif opt in ("-l"):
               LOGFILE_DIR = re.sub("/*$","/",arg)
       elif opt in ("-s"):
               START_FILEDATE_SEQ = arg
       elif opt in ("-t"):
               SLEEPTIME = int(arg)
       elif opt in ("-n"):
               MAXFILES = int(arg)
       elif opt in ("-m"):
               MAXBATCHSIZE = int(arg)

   START_FILEDATE_SEQ = int(re.sub("_","",START_FILEDATE_SEQ))
   LOGFILENAME=LOGFILE_DIR + 'DQ_IL2_drip_feed_raw_files_' + YYYYMMDDSTR + '.log'
   LOGFILE = open(LOGFILENAME, 'a')
   ##############################################################################################################################
   # START
   ##############################################################################################################################

   while True:
       GLOBAL_COUNTER=0
       from_dir_list =  [f for f in os.listdir(FROM_DIR) if (re.match(RAW_REGEX, f))]
       from_dir_list.sort()
       to_dir_list = [f for f in os.listdir(TO_DIR) if (re.match(RAW_REGEX, f))]
       if len(to_dir_list) >= MAXBATCHSIZE:
           add_log_entry(LOGFILE,'BATCH LIMIT REACHED', str(MAXBATCHSIZE) + ' file(s)') 
       elif from_dir_list:
          SRC_FILE_COUNTER=0
          for filename in from_dir_list:
              filedateseq =  int(re.sub("_","",filename[4:22]))
              if filedateseq >= START_FILEDATE_SEQ:
                  overwrite_file(FROM_DIR + filename, TO_DIR + filename)
                  add_log_entry(LOGFILE,'MOVED FILE', filename)
                  SRC_FILE_COUNTER+=1
                  GLOBAL_COUNTER+=1
              if (SRC_FILE_COUNTER >= MAXFILES):
                  break;

       #os.system('cls')
       add_log_entry(LOGFILE,'Moved ' + str(GLOBAL_COUNTER) + ' file(s)', '')
       time.sleep(SLEEPTIME)
       LOGFILE.close()
       LOGFILE = open(LOGFILENAME, 'a')


   ##############################################################################################################################
   # SCRIPT END 
   ##############################################################################################################################



# Run the main function
if __name__ == "__main__":
	main(sys.argv[1:])
