#!/usr/bin/env python

"""
batch_move_files.py

Batch moves files (default: move) from a given folder into a folder matching the filedate from the filename.

If one "target_dir" location is given, all files are written here in a new folder name given by "target_dir"/yyyymmdd
If more than one "target_dir" location is specified, files are written to each location based on file date month, e.g.

For 2 target_dirs then months are split as follows:  (2, 4, 6, 8, 10, 12) and (1, 3, 5, 7, 9, 11).
i.e. Feb, Apr, June etc. (even months) are written to the first location whereas odd are written to the second.

For 3 target_dirs then months are split as follows: (3, 6, 9, 12) , (2, 5, 8, 11), (1, 4, 7, 10)

The archive folders are grouped by day, e.g. 20171013.

Configuration options are:

[default]
source_file_dir     = <source file dir, e.g. ../archive>
logfile_dir         = <log file dir, e.g. ../log>
max_batch_size      = <the max no. of files to process per filetype, e.g. 3>
move_file           = <to either move file, or robocopy file, default is move, e,g, True>
log_frequency       = midnight
log_interval        = 1
log_backup_count    = 365
debug               = 1

[parsed]
regex               = ^PARSED_[0-9]{8}_[0-9]{4}_[0-9]{4}.*\.zip$
target_dir          = <folder to archive to - can have multiple values separated by "\n", e.g. ../target1/PARSED_ARCHIVE>
retention_days      = <number of days worth of files to retain on the source server, e.g. 30 retains 30 days worth, and archives when 31 days old>

[stored]
regex               = ^STORED_[0-9]{8}_[0-9]{4}_[0-9]{4}.*\.zip$
target_dir          = <folder to archive to - can have multiple values separated by "\n", e.g. ../target1/STORED_ARCHIVE>
retention_days      = <number of days worth of files to retain on the source server, e.g. 30 retains 30 days worth, and archives when 31 days old>

[failed]
regex               = ^FAILED_[0-9]{8}_[0-9]{4}_[0-9]{4}.*\.zip$
target_dir          = <folder to archive to - can have multiple values separated by "\n", e.g. ../target1/FAILED_ARCHIVE>
retention_days      = <number of days worth of files to retain on the source server, e.g. 30 retains 30 days worth, and archives when 31 days old>
"""

import os
import sys
import datetime
from datetime import timedelta
import shutil
import re
import subprocess
import logging
import ConfigParser
from logging.handlers import TimedRotatingFileHandler

info_logger = logging.getLogger('batch move')


def setup_logger(name, log_file, debug=False, write_to_console=True, log_frequency='D', log_interval=1, log_backup_count=0):
    """
    Setup python loggers to file and handle standard output
    :param name:
    :param log_file: the location of the log file
    :param debug: set to True to increase logging verbosity
    :param write_to_console: output to stdout
    :param log_frequency:
    :param log_interval:
    :param log_backup_count:
    :returns: logger
    """
    logger = logging.getLogger(name)
    level = logging.INFO

    if debug:
        level = logging.DEBUG

    formatter = logging.Formatter('%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s', '%Y-%m-%d %H:%M:%S')
    handler = TimedRotatingFileHandler(log_file, when=log_frequency, interval=log_interval, backupCount=log_backup_count)
    handler.suffix = "%Y-%m-%d.log"
    handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
    handler.setFormatter(formatter)

    if write_to_console:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def run_command(command):
    """Runs a local command and returns the return_code.
    This function fails if the command fails.
    :param command:
    :returns: int
    """

    pipe = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    output, err = pipe.communicate()
    return_code = pipe.returncode

    if return_code not in (0, 1):
        err_msg = 'Error running %s, %s' % (command, err)
        info_logger.info(err_msg)
        raise Exception(err_msg)

    return return_code


def get_time_delta_in_secs(start_time, end_time=None):
    """
    Returns the difference between 'start_time' and 'end_time' in seconds/microseconds
    :param start_time:
    :param end_time:
    :returns: string
    """
    if end_time is None:
        end_time = datetime.datetime.now()
    delta = end_time - start_time
    elapsed_time = '%s.%s' % (delta.seconds, '%06d' % delta.microseconds)

    return elapsed_time


def archive_files(source_dir, target_dirs, regex, retention_date, max_batch_size=5, move_file=False):
    """
    Moves files matching regex from source to target to locations lists by archive_loc_list
    :param source_dir:
    :param target_dirs:
    :param regex:
    :param retention_date:
    :param max_batch_size:
    :param move_file:
    :returns: None
    """
    info_logger.info('Processing file(s) matching "%s"' % (regex))
    source_dir_list = [f for f in os.listdir(source_dir) if (re.match(regex, f)) and int(f.split('_')[1]) < retention_date]
    source_dir_list.sort()

    if source_dir_list:

        filecount = 0

        for f in source_dir_list[:max_batch_size]:
            full_filepath = os.path.join(source_dir, f)
            filename_yyyymmdd = f.split('_')[1]
            filename_mm = int(filename_yyyymmdd[4:][:2])

            target_dir = target_dirs[filename_mm % len(target_dirs)]

            try:
                os.makedirs(os.path.join(target_dir, filename_yyyymmdd))
            except:
                pass

            if not move_file:
                command = 'robocopy /j /move "%s" "%s" "%s"' % (source_dir, os.path.join(target_dir, filename_yyyymmdd), f)
                run_command(command)
                info_logger.info('Robocopied %s to %s' % (f, target_dir))
            else:
                shutil.move(full_filepath, os.path.join(target_dir, filename_yyyymmdd, f))
                info_logger.info('Moved %s to %s' % (f, target_dir))

            filecount += 1
        info_logger.info('Moved %s file(s)' % (filecount))
    else:
        info_logger.info('No items')


def main(argv):
    starttime = datetime.datetime.now()
    config_file = os.path.join(os.path.dirname(__file__), os.path.basename(__file__).replace('.py', '.ini'))
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    source_file_dir = config.get('DEFAULT', 'source_file_dir')
    logfile_dir = config.get('DEFAULT', 'logfile_dir')
    max_batch_size = int(config.get('DEFAULT', 'max_batch_size'))

    move_file = config.getboolean('DEFAULT', 'move_file')
    log_frequency = config.get('DEFAULT', 'log_frequency')
    log_interval = int(config.get('DEFAULT', 'log_interval'))
    log_backup_count = int(config.get('DEFAULT', 'log_backup_count'))
    debug = config.getboolean('DEFAULT', 'debug')

    log_filename = os.path.join(logfile_dir, '%s.log' % (os.path.basename(__file__)))

    info_logger = setup_logger('batch move', log_filename, debug=debug, log_frequency=log_frequency, log_interval=log_interval, log_backup_count=log_backup_count)
    info_logger.info('Starting %s' % (os.path.basename(sys.argv[0])))

    filetypes = config.sections()

    for filetype in filetypes:
        if config.has_section(filetype):
            retention_days = int(config.get(filetype, 'retention_days'))
            retention_date = int((datetime.datetime.now() - timedelta(days=retention_days)).strftime('%Y%m%d'))

            target_dirs = []
            for target_dir in config.get(filetype, 'target_dir').split('\n'):
                target_dirs.append(target_dir)

            archive_files(source_file_dir, target_dirs, config.get(filetype, 'regex'), retention_date, max_batch_size=max_batch_size, move_file=move_file)

    info_logger.info('Elapsed time: %s' % (get_time_delta_in_secs(starttime)))


if __name__ == "__main__":
    main(sys.argv[1:])
