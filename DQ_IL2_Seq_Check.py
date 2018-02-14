#!/usr/bin/env python

"""
DQ_IL2_Seq_Check.py

AUTHOR:      Stewart Lee
DATE:        2015/07/14
UPDATED:     2017/11/06

DESCRIPTION:

This script does the following:
- Checks the current sequence values from the MAX_LOGS.log file
- Checks/validates sequences of the .zip files in the SOURCE_FILE_DIR
- Updates the MAX_LOGS.log file with new sequences
- Unzips the files using multicore processing
- Move the files to an output folder
- Removes temporary folders
- Checks how old the MDS extract file is and if older than MDS_REFRESH_HRS, then it is refreshed from the MDS database
- Parses all xml files using multicore processing

        #<commonAPIPlus>
        #    <APIData>
        #        <flightDetails>
        #            <flightId>XXX_GA</flightId>
        #        </flightDetails>
        #    </APIData>
        #</commonAPIPlus>

- Filters GA rows from the xmls (all files are written to the "out" folder)
- Updated to fix sequences bug which does not handle sequences resetting at midnight
- If the record does not have an API tag then it is removed
"""

import os
import sys
import time
import datetime
import zipfile
import re
import shutil
import logging
import ConfigParser
import xml.etree.ElementTree as ET
import pyodbc
import getpass
import multiprocessing
import itertools
from logging.handlers import TimedRotatingFileHandler
from multiprocessing import freeze_support

info_logger = logging.getLogger('Seq Check')
seq_logger = logging.getLogger('Sequences')


# Logging function


def setup_logger(name, log_file, debug=False, write_to_console=False, log_frequency='D', log_interval=1, log_backup_count=0):
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


# Filesystem functions


def prepare_batch_files(output_file_dir, max_output_batch_size, ftp_landing_zone, source_file_dir, regex, max_batch_size, filetypes):
    """
    Moves batch of files from source to target matching the file suffix and expected filetypes
    :param output_file_dir:
    :param max_output_batch_size:
    :param ftp_landing_zone:
    :param source_file_dir:
    :param regex:
    :param max_batch_size:
    :param filetypes:
    :returns: None
    """

    output_dir_length = len(os.listdir(output_file_dir))

    if output_dir_length > max_output_batch_size:
        info_logger.warn('Output batch size exceeded: %s files in %s' % (output_dir_length, output_file_dir))
        return 0
    else:
        info_logger.info('Output batch size ok: %s file(s) in %s' % (output_dir_length, output_file_dir))

    for filetype in filetypes:
        ftp_landing_zone_dir_list = [f for f in os.listdir(ftp_landing_zone) if re.match(regex, f) and f.startswith(filetype)]
        ftp_landing_zone_dir_list.sort()

        batch_dir_list = [f for f in os.listdir(source_file_dir) if re.match(regex, f) and f.startswith(filetype)]
        current_batch_size = len(batch_dir_list)
        if current_batch_size >= max_batch_size:
            info_logger.warn('%s %s zipfile(s) present - no files added ' % (max_batch_size, filetype))
        elif ftp_landing_zone_dir_list:
            ftp_landing_zone_file_counter = 0
            for filename in ftp_landing_zone_dir_list:
                shutil.move(os.path.join(ftp_landing_zone, filename), os.path.join(source_file_dir, filename))
                info_logger.info('Moved %s' % filename)
                ftp_landing_zone_file_counter += 1
                if ftp_landing_zone_file_counter + current_batch_size >= max_batch_size:
                    info_logger.info('Batch limit reached: %s %s files' % (max_batch_size, filetype))
                    break
        else:
            info_logger.info('No %s files' % (filetype))

    return len([f for f in os.listdir(source_file_dir) if re.match(regex, f)])


def copy_files_for_aws(source_dir_list, source_folder, target_folder):
    """
    Copy files in source list from source to target
    :param source_dir_list:
    :param source_folder:
    :param target_folder:
    :returns: None
    """
    file_count = 0
    temporary_folder = os.path.join(target_folder,'tmp')
    for fname in source_dir_list:
        shutil.copy(os.path.join(source_folder, fname), os.path.join(temporary_folder, fname))
        shutil.move(os.path.join(temporary_folder, fname), os.path.join(target_folder, fname))
        file_count += 1
    info_logger.info(str(file_count) + ' file(s) copied to ' + target_folder)


def move_files(source_dir_list, source_folder, target_folder):
    """
    Moves files in source list from source to target
    :param source_dir_list:
    :param source_folder:
    :param target_folder:
    :returns: None
    """
    file_count = 0
    for fname in source_dir_list:
        shutil.move(os.path.join(source_folder, fname), os.path.join(target_folder, fname))
        file_count += 1
    info_logger.info(str(file_count) + ' file(s) moved')


def remove_temp_folders(source_folder, regex):
    """
    Removes folders in source folder matching regex from source to target
    :param source_folder:
    :param regex:
    :returns: None
    """
    source_folder_list = [f for f in os.listdir(source_folder) if re.match(regex, f) and os.path.isdir(os.path.join(source_folder, f))]
    if source_folder_list:

        for folder in source_folder_list:
            shutil.rmtree(os.path.join(source_folder, folder))
            info_logger.info('Removing %s' % (folder))
    else:
        info_logger.info('Nothing to cleanup')


# Numeric functions


def modulo_seq_add(in_val, in_val_to_add, max_file_seq):
    """ Increments sequence numbers in the format "0000" (i.e. 4 digits, padded with zeroes)
    The max_file_seq is set to 9999+1 (not 1440) ... the reset back to 0 is handled by comparing the current date to that in the file name - if more then reset to "0001"
    :param in_val:
    :param in_val_to_add:
    :param max_file_seq:
    :returns: string
    """
    return str(((int(in_val)+in_val_to_add) % max_file_seq)).zfill(4)


# Multiprocessing functions


def mp_unzip_files(multiprocessing_pool_vars):
    """ Unzip a file from a zipfile to a given folder
    multiprocessing_pool_vars (iterable): [zipfilename, target_dir]
    :param in_val_to_pad:
    :returns: string
    """
    zipfilename = multiprocessing_pool_vars[0]
    target_dir = multiprocessing_pool_vars[1]
    dirname = re.sub('\.zip$', '', os.path.join(target_dir, os.path.basename(zipfilename)))

    if not os.path.exists(dirname):
        os.makedirs(dirname)
    try:
        with zipfile.ZipFile(zipfilename) as zf:
            zf.extractall(dirname)
            zf.close()
    except Exception, e:
        return [False, os.path.basename(zipfilename) + ': ' + str(e)]
    return [True, os.path.basename(zipfilename)]


def mp_parse_xml(multiprocessing_pool_vars):
    """ Parses XML, finds GA rows (either from flight_id or matched MDS GA carriers), writes to output files
    multiprocessing_pool_vars (iterable) [filename, root_dir, parsed_flight_id_xml_path, iata_executive_carriers, icao_executive_carriers]
    :param multiprocessing_pool_vars:
    :returns: list
    """
    filename = multiprocessing_pool_vars[0]
    root_dir = multiprocessing_pool_vars[1]
    parsed_flight_id_xml_path = multiprocessing_pool_vars[3] + multiprocessing_pool_vars[2]
    parsed_api_xml_path = multiprocessing_pool_vars[3]
    iata_executive_carriers = multiprocessing_pool_vars[4]
    icao_executive_carriers = multiprocessing_pool_vars[5]

    output_file_dir = os.path.join(root_dir, 'out/')
    ga_inprocess_dir = os.path.join(root_dir, 'ga_inprocess/')
    reject_file_dir = os.path.join(root_dir, 'reject/')

    filename_basename = os.path.basename(filename)

    is_ga_record = False

    try:
        tree = ET.parse(filename)
    except Exception, e:
        shutil.move(filename, os.path.join(reject_file_dir, filename_basename))
        return [False, filename_basename + ': ' + str(e), None]

    if len(tree.findall(parsed_api_xml_path)):

        p = tree.findall(parsed_flight_id_xml_path)

        for flightId in p:

            if flightId.text is not None:
                flight_id = flightId.text.strip()
                if re.search('^_GA.*$', flight_id) or is_executive_flight(flight_id, iata_executive_carriers, 2) or is_executive_flight(flight_id, icao_executive_carriers, 3):
                    is_ga_record = True
                    break

        if is_ga_record:
            shutil.copy(filename, os.path.join(ga_inprocess_dir, filename_basename))

    else:
        os.remove(filename)
        return [True, filename_basename, 'PNR']

    shutil.move(filename, os.path.join(output_file_dir, filename_basename))
    return [True, filename_basename, 'API']


def mp_move_files(multiprocessing_pool_vars):
    """ Moves the file as given with multiprocessing_pool_vars (iterable)
    multiprocessing_pool_vars: [filename, from_dir, to_dir]
    :param multiprocessing_pool_vars:
    :returns: list
    """
    filename = multiprocessing_pool_vars[0]
    from_dir = multiprocessing_pool_vars[1]
    to_dir = multiprocessing_pool_vars[2]

    from_filename = os.path.join(from_dir, filename)
    to_filename = os.path.join(to_dir, filename)

    if os.path.exists(to_filename):
        os.remove(to_filename)
    try:
        os.rename(from_filename, to_filename)
    except Exception, e:
        return [False, os.path.basename(from_filename) + ': ' + str(e)]
    return [True, os.path.basename(from_filename)]


def process_mp_unzip_files(pool, source_dir_list, source_file_dir, target_file_dir, regex, no_of_processes=4):
    parsed_zipfile_list = [os.path.join(source_file_dir, f) for f in source_dir_list if re.match(regex, f)]

    zip_count = len(parsed_zipfile_list)

    if parsed_zipfile_list:
        info_logger.info('Unzipping/copying: Starting (No. of processes: %s)' % (no_of_processes))

        results = pool.map(mp_unzip_files, itertools.izip(parsed_zipfile_list,  itertools.repeat(target_file_dir)))

        info_logger.info('Unzipping/copying: done (%s file(s) processed)' % (zip_count))

        check_multiprocessing_errors(results)
    else:
        info_logger.info('No source files')


def process_mp_parse_xml(pool, target_file_dir, root_dir, parsed_ns, iata_executive_carriers, icao_executive_carriers, ga_inprocess_dir, ga_file_dir, no_of_processes=4):
    xml_list = [os.path.join(root, filename)
                for root, dirnames, filenames in os.walk(target_file_dir)
                for filename in filenames if filename.lower().endswith('.xml')]

    if xml_list:

        info_logger.info('Parsing: Starting (No. of processes: %s)' % (no_of_processes))
        results = pool.map(mp_parse_xml, itertools.izip(xml_list,
                                                        itertools.repeat(root_dir),
                                                        itertools.repeat("/{%s}flightDetails/{%s}flightId" % (parsed_ns, parsed_ns)),
                                                        itertools.repeat("{%s}APIData" % (parsed_ns)),
                                                        itertools.repeat(iata_executive_carriers),
                                                        itertools.repeat(icao_executive_carriers)))

        info_logger.info('Parsing XML: Done (%s file(s) processed)' % (len(xml_list)))
        check_multiprocessing_parse_xml_errors(results)

        ga_inprocess_dir_list = [f for f in os.listdir(ga_inprocess_dir) if f.lower().endswith('.xml')]

        if ga_inprocess_dir_list:

            info_logger.info('Moving GA files')
            results = pool.map(mp_move_files, itertools.izip(ga_inprocess_dir_list, itertools.repeat(ga_inprocess_dir), itertools.repeat(ga_file_dir)))

            info_logger.info('Done (%s GA file(s) processed)' % (len(ga_inprocess_dir_list)))

            check_multiprocessing_errors(results)
        else:
            info_logger.info('No source files')
    else:
        info_logger.info('No source files')


def check_multiprocessing_errors(results_list):
    """ Takes a list of list objects e.g. [[False, <error msg>],[True, <error msg>]] and outputs to log when an error has been encountered
    :param results_list:
    :returns: list
    """
    no_of_errors = 0
    for result in results_list:
        success = result[0]
        details = result[1]
        if not success:
            no_of_errors += 1
            info_logger.info('Multiprocessing error: %s' % (details))
    info_logger.info('Multiprocessing errors: %s' % (no_of_errors))


def check_multiprocessing_parse_xml_errors(results_list):
    """ Takes a list of list objects e.g. [[False, <error msg>],[True, <error msg>]] and outputs to log when an error has been encountered
    :param results_list:
    :returns: list
    """
    no_of_errors = 0
    api_count = 0
    pnr_count = 0
    for result in results_list:
        success = result[0]
        details = result[1]
        msg_type = result[2]
        if not success:
            no_of_errors += 1
            info_logger.info('Multiprocessing error: %s' % (details))
        if msg_type == 'API':
            api_count += 1
        elif msg_type == 'PNR':
            pnr_count += 1

    info_logger.info('Total multiprocessing errors: %s' % (no_of_errors))
    info_logger.info('API count: %s' % (api_count))
    info_logger.info('PNR count: %s' % (pnr_count))

# GA functions


def is_executive_flight(flight_id, executive_carriers, expected_length):
    if (len(flight_id) > expected_length and re.match('[0-9]', flight_id[expected_length])):
        return flight_id[:expected_length] in executive_carriers
    return False


def check_mds_file(mds_extract, mds_db_sql, mds_db_host, mds_db_database, mds_refresh_hrs=8):
    if not os.path.exists(mds_extract):
        info_logger.warn('MDS EXTRACT does not exist - creating file')
        open(mds_extract, 'a').close()

    mds_extract_empty = (os.stat(mds_extract).st_size == 0)
    mds_extract_expired = (os.stat(mds_extract).st_mtime < time.time() - (int(mds_refresh_hrs) * 60 * 60))

    if (mds_extract_expired or mds_extract_empty):
        if mds_extract_empty:
            info_logger.warn('MDS EXTRACT is empty')
        if mds_extract_expired:
            info_logger.info('MDS EXTRACT has expired')

        mds_connect_error = 0
        info_logger.debug('Windows Authentication as: %s' % (getpass.getuser()))
        exception_msg = ''
        info_logger.debug('server=%s database=%s' % (mds_db_host, mds_db_database))

        try:
            db = pyodbc.connect(driver='{SQL Server}', server=mds_db_host, database=mds_db_database, autocommit='False')
        except Exception, e:
            mds_connect_error = 1
            exception_msg = str(e)

        if not mds_connect_error:
            info_logger.info('Connected')

            cur = db.cursor()
            mds_sql_error = 0

            info_logger.debug(mds_db_sql)

            try:
                cur.execute(mds_db_sql)
            except Exception, e:
                mds_sql_error = 1
                exception_msg = str(e)

            if not mds_sql_error:
                rows = cur.fetchall()
                mds_extract_file = open(mds_extract, 'wb')

                for row in rows:
                    try:
                        mds_extract_file.write(str(row[0]) + ',' + str(row[1]) + '\r\n')
                    except ValueError, err:
                        info_logger.exception('Error: %s, check MDS view and SQL statement (in config file)' % (err))

                mds_extract_file.close()
            else:
                info_logger.exception('Error: %s' % (exception_msg))
        else:
            info_logger.exception('Error: %s' % (exception_msg))
    else:
        info_logger.warn('Not updated - file was updated %s' % (str(datetime.datetime.strptime(time.ctime(os.stat(mds_extract).st_mtime), "%a %b %d %H:%M:%S %Y"))))


def read_mds_extract(mds_extract):
    """
    Reads the MDS extract and returns two lists of iata/icao executive carriers
    :param mds_extract:
    :returns: list, list
    """
    try:
        mds_extract_file = open(mds_extract, 'r')
        mds_extract_file_lines = mds_extract_file.readlines()
    except Exception, e:
        info_logger.exception(str(e))
        raise
    counter = 0

    iata_executive_carriers = []
    icao_executive_carriers = []

    for row in mds_extract_file_lines:
        try:
            airport_code_standard = row.split(',')[0].strip()
            carrier_code = row.split(',')[1].strip()

            if airport_code_standard == 'IATA':
                iata_executive_carriers.append(carrier_code)
            elif airport_code_standard == 'ICAO':
                icao_executive_carriers.append(carrier_code)

        except IndexError, err:
            info_logger.warn('Error: %s, check MDS extract file' % (err))
            raise
        counter += 1
    mds_extract_file.close()
    return iata_executive_carriers, icao_executive_carriers


# Config functions


def check_sequence_config_file(max_seqs_log, keys=['PARSED', 'STORED', 'RAW', 'FAILED']):
    """
    Creates a ConfigParser object from the given filename and creates a template file if it doesn't exist
    :param max_seqs_log:
    :param keys:
    :returns: ConfigParser object
    """
    seq_config = ConfigParser.ConfigParser()
    if not os.path.exists(max_seqs_log) or os.stat(max_seqs_log).st_size == 0:
        for f in keys:
            seq_config.add_section(f)
            seq_config.set(f, 'last_sequence', 'N/A')
            seq_config.set(f, 'last_updated', '19000101')
        with open(max_seqs_log, 'w') as configfile:
            seq_config.write(configfile)
    else:
        try:
            seq_config.read(max_seqs_log)
        except ConfigParser.MissingSectionHeaderError:
            raise
        except ConfigParser.ParsingError:
            raise

    return seq_config


def get_sequence_config_file_values(seq_info, seq_config, max_file_seq):
    """
    Updates the sequences dictionary with the values from the sequences config file, initialises values for the first run
    :param seq_info:
    :param seq_config:
    :param max_file_seq:
    :returns: dict
    """
    for f in seq_info:
        last_sequence = get_config_option(seq_config, f, 'last_sequence')
        last_updated = get_config_option(seq_config, f, 'last_updated')
        if last_sequence is not None:
            seq_info[f]['last_sequence'] = last_sequence
        if last_updated is not None:
            seq_info[f]['last_updated'] = datetime.datetime.strptime(last_updated, '%Y%m%d')
        seq_info[f]['expected_sequence'] = '0001' if seq_info[f]['last_sequence'] in ['N/A', '', None] else modulo_seq_add(seq_info[f]['last_sequence'], 1, max_file_seq)
        info_logger.info('%s: Expected: %s, Current: %s' % (f, seq_info[f]['expected_sequence'], seq_info[f]['last_sequence']))
    return seq_info


def get_config_option(config, section, option, default_value=None):
    """
    Returns the required config value from the provided ConfigParser object
    :param start_time:
    :param section:
    :param option:
    :param default_value:
    :returns: string
    """
    return_value = None
    try:
        return_value = config.get(section, option)
    except ConfigParser.NoSectionError:
        info_logger.exception('Error reading sequence config section: %s' % (section))
        raise
    except ConfigParser.NoOptionError:
        info_logger.exception('Error reading sequence config option: %s' % (option))
        raise
    except ValueError:
        info_logger.exception('Error reading sequence config values')
        raise
    return return_value if return_value else default_value


def check_sequences(source_dir_list, seq_info, max_file_seq=10000):
    """
    Checks the sequence numbers of the files in the source dir list against those stored in the dictionary
    :param source_dir_list:
    :param seq_info:
    :param max_file_seq:
    :returns: dict
    """
    if source_dir_list:
        for filename in source_dir_list:
            info_logger.info(filename)
            filetype = filename[:filename.index('_')]
            current_filedate = datetime.datetime.strptime(filename.split("_")[1], '%Y%m%d')
            current_received_seq = filename.split("_")[3].split(".")[0]

            if (current_filedate - seq_info[filetype]['last_updated']).days > 0:
                seq_info[filetype]['expected_sequence'] = '0001'
            seq_info[filetype]['last_sequence'] = current_received_seq

            if seq_info[filetype]['last_sequence'] == seq_info[filetype]['expected_sequence']:
                info_logger.debug('Expected %s, got %s' % (seq_info[filetype]['expected_sequence'], seq_info[filetype]['last_sequence']))
            else:
                seq_logger.info('Invalid sequence: %s, expected %s, got %s' % (filename, seq_info[filetype]['expected_sequence'], seq_info[filetype]['last_sequence']))
                info_logger.warn('Expected %s, got %s' % (seq_info[filetype]['expected_sequence'], seq_info[filetype]['last_sequence']))

            seq_info[filetype]['expected_sequence'] = modulo_seq_add(seq_info[filetype]['last_sequence'], 1, max_file_seq)
            seq_info[filetype]['last_updated'] = current_filedate
    else:
        info_logger.info('No source files')
    return seq_info


def update_config_file(seq_info, seq_config, max_seqs_log_temp, max_seqs_log, archive_file_dir):
    """
    Updates the config file values to a temporary file, backs up the original file to the archive folder, then writes the new sequences log file
    :param source_dir_list:
    :param seq_info:
    :param max_file_seq:
    :returns: dict
    """
    for f in seq_info:
        if seq_info[f]['last_sequence'] != 'N/A':
            seq_config.set(f, 'last_sequence', str((int(seq_info[f]['last_sequence']))).zfill(4))
            seq_config.set(f, 'last_updated', seq_info[f]['last_updated'].strftime("%Y%m%d"))

    with open(max_seqs_log_temp, 'w') as configfile:
        seq_config.write(configfile)

    shutil.move(max_seqs_log, os.path.join(archive_file_dir, os.path.basename(max_seqs_log)))
    info_logger.debug('Backing up seq file %s to %s' % (max_seqs_log, os.path.join(archive_file_dir, os.path.basename(max_seqs_log))))
    shutil.move(max_seqs_log_temp, max_seqs_log)
    info_logger.debug('Moving seq file %s to %s' % (max_seqs_log_temp, max_seqs_log))


# Date functions


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


def main(argv):
    seq_info = {'RAW': {'last_sequence': 'N/A', 'expected_sequence': 'N/A', 'last_updated': '19000101', 'regex': r'^RAW_[0-9]{8}_[0-9]{4}_[0-9]{4}.*\.zip$'},
                'PARSED': {'last_sequence': 'N/A', 'expected_sequence': 'N/A', 'last_updated': '19000101', 'regex': r'^PARSED_[0-9]{8}_[0-9]{4}_[0-9]{4}.*\.zip$'},
                'STORED': {'last_sequence': 'N/A', 'expected_sequence': 'N/A', 'last_updated': '19000101', 'regex': r'^STORED_[0-9]{8}_[0-9]{4}_[0-9]{4}.*\.zip$'},
                'FAILED': {'last_sequence': 'N/A', 'expected_sequence': 'N/A', 'last_updated': '19000101', 'regex': r'^FAILED_[0-9]{8}_[0-9]{4}_[0-9]{4}.*\.zip$'}}

    starttime = datetime.datetime.now()
    debug = False
    config_file = os.path.join(os.path.dirname(__file__), 'DQ_IL2_Config.ini')
    default_section = 'DEFAULT'
    custom_section = os.path.basename(__file__).replace('.py', '')
    parsed_ns = 'http://www.ibm.com/semaphore/commonAPI/'
    max_file_seq = 10000
    iata_executive_carriers = []
    icao_executive_carriers = []

    config = ConfigParser.ConfigParser()
    config.read(config_file)

    root_dir = config.get(default_section, 'ROOT_DIR')
    ftp_landing_zone = config.get(default_section, 'FTP_LANDING_ZONE')
    source_file_dir = config.get(custom_section, 'SOURCE_FILE_DIR')
    aws_file_dir = config.get(custom_section, 'AWS_FILE_DIR')
    ga_file_dir = config.get(custom_section, 'GA_FILE_DIR')
    mds_refresh_hrs = float(config.get(custom_section, 'MDS_REFRESH_HRS'))
    mds_db_host = config.get(default_section, 'MDS_DB_HOST')
    mds_db_database = config.get(custom_section, 'MDS_DB_DATABASE')
    mds_db_sql = config.get(custom_section, 'MDS_DB_SQL')
    max_batch_size = int(config.get(custom_section, 'MAX_BATCH_SIZE'))
    max_output_batch_size = int(config.get(custom_section, 'MAX_OUTPUT_BATCH_SIZE'))
    no_of_processes = int(config.get(custom_section, 'NO_OF_PROCESSES'))
    log_frequency = config.get(custom_section, 'log_frequency')
    log_interval = int(config.get(custom_section, 'log_interval'))
    log_backup_count = int(config.get(custom_section, 'log_backup_count'))
    debug = config.getboolean(custom_section, 'DEBUG')
    aws_data_feed = config.getboolean(custom_section, 'AWS_DATA_FEED')

    target_file_dir = os.path.join(root_dir, 'tmp/')
    archive_file_dir = os.path.join(root_dir, 'archive/')
    archive_parsed__file_dir = os.path.join(archive_file_dir, 'parsed/')
    archive_stored_file_dir = os.path.join(archive_file_dir, 'stored/')
    archive_failed_file_dir = os.path.join(archive_file_dir, 'failed/')
    output_file_dir = os.path.join(root_dir, 'out/')
    logfile_dir = os.path.join(root_dir, 'log/')
    raw_file_inprocess_dir = os.path.join(root_dir, 'raw_inprocess/')
    ga_inprocess_dir = os.path.join(root_dir, 'ga_inprocess/')
    mds_extract_dir = os.path.join(root_dir, 'mds/')
    max_seqs_log = os.path.join(os.path.dirname(__file__), 'MAX_SEQS.ini')
    max_seqs_log_temp = os.path.join(logfile_dir, '%s.tmp' % (max_seqs_log))
    mds_extract = os.path.join(mds_extract_dir, 'MDS_EXTRACT.csv')
    all_regex = '|'.join([seq_info[filetype]['regex'] for filetype in seq_info])
    seq_logfilename = os.path.join(logfile_dir, 'DQ_Invalid_Sequences.log')
    log_filename = os.path.join(logfile_dir, '%s.log' % (os.path.basename(__file__)))

    info_logger = setup_logger('Seq Check', log_filename, debug=debug, log_frequency=log_frequency, log_interval=log_interval, log_backup_count=log_backup_count)
    seq_logger = setup_logger('Sequences', seq_logfilename, debug=debug, log_frequency=log_frequency, log_interval=log_interval, log_backup_count=log_backup_count)

    info_logger.info('*** Run Start ***')

    info_logger.info('PREPARING BATCH')

    if prepare_batch_files(output_file_dir, max_output_batch_size, ftp_landing_zone, source_file_dir, all_regex, max_batch_size, seq_info.keys()) > 0:

        info_logger.info('READING MAX SEQUENCES FILE')
        seq_config = check_sequence_config_file(max_seqs_log, seq_info.keys())
        seq_info = get_sequence_config_file_values(seq_info, seq_config, max_file_seq)

        source_dir_list = [f for f in os.listdir(source_file_dir) if re.match(all_regex, f)]
        source_dir_list.sort()

        info_logger.info('CHECKING SEQUENCES')
        seq_info = check_sequences(source_dir_list, seq_info, max_file_seq)
        update_config_file(seq_info, seq_config, max_seqs_log_temp, max_seqs_log, archive_file_dir)

        pool = multiprocessing.Pool(no_of_processes)
        process_mp_unzip_files(pool, source_dir_list, source_file_dir, target_file_dir, seq_info['PARSED']['regex'], no_of_processes=no_of_processes)

        info_logger.info('READING MDS EXTRACT')
        check_mds_file(mds_extract, mds_db_sql, mds_db_host, mds_db_database, mds_refresh_hrs=mds_refresh_hrs)
        iata_executive_carriers, icao_executive_carriers = read_mds_extract(mds_extract)

        info_logger.info('PARSING XML')
        process_mp_parse_xml(pool, target_file_dir, root_dir, parsed_ns, iata_executive_carriers, icao_executive_carriers, ga_inprocess_dir, ga_file_dir, no_of_processes=no_of_processes)

        if aws_data_feed:
            info_logger.info("COPYING FILES FOR AWS DATA FEED")
            copy_files_for_aws([f for f in source_dir_list if re.match(all_regex, f)], source_file_dir, aws_file_dir)

        info_logger.info('MOVING RAW FILES')
        move_files([f for f in source_dir_list if re.match(seq_info['RAW']['regex'], f)], source_file_dir, raw_file_inprocess_dir)

        info_logger.info('ARCHIVING')
        move_files([f for f in source_dir_list if re.match(seq_info['PARSED']['regex'], f)], source_file_dir, archive_parsed_file_dir)
        move_files([f for f in source_dir_list if re.match(seq_info['FAILED']['regex'], f)], source_file_dir, archive_failed_file_dir)
        move_files([f for f in source_dir_list if re.match(seq_info['STORED']['regex'], f)], source_file_dir, archive_stored_file_dir)

        info_logger.info('CLEANING UP')
        remove_temp_folders(target_file_dir, '^RAW|^PARSED|^STORED|^FAILED')

    else:
        info_logger.info('No files to process')

    info_logger.info('*** Run Complete *** (Elapsed time: %s)' % (get_time_delta_in_secs(starttime)))


if __name__ == "__main__":
    freeze_support()
    main(sys.argv[1:])
