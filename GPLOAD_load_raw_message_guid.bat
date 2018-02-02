@echo off

set PATH_TO_GP_LOAD_EXE=C:\Program Files (x86)\Greenplum\greenplum-loaders-4.3.4.0-build-1\bin\gpload.py
set GP_HOST=XXX
set GP_DB=DQ_dev_1
set PATH_TO_SCRIPTS=E:\dq\nrt\s4_file_ingest\scripts
set PATH_TO_LOG=E:\dq\nrt\s4_file_ingest\log

cd %PATH_TO_SCRIPTS%


rem *************************************************************
rem *** LOAD THE DATABASE TABLES
rem *************************************************************
"%PATH_TO_GP_LOAD_EXE%" -f "%PATH_TO_SCRIPTS%"\gpload_load_raw_message_guid.yml -l "%PATH_TO_LOG%"\gpload_raw_msg_guid_ext_ctrl_doc.log -h %GP_HOST% -d %GP_DB% > "%PATH_TO_LOG%"\gpload_raw_msg_guid_ext_ctrl_doc_last_run.log
if %errorlevel% neq 0 exit /b %errorlevel%