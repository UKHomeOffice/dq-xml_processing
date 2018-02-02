@echo off

set PATH_TO_GP_LOAD_EXE=C:\Program Files (x86)\Greenplum\greenplum-loaders-4.3.4.0-build-1\bin\gpload.py
set GP_HOST=
set GP_DB=DQ_db
set PATH_TO_SCRIPTS=E:\dq\nrt\s4_file_ingest\scripts
set PATH_TO_LOG=E:\dq\nrt\s4_file_ingest\log

cd %PATH_TO_SCRIPTS%


rem *************************************************************
rem *** LOAD THE DATABASE TABLES
rem *************************************************************

"%PATH_TO_GP_LOAD_EXE%" -f "%PATH_TO_SCRIPTS%"\gpload_load_voyage_external.yml -l "%PATH_TO_LOG%"\gpload_voyage_ext_control_document.log -h %GP_HOST% -d %GP_DB%
if %errorlevel% neq 0 exit /b %errorlevel%