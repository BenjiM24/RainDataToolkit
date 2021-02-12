import glob
import os
import pyodbc
import CommonFunctions as cf
import CommonSQLFunctions as csf

#restore backup into SQL Server

#run script which updates carparc figures

list_of_files = glob.glob(r'Y:\SQL Backups\Weekly_VRM\*.bak') # * means all if need specific format then *.csv
latest_file = max(list_of_files, key=os.path.getctime)

latest_file = 'Z' + latest_file[1:]
processFile = cf.get_bool(f'Latest file is: {cf.bcolors.OKGREEN}{latest_file}.{cf.bcolors.ENDC}'
                          f'\nDo you wish to proceed?')
query = csf.restoreDatabase('vrm', latest_file, 'VRM_Log', 'VRM_Data')

if cf.get_bool('Please run the following SQL statement on 192.168.0.32:'
            f'\n{cf.bcolors.OKBLUE}{query}'
               f'\nUSE vrm'
               f'\nCREATE USER BatchManagerUser FOR LOGIN BatchManagerUser'
               f"\nEXEC sys.sp_addrolemember @rolename = 'db_owner', @membername = 'BatchManagerUser'"
               f'{cf.bcolors.ENDC}'
            f'\nEnter true to continue once the backup is complete.'):
    csf.executeSQL('EXEC MAMData.dbo.UpdateCountsFromVRM')


