import configparser
import urllib
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

from tqdm import tqdm

import CommonFunctions as cf

root_dir = os.path.dirname(os.path.abspath(__file__))

Config = configparser.ConfigParser()
Config.read(os.path.join(root_dir,'config.ini'))

driver = Config.get('CoreContext', 'driver')
host = Config.get('CoreContext', 'host')
user = Config.get('CoreContext', 'user')
password = Config.get('CoreContext', 'password')
database = Config.get('CoreContext', 'database')

conn = pyodbc.connect('driver=%s;server=%s;database=%s;uid=%s;pwd=%s' % (driver, host, database, user, password))
def getConn(db=database):
    return pyodbc.connect('driver=%s;server=%s;database=%s;uid=%s;pwd=%s' % (driver, host, db, user, password))

#quoted = urllib.parse.quote_plus(conn)
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn))


def executeSQL(queryString, db=database):
    cursor = getConn(db=db).cursor()
    cursor.execute(queryString)
    cursor.commit()

def executeSQLWithResults(query):
    crsr = conn.cursor()
    query = 'SET NOCOUNT ON ' + query
    crsr.execute(query)
    result = crsr.fetchall()
    dataList = []
    data = None

    col_names = [x[0] for x in crsr.description]
    data = [tuple(x) for x in result]  # convert pyodbc.Row objects to tuples
    data = pd.DataFrame(data, columns=col_names)

    dataList.append(data)

    while (crsr.nextset()):
        rows = crsr.fetchall()
        columns = [column[0] for column in crsr.description]
        dataList.append(pd.DataFrame.from_records(rows, columns=columns))

    if len(dataList) > 1:
        data = dataList

    crsr.close()
    return data


def loadDataIntoSQL(folderLocation, fileName, database, schema, tablename, truncateExisting = False, fileType = 'txt'):
    # Import CSV
    inputFile = folderLocation + '\\' + fileName + '.' + fileType

    separators = {
        'csv': ',',
        'txt': '\t'
    }

    cols = pd.read_csv(inputFile, sep=separators[fileType], nrows=0).columns.tolist()
    colsAsString = ','.join(['[' + str(elem) + ']' for elem in cols])
    valuesAsString = ','.join(['?' for elem in cols])

    sqlTableName = "[" + database + "].[" + schema + "].[" + tablename + "]"

    if truncateExisting:
        truncateTable(sqlTableName)

    with open(inputFile) as infile:
        lineNumber = 0
        for line in infile:
            if lineNumber == 0:
                lineNumber += 1
                continue
            cursor = conn.cursor()
            data = line.split('\t')
            query = ("insert into " + sqlTableName + " (" + colsAsString + ") " +
                     "values (" + valuesAsString + ")")
            cursor.execute(query, *data)
            cursor.commit()

def runModularCleanse(supplierId):
    executeSQL(f'EXEC CatalogueManager.ModularDataCleanser.InitiateModularCleanse @SupplierId = {supplierId},'
               '@TargetEntityCategorySourceId = 1;')

def loadCatalogueManagerIntoTecDocOutputTables(supplierId):
    executeSQL(f'EXEC CatalogueManagerOutput.TecDoc.PopulateTecDocTablesFromCatMan @SupplierId = {supplierId}, '
               f'@SupplierIdForTecDoc = {supplierId};')

def getMandatoryCriteriaReport(supplierId):
    query = f"EXEC CatalogueManagerOutput.TecDoc.MandatoryCriteriaReport @SupplierId = {supplierId};"
    return executeSQLWithResults(query)

def truncateTable(sqlTable):
    cursor = conn.cursor()
    cursor.execute('Truncate Table ' + sqlTable)
    cursor.commit()

##################V8 functions
def getListOfSectionsByAuditId(auditId):
    crsr = conn.cursor()
    query = f"EXEC dbo.GetSeccodefromaudit @AuditId = {auditId}"
    crsr.execute(query)
    result = crsr.fetchall()
    result = [row[0] for row in result]
    return result

def getSupplierNameFromAudit(auditId):
    crsr = conn.cursor()
    query = f"EXEC dbo.GetSupplierfromAuditId @AuditId = {auditId}"
    crsr.execute(query)
    result = crsr.fetchall()
    result = result[0][0]
    return result

def insertListIntoDB(list, databaseName, tableName, schemaName = 'dbo'):
    #have to reinitiate the connection string to allow for dynamic database name (for df.to_sql)
    engine = create_engine(f'mssql+pyodbc://{user}:{password}@{host}/{databaseName}?driver=SQL+Server')


    df = pd.DataFrame({'col': list})
    df.to_sql(tableName, schema=schemaName, con=engine, if_exists='replace')


def insertDFIntoDB(df, databaseName, tableName, schemaName='dbo', appendaction='append'):
    # have to reinitiate the connection string to allow for dynamic database name (for df.to_sql)
    engine = create_engine(f'mssql+pyodbc://{user}:{password}@{host}/{databaseName}?driver=SQL+Server')

    insert_with_progress(df, engine=engine, tableName=tableName, schemaName=schemaName, appendAction=appendaction)

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def insert_with_progress(df, engine, tableName, schemaName, appendAction):
    chunksize = int(len(df) / 20)
    with tqdm(total=len(df)) as pbar:
        for i, cdf in enumerate(chunker(df, chunksize)):
            if appendAction == 'replace' and i > 0:
                appendAction = 'append'
            cdf.to_sql(tableName, schema=schemaName, con=engine, if_exists=appendAction, index=False)
            pbar.update(chunksize)
            tqdm._instances.clear()

def restoreDatabase(databaseName, bakFile, logFilename, dataFilename):
    #does not work for some reason
    if cf.get_bool(f'{cf.bcolors.WARNING}WARNING: You are about to restore a database over another. '
                   f'Is your job worth it?{cf.bcolors.ENDC}'):
        sql = f"USE [master]; " \
              f"\nIF EXISTS (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{databaseName}')" \
              f"\nBEGIN ALTER DATABASE {databaseName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; END" \
              f"\n           RESTORE DATABASE {databaseName} FROM  DISK =   N'{bakFile}' " \
              f"\n WITH MOVE '{dataFilename}' TO 'E:\\Data\\{dataFilename}.mdf'," \
              f"\n MOVE '{logFilename}' TO 'E:\\Logs\\{logFilename}.ldf' " \
              f"\nALTER DATABASE {databaseName} SET MULTI_USER;"

        return sql
        #executeSQL(sql)

def loadFlatFileIntoDB(folderFilepath, fileName, fileType, databaseName, tableName, schemaName, appendaction,
                        encoding='utf8'):

    df = cf.loadFlatFileIntoDF(folderFilepath=folderFilepath, fileName=fileName,
                                        fileType=fileType, encoding=encoding)
    insertDFIntoDB(df, databaseName=databaseName, tableName=tableName, schemaName=schemaName,
                                      appendaction=appendaction)

