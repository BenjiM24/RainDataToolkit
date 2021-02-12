import os
from pathlib import Path
import datetime
import pandas as pd
import zipfile
import CommonFunctions as cf
import CommonSQLFunctions as csf
import csv
import zlib
from tqdm import tqdm

def createRequiredSubfolders(zipRequired, crossRefsrequired, saveLocation, supplierName, auditId):

    parentFolder = supplierName + ' - ' + str(auditId) + ' - ' + cf.getTimeStamp()

    saveLocation = saveLocation + '\\' + parentFolder

    Path(saveLocation).mkdir(parents=True, exist_ok=True)
    Path(saveLocation + r'\RainData').mkdir(parents=True, exist_ok=True)

    if crossRefsrequired:
        Path(saveLocation + r'\CROSSREF').mkdir(parents=True, exist_ok=True)

    if zipRequired:
        Path(saveLocation + r'\DATA').mkdir(parents=True, exist_ok=True)

    return saveLocation

def saveSections(auditId, sectionCodes, saveLocation, zipRequired, excelFilesRequired):
    sections = []
    print('Attempting to save sections')
    pbar = tqdm(sectionCodes, position=0, leave=True)
    for section in tqdm(pbar):
        try:
            pbar.set_description(f'Retrieving {section} from database.')
            query = f"EXEC dbo.GetSectionByAuditId @AuditId = {auditId}, @Sec = '{section}'"
            sectionData = csf.executeSQLWithResults(query)

            # check results
            if excelFilesRequired:
                cf.saveDataInExcel(saveLocation+'\\RainData\\', section, sectionData)
            if zipRequired:
                sectionData.columns = ['ID',	'Manuf',	'Model',	'SubModel',	'ModelDesc',	'Years',
                                       'StartYr',	'EndYr',	'EngineSize',	'FuelType',	'ProductCode',	'Subpg',
                                       'PartNumber',	'Repair',	'ExactCC',	'Cylinders',	'Cam',	'Valve',
                                       'Bodytype',	'TransDrive',	'Mpos',	'Xinfo1',	'Xinfo2',	'Xinfo3',
                                       'Xinfo4',	'Xinfo5',	'Xinfo6',	'Xinfo7',	'Xinfo8',	'Xinfo9',
                                       'Xinfo10',	'Xinfo11',	'Xinfo12',	'Xinfo13',	'Xinfo14',	'Xinfo15',
                                       'Xinfo16',	'Xinfo17',	'Xinfo18',	'Xinfo19',	'Xinfo20',	'ModInd',
                                       'Sec',	'OLR',	'BrandSupplier',	'ProductDesc',	'V8Key']

                sectionData.to_csv(saveLocation + '\\RainData\\' + section + '.txt', sep="\t", index=False,
                                   quoting=csv.QUOTE_NONE)

                headers = csf.executeSQLWithResults(f'EXEC dbo.GetSectionHeadersByAuditId @AuditId = {auditId},'
                                                    f' @Sec = {section}')

                headers.to_csv(saveLocation + '\\RainData\\' + section + '-Headers.txt', sep='\t', index=False,
                               quoting=csv.QUOTE_NONE)

                os.chdir(saveLocation + '\\RainData\\')
                zf = zipfile.ZipFile(saveLocation + '\\DATA\\' + section + '.zip', mode="w",
                                     compression=zipfile.ZIP_DEFLATED)
                zf.write(section + '.txt')
                zf.write(section + '-Headers.txt')
                zf.close()

                os.remove(section + '.txt')
                os.remove(section + '-Headers.txt')
        except Exception as error:
            print(f'Something went wrong with section {section}.')
            print(repr(error))

def start():
    auditId = cf.inputRequest('Enter the audit ID:', 'int')
    saveLocation = cf.inputRequest('Enter the save location:', 'filepath')
    sectionCodes = cf.inputRequest('Enter section codes if required (leave blank to extract all):')
    zipRequired = cf.inputRequest('Do you require zip files? True/False', 'bool')
    excelFilesRequired = cf.inputRequest('Do you require excel files? True/False', 'bool')
    crossRefsRequired = cf.inputRequest('Do you require cross refs? True/False', 'bool')

    if sectionCodes is None or sectionCodes == '' or sectionCodes[0] == '':
        sectionCodes = csf.getListOfSectionsByAuditId(auditId)
    if sectionCodes is None or sectionCodes == '' or sectionCodes[0] == '':
        raise ValueError('No sections found. Has this audit been archived?')

    supplierName = csf.getSupplierNameFromAudit(auditId)

    saveLocation = createRequiredSubfolders(zipRequired, crossRefsRequired, saveLocation, supplierName, auditId)

    saveSections(auditId, sectionCodes, saveLocation, zipRequired,excelFilesRequired)

start()