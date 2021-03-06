import os
from pathlib import Path
import datetime
import pandas as pd
import zipfile
import CommonFunctions as cf
import CommonSQLFunctions as csf
import csv
import zlib
import shutil
from tqdm import tqdm

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

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

def saveSections(auditId, sectionCodes, saveLocation, zipRequired, excelFilesRequired, csvPreviewFilesRequired):
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
                cf.saveDataInExcel(ROOT_DIR + '\\temp', section, sectionData)
                shutil.move(ROOT_DIR + '\\temp\\' + section + '.xlsx',
                            saveLocation + '\\RainData\\' + section + '.xlsx')

            if csvPreviewFilesRequired:
                sectionData.to_csv(ROOT_DIR + '\\temp\\' + section + '.csv', index=False)
                shutil.move(ROOT_DIR + '\\temp\\' + section + '.csv', saveLocation + '\\RainData\\' + section + '.csv')

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

def processReports(saveLocation, auditId, sectionCodes):
    reportSaveLocation = saveLocation + r'\RainData\Reports'
    Path(reportSaveLocation).mkdir(parents=True, exist_ok=True)

    previousAuditId = 0

    options = {
        1: 'Audit Comparison',
        2: 'Unused Headers',
        '*': 'Run all of the above'
    }

    print('Please select from the following options:')
    for x in options:
        print(str(x) + ': ' + options[x])
    chosenOption = input()

    if chosenOption == '1' or chosenOption == '*':
        if previousAuditId == 0:
            previousAuditId = cf.inputRequest('Enter previous audit id:', 'int')

        cf.saveDataInExcel(reportSaveLocation, f'AuditComparison {auditId} vs {previousAuditId}',
                           csf.executeSQLWithResults(f'EXEC Report.AuditComparison @NewAuditId = {auditId}, '
                                                     f'@OldAuditId = {previousAuditId}, '
                                                     f'@IncludeImageReference = NULL'))

    if chosenOption == '2' or chosenOption == '*':
        for section in tqdm(sectionCodes):
            df = csf.executeSQLWithResults(f"EXEC Report.UnusedCriteriaAndHeaders @AuditId = {auditId}, "
                                           f"@Sec = '{section}'")

            if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                cf.saveDataInExcel(reportSaveLocation, f'{section} Unused Headers - {auditId}', df)
            else:
                print(f'No unused headers found for: {section}. Maybe this is a trade number or mirror section...?')

def start():

    options = {
        1: 'Extract Audit Data',
        2: 'Generate Audit Reports',
        3: 'Fast Preview Mode',
        '*': 'Run all of the above'
    }

    print('Please select from the following options:')
    for x in options:
        print(str(x) + ': ' + options[x])

    chosenOption = input()

    auditId = cf.inputRequest('Enter the audit ID:', 'int')
    saveLocation = cf.inputRequest('Enter the save location:', 'filepath')

    if chosenOption == '3':
        sectionCodes = csf.getListOfSectionsByAuditId(auditId)
    else:
        sectionCodes = cf.inputRequest('Enter section codes if required (leave blank to extract all):')

    if sectionCodes is None or sectionCodes == '' or sectionCodes[0] == '':
        sectionCodes = csf.getListOfSectionsByAuditId(auditId)

    supplierName = csf.getSupplierNameFromAudit(auditId)

    if chosenOption == '1' or chosenOption == '*':
        zipRequired = cf.inputRequest('Do you require zip files? True/False', 'bool')
        excelFilesRequired = cf.inputRequest('Do you require excel files? True/False', 'bool')
        csvPreviewFilesRequired = False
        crossRefsRequired = cf.inputRequest('Do you require cross refs? True/False', 'bool')

    if chosenOption == '3':
        zipRequired = False
        excelFilesRequired = False
        csvPreviewFilesRequired = True
        crossRefsRequired = False

    if chosenOption not in ['*', '1', '3']:
        print('Invalid option selected.')
        return

    saveLocation = createRequiredSubfolders(zipRequired, crossRefsRequired, saveLocation, supplierName, auditId)

    if chosenOption == '1' or chosenOption == '3' or chosenOption == '*':
        if sectionCodes is None or sectionCodes == '' or sectionCodes[0] == '':
            raise ValueError('No sections found. Has this audit been archived?')

        saveSections(auditId, sectionCodes, saveLocation, zipRequired, excelFilesRequired, csvPreviewFilesRequired)

    if chosenOption == '2' or chosenOption == '*':
        processReports(saveLocation, auditId, sectionCodes)


start()