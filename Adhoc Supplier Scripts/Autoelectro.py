import os
from pathlib import Path
import pandas as pd
import CommonFunctions
import CommonSQLFunctions
import shutil
from tqdm import tqdm

masterImagesFolder = r'Z:\Rain Data\Supplier Work\Autoelectro\Master Images'
masterBulletinsFolder = r'Z:\Rain Data\Supplier Work\Autoelectro\Master Bulletins'
paperCatalogueDB = 'PaperCatalogueImport'
paperCatalogueSchema = 'Autoelectro'

def createRequiredSubfolders(saveLocation):
    Path(saveLocation + r'\Input').mkdir(parents=True, exist_ok=True)

    Path(saveLocation + r'\Output').mkdir(parents=True, exist_ok=True)

    Path(saveLocation + r'\Work').mkdir(parents=True, exist_ok=True)

    Path(saveLocation + r'\Work\Reports').mkdir(parents=True, exist_ok=True)

def loadAutoelectroData(jobFolder):
    CommonSQLFunctions.loadFlatFileIntoDB(folderFilepath=jobFolder + r'\Input\MAMSubmission', fileName='AOA',
                                          fileType='txt', databaseName=paperCatalogueDB, tableName='AOA',
                                          schemaName=paperCatalogueSchema, appendaction='replace')

    CommonSQLFunctions.loadFlatFileIntoDB(folderFilepath=jobFolder + r'\Input\MAMSubmission', fileName='AOS',
                                          fileType='txt', databaseName=paperCatalogueDB, tableName='AOS',
                                          schemaName=paperCatalogueSchema, appendaction='replace')

    CommonSQLFunctions.loadFlatFileIntoDB(folderFilepath=jobFolder + r'\Input\MAMSubmission', fileName='XRef',
                                          fileType='csv', databaseName=paperCatalogueDB, tableName='CrossRef',
                                          schemaName=paperCatalogueSchema, appendaction='replace')

    CommonSQLFunctions.loadFlatFileIntoDB(folderFilepath=jobFolder + r'\Input\MAMSubmission', fileName='AOA',
                                          fileType='csv', databaseName=paperCatalogueDB, tableName='Images',
                                          schemaName=paperCatalogueSchema, appendaction='replace', encoding='utf_16_le')

    CommonSQLFunctions.loadFlatFileIntoDB(folderFilepath=jobFolder + r'\Input\MAMSubmission', fileName='AOS',
                                          fileType='csv', databaseName=paperCatalogueDB, tableName='Images',
                                          schemaName=paperCatalogueSchema, appendaction='append', encoding='utf_16_le')


    #get list of all image filenames
    masterImages = CommonFunctions.getFilePathInput('Enter the folder path for master images:')
    filenames = CommonFunctions.getFilenamesWithinFolder(masterImages)
    CommonSQLFunctions.insertListIntoDB(filenames, 'PaperCatalogueImport', 'ImageFiles', 'Autoelectro')

    masterBulletins = CommonFunctions.getFilePathInput('Enter the folder path for master bulletins:')
    filenames = CommonFunctions.getFilenamesWithinFolder(masterBulletins)
    CommonSQLFunctions.insertListIntoDB(filenames, 'PaperCatalogueImport', 'BulletinFiles', 'Autoelectro')

def runDataThrough(jobfolder):

    CommonSQLFunctions.executeSQL('EXEC PaperCatalogueImport.Autoelectro.LoadIntoPaperCatalogueForLoadToCatMan;')
    CommonSQLFunctions.executeSQL('EXEC CatalogueManager.dbo.PaperCatalogueKTypeImport @SupplierId = 4842;')

def mergeNewImagesIntoMaster(jobFolder):
    imagesFolder = CommonFunctions.getFilePathInput('Enter the folder path for input images:')

    if (os.path.exists(imagesFolder)):
        print('MAM folder found in input. Merging images into Master Images folder.')
        for filename in tqdm(os.listdir(imagesFolder)):
            if filename.endswith('.jpg') or filename.endswith('.bmp'):
                src = os.path.join(imagesFolder, filename)
                dest = os.path.join(masterImagesFolder, filename)

                if os.path.isfile(dest):
                    if os.stat(src).st_mtime > os.stat(dest).st_mtime > 1:
                        shutil.copy(src, dest)
    else:
        print(f'{CommonFunctions.bcolors.WARNING}Images folder not found{CommonFunctions.bcolors.ENDC}')

def mergeNewDocumentsIntoMaster(jobFolder):
    bulletinsFolder = CommonFunctions.getFilePathInput('Enter the folder path for input bulletins:')

    if (os.path.exists(bulletinsFolder)):
        print('Packing info folder found in input. Merging documents into Master bulletins folder.')
        for filename in tqdm(os.listdir(bulletinsFolder)):
            if filename.endswith('.pdf'):
                shutil.copy(os.path.join(bulletinsFolder, filename), os.path.join(masterBulletinsFolder, filename))


def runAutoelectroReports(jobFolder):
    saveLocation = jobFolder + '\\Work\\Reports'

    df = CommonSQLFunctions.getMandatoryCriteriaReport(4842)
    CommonFunctions.saveDataInExcel(saveLocation, f'Missing Mandatory Criteria (Error 596) '
                                                  f'{CommonFunctions.getTimeStamp()}', df)

    df = CommonSQLFunctions.getLinksWithNoDifferentiatingCriteria(4842)
    CommonFunctions.saveDataInExcel(saveLocation, f'Links with No Differentiating Criteria (Warning 145) '
                                                  f'{CommonFunctions.getTimeStamp()}', df)

    input(f'{CommonFunctions.bcolors.WARNING}Reports saved in work folder. Check files and press enter to continue...'
          f'{CommonFunctions.bcolors.ENDC}')

def outputReports(jobFolder):
     df = CommonSQLFunctions.executeSQLWithResults('EXEC PaperCatalogueImport.Autoelectro.GapReport')
     CommonFunctions.saveDataInExcel(jobFolder + '\\Output\\', f'AE - No Target KType Found - '
                                                               f'{CommonFunctions.getTimeStamp()}', df)

def start():
    jobFolder = CommonFunctions.getFilePathInput('Please enter the job folder:')

    createRequiredSubfolders(jobFolder)

    while 1 == 1:
        if CommonFunctions.get_bool(f'Is the latest data is saved here: {jobFolder}\\input? True/False') == True:
            break

    while 1 == 1:
        options = {
                   1: 'Merge new images & documents into master',
                   2: 'Load input data into SQL',
                   3: 'Run data through SQL',
                   4: 'Run CatalogueManager cleansing modules',
                   5: 'Push data into DAT template tables',
                   6: 'Generate TecDoc quality reports',
                   7: 'Generate Output Reports for Customer',
                   8: 'Export DAT Files',
                   '*': 'Run all of the above'
                   }

        print('Please select from the following options:')
        for x in options:
            print(str(x) + ': ' + options[x])

        chosenOption = input()

        if chosenOption == '1' or chosenOption.lower() == '*':
            mergeNewImagesIntoMaster(jobFolder)
            mergeNewDocumentsIntoMaster(jobFolder)

        if chosenOption == '2' or chosenOption.lower() == '*':
            loadAutoelectroData(jobFolder)

        if chosenOption == '3' or chosenOption.lower() == '*':
            runDataThrough(jobFolder)

        if chosenOption == '4' or chosenOption.lower() == '*':
            CommonSQLFunctions.runModularCleanse(4842)

        if chosenOption == '5' or chosenOption.lower() == '*':
            CommonSQLFunctions.loadCatalogueManagerIntoTecDocOutputTables(4842)

        if chosenOption == '6' or chosenOption.lower() == '*':
            runAutoelectroReports(jobFolder)

        if chosenOption == '7' or chosenOption.lower() == '*':
            outputReports(jobFolder)

        if chosenOption == '8' or chosenOption.lower() == '*':
            import Applications.DATFileExport as datExport
            datExport.start(saveLocation=jobFolder + '\\Output')

start()
