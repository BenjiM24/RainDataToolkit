import os
from pathlib import Path
import pandas as pd
import CommonFunctions
import CommonSQLFunctions
import shutil


masterImagesFolder = r'Z:\Rain Data\Supplier Work\Autoelectro\Master Images'
masterBulletinsFolder = r'Z:\Rain Data\Supplier Work\Autoelectro\Master Bulletins'

def createRequiredSubfolders(saveLocation):
    Path(saveLocation + r'\Input').mkdir(parents=True, exist_ok=True)

    Path(saveLocation + r'\Output').mkdir(parents=True, exist_ok=True)

    Path(saveLocation + r'\Work').mkdir(parents=True, exist_ok=True)

def loadAutoelectroData(jobFolder):
    CommonSQLFunctions.loadDataIntoSQL(folderLocation=jobFolder + r'\Input\MAMSubmission',
                    fileName='AOA',
                    database='PaperCatalogueImport',
                    schema='Autoelectro',
                    tablename='AOA',
                    truncateExisting=True)

    CommonSQLFunctions.loadDataIntoSQL(folderLocation=jobFolder + r'\Input\MAMSubmission',
                    fileName='AOS',
                    database='PaperCatalogueImport',
                    schema='Autoelectro',
                    tablename='AOS',
                    truncateExisting=True)

    CommonSQLFunctions.loadDataIntoSQL(folderLocation=jobFolder + r'\Input\MAMSubmission',
                    fileName='XRef',
                    database='PaperCatalogueImport',
                    schema='Autoelectro',
                    tablename='CrossRef',
                    truncateExisting=True,
                    fileType='csv')

    CommonSQLFunctions.loadDataIntoSQL(folderLocation=jobFolder + r'\Input\MAMSubmission',
                    fileName='AOA',
                    database='PaperCatalogueImport',
                    schema='Autoelectro',
                    tablename='Images',
                    truncateExisting=True,
                    fileType='csv')

    CommonSQLFunctions.loadDataIntoSQL(folderLocation=jobFolder + r'\Input\MAMSubmission',
                    fileName='AOS',
                    database='PaperCatalogueImport',
                    schema='Autoelectro',
                    tablename='Images',
                    truncateExisting=False,
                    fileType='csv')

    #get list of all image filenames
    filenames = CommonFunctions.getFilenamesWithinFolder(masterImagesFolder)
    CommonSQLFunctions.insertListIntoDB(filenames, 'PaperCatalogueImport', 'ImageFiles', 'Autoelectro')
    filenames = CommonFunctions.getFilenamesWithinFolder(masterBulletinsFolder)
    CommonSQLFunctions.insertListIntoDB(filenames, 'PaperCatalogueImport', 'BulletinFiles', 'Autoelectro')

    CommonSQLFunctions.executeSQL('EXEC PaperCatalogueImport.Autoelectro.LoadIntoPaperCatalogueForLoadToCatMan;')
    CommonSQLFunctions.executeSQL('EXEC CatalogueManager.dbo.PaperCatalogueKTypeImport @SupplierId = 4842;')

def mergeNewImagesIntoMaster(jobFolder):
    imagesFolder = jobFolder + '\\input\\MAM'

    if (os.path.exists(imagesFolder)):
        print('MAM folder found in input. Merging images into Master Images folder.')
        for filename in os.listdir(imagesFolder):
            if filename.endswith('.jpg') or filename.endswith('.bmp'):
                shutil.copy(os.path.join(imagesFolder, filename), os.path.join(masterImagesFolder, filename))

def mergeNewDocumentsIntoMaster(jobFolder):
    bulletinsFolder = jobFolder + '\\Input\\packinginfo\\packinginfo'

    if (os.path.exists(bulletinsFolder)):
        print('Packing info folder found in input. Merging documents into Master Images folder.')
        for filename in os.listdir(bulletinsFolder):
            if filename.endswith('.pdf'):
                shutil.copy(os.path.join(bulletinsFolder, filename), os.path.join(masterBulletinsFolder, filename))


def runAutoelectroReports(jobFolder):
    df = CommonSQLFunctions.getMandatoryCriteriaReport(4842)
    saveLocation = jobFolder + '\\work'
    CommonFunctions.saveDataInExcel(saveLocation, 'mandatory criteria', df)

def pushImageChangesIntoMaster(jobFolder):
    imagesFolder = CommonFunctions.getFilePathInput('Please enter the Master Images folder for this supplier:')

    newImagesFolder = jobFolder + '\\input\\'

def start():
    jobFolder = CommonFunctions.getFilePathInput('Please enter the job folder:')

    createRequiredSubfolders(jobFolder)

    while 1 == 1:
        if CommonFunctions.get_bool(f'Is the latest data is saved here: {jobFolder}\\input? True/False') == True:
            break

    while 1 == 1:
        options = {
                   0: 'Merge new images & documents into master',
                   1: 'Load input data into SQL',
                   2: 'Run CatalogueManager cleansing modules',
                   3: 'Push data into DAT template tables',
                   4: 'Create DAT files',
                   5: 'Generate TecDoc quality reports',
                   6: 'Export DAT Files',
                   '*': 'Run all of the above'
                   }

        print('Please select from the following options:')
        for x in options:
            print(str(x) + ': ' + options[x])

        chosenOption = input()

        if chosenOption == '0' or chosenOption.lower() == '*':
            mergeNewImagesIntoMaster(jobFolder)
            mergeNewDocumentsIntoMaster(jobFolder)

        if chosenOption == '1' or chosenOption.lower() == '*':
            loadAutoelectroData(jobFolder)

        if chosenOption == '2' or chosenOption.lower() == '*':
            CommonSQLFunctions.runModularCleanse(4842)

        if chosenOption == '3' or chosenOption.lower() == '*':
            CommonSQLFunctions.loadCatalogueManagerIntoTecDocOutputTables(4842)

        if chosenOption == '4' or chosenOption.lower() == '*':
            print('todo')

        if chosenOption == '5' or chosenOption.lower() == '*':
            runAutoelectroReports(jobFolder)

        if chosenOption == '6' or chosenOption.lower() == '*':
            import Applications.DATFileExport as datExport
            datExport.start()


start()
