import os
import ftplib
from pathlib import Path
import CommonFunctions as cf
import CommonSQLFunctions as csf
from tqdm import tqdm

sealeyFolder = r'Z:\Rain Data\Supplier Work\Sealey'

def createFolders(jobFolder, inputFolder, imagesFolder, workFolder):
    Path(jobFolder).mkdir(parents=True, exist_ok=True)
    Path(inputFolder).mkdir(parents=True, exist_ok=True)
    Path(imagesFolder).mkdir(parents=True, exist_ok=True)
    Path(workFolder).mkdir(parents=True, exist_ok=True)

def getFTP():
    ftp = ftplib.FTP("sealey.iweb-storage.com")
    ftp.login("sealey-spip", "vYnnS3*m")

    return ftp

#download data
def downloadData(inputFolder):
    ftp = getFTP()

    path = '/Protected/MasterData/Data'
    ftp.cwd(path)
    filenames = ftp.nlst()

    # save in folder
    for filename in tqdm(filenames):
        local_filename = os.path.join(inputFolder + '\\', filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR '+ filename, file.write)

        file.close()

    ftp.quit()

def downloadImages(imagesFolder):
    ftp = getFTP()

    path = '/Protected/MasterData/Images/WEBMain'
    ftp.cwd(path)
    filenames = ftp.nlst()

    # save in folder
    for filename in tqdm(filenames):
        local_filename = os.path.join(imagesFolder + '\\', filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR '+ filename, file.write)

        file.close()

    ftp.quit()

def processNewProducts():
    csf.executeSQL('EXEC PaperCatalogueImport.Sealey.AddNewProductsToEntityMatch')
    csf.executeSQL('EXEC PaperCatalogueImport.Sealey.LoadIntoPaperCatalogue')
    csf.executeSQL('EXEC CatalogueManager.dbo.PaperCatalogueKTypeImport @SupplierId = -22')

def generateConsumablesAudit():
    csf.executeSQL('EXEC SectionDataImport.Consumables.ConvertConsumablesData @SupplierId = -22')

def loadIntoDB(inputFolder):
    csf.loadFlatFileIntoDB(folderFilepath=inputFolder, fileName='Datacut_1_Export_models_select_files_xls',
                           fileType='csv', databaseName='PaperCatalogueImport',
                           tableName='Datacut_1_Export_models_select_files_xls',
                           schemaName='Sealey', appendaction='replace')

    csf.loadFlatFileIntoDB(folderFilepath=inputFolder, fileName='Datacut_2_Export_models_Tspecs',
                           fileType='csv', databaseName='PaperCatalogueImport',
                           tableName='Datacut_2_Export_models_Tspecs',
                           schemaName='Sealey', appendaction='replace')

    csf.loadFlatFileIntoDB(folderFilepath=inputFolder, fileName='Datacut_4_Export_Models_Images_videos',
                           fileType='csv', databaseName='PaperCatalogueImport',
                           tableName='Datacut_4_Export_Models_Images_videos',
                           schemaName='Sealey', appendaction='replace')

def start():
    if(cf.get_bool('Does the job folder already exist?')):
        jobFolder = cf.getFilePathInput('Enter filepath')
    else:
        jobFolder = sealeyFolder + '\\Refresh ' + cf.getTimeStamp()

    inputFolder = jobFolder + '\\Input'
    imagesFolder = jobFolder + '\\Input\\WebMain'
    workFolder = jobFolder + '\\Work'

    createFolders(jobFolder, inputFolder, imagesFolder, workFolder)

    while 1 == 1:
        options = {
            1: 'Download Data',
            2: 'Download Images',
            3: 'Convert PNG to JPG',
            4: 'Load Data into DB',
            5: 'Load Data into Paper Cat & New Products into EntityMatch',
            6: 'Generate Consumables Audit',
            '*': 'Run all of the above'
            }

        print('Please select from the following options:')
        for x in options:
            print(str(x) + ': ' + options[x])

        chosenOption = input()

        if chosenOption == '1' or chosenOption == '*':
            downloadData(inputFolder)

        if chosenOption == '2' or chosenOption == '*':
            downloadImages(imagesFolder)

        if chosenOption == '3' or chosenOption == '*':
            cf.convertToJPG(imagesFolder)

        if chosenOption == '4' or chosenOption == '*':
            loadIntoDB(inputFolder)

        if chosenOption == '5' or chosenOption == '*':
            processNewProducts()

            if cf.get_bool('Export full matched with unmatched report?'):
                import Misc.EntityMatchTasks as emt
                emt.getMatchedWithUnmatched(13, 12, workFolder)

            input('Check supplier data email inbox for new product codes to match. '
                  'Do not proceed until matched to MAM Consumables Tree.')

        if chosenOption == '6' or chosenOption == '*':
            if cf.get_bool('Have the latest products been matched up in Entity Match?'):
                generateConsumablesAudit()




#create files to send to MAM


start()