import shutil
import zipfile
from pathlib import Path
import CommonFunctions as cf
import CommonSQLFunctions as csf
import csv
import os
from tqdm import tqdm

def getDATData(supplierNumber):
    print('Retrieving data from SQL. No progress bar for this one :(')
    return csf.executeSQLWithResults(f'EXEC CatalogueManagerOutput.TecDoc.GenerateDATFiles @SupplierNumber '
                                     f'= {supplierNumber};')

def saveDATIntoFiles(data, saveLocation, supplierNumber):
    print('Saving data into raw DAT Files.')
    for df in tqdm(data):
        columns = list(df.columns.values)

        fileName = columns[0]
        Path(saveLocation + '\\RawDATFiles').mkdir(parents=True, exist_ok=True)

        df.to_csv(saveLocation + '\\RawDATFiles\\' + fileName + '.' + str(supplierNumber), sep="\t", index=False,
                           quoting=csv.QUOTE_NONE, header=False)


def copyDocumentFiles(supplierNumber, masterImagesLocation, masterDocumentsLocation, saveLocation):
    requiredFiles = csf.executeSQLWithResults(f"EXEC CatalogueManagerOutput.TecDoc.GetRequiredDocuments "
                                              f"@SupplierNumber = '{supplierNumber}';")

    saveLocation = saveLocation + '\\rawDocuments'
    Path(saveLocation).mkdir(parents=True, exist_ok=True)

    print('Copying required images and documents.')
    for index, row in tqdm(requiredFiles.iterrows(), total=requiredFiles.shape[0]):
        filename = row[0]
        if (os.path.isfile(masterDocumentsLocation + '\\' + filename)) and filename.endswith('.pdf'):
            #copy
            shutil.copy(os.path.join(masterDocumentsLocation, filename), os.path.join(saveLocation, filename))
        if (os.path.isfile(masterImagesLocation + '\\' + filename)) and filename.endswith('.jpg'):
            #move and resize
            imageSaveLocation = os.path.join(saveLocation, filename)
            shutil.copy(os.path.join(masterImagesLocation, filename), imageSaveLocation)
            cf.resizeImage(imageSaveLocation)


def zipFilesTogether(saveLocation, supplierNumber):
    Path(saveLocation + '\\DATPackage').mkdir(parents=True, exist_ok=True)
    zf = zipfile.ZipFile(saveLocation + '\\DATPackage\\' + str(supplierNumber) + '.zip', mode="w",
                         compression=zipfile.ZIP_DEFLATED)

    print('Adding Documents to ZIP file.')
    os.chdir(saveLocation + '\\rawDocuments')
    for file in tqdm(os.listdir(saveLocation + '\\rawDocuments')):
        zf.write(file)

    print('Adding DAT files to ZIP file.')
    os.chdir(saveLocation + '\\rawDATFiles')
    for file in tqdm(os.listdir(saveLocation + '\\rawDATFiles')):
        zf.write(file)

    zf.close()


def start(saveLocation=None):
    if saveLocation == None:
        saveLocation = cf.getFilePathInput('Enter the required save location:')
    saveLocation = saveLocation + '\\DAT Export ' + cf.getTimeStamp()
    Path(saveLocation).mkdir(parents=True, exist_ok=True)
    supplierNumber = cf.inputRequest('Enter the required supplier number:', 'int')

    imagesRequired = cf.get_bool('Do you require images & documents (PDFs)?')

    DATData = getDATData(supplierNumber)
    saveDATIntoFiles(DATData, saveLocation, supplierNumber)

    if imagesRequired:
        masterImagesLocation = cf.getFilePathInput('Enter the location of the images. Enter the Master Images filepath '
                                                   'for a full delivery, or the latest input images filepath to just '
                                                   'load changes.')
        masterDocumentsLocation = cf.getFilePathInput('Enter the location of the Documents. Enter the Master Documents '
                                                      'filepath for a full delivery, or the latest document filepath to'
                                                      'just load changes.')
        copyDocumentFiles(supplierNumber, masterImagesLocation, masterDocumentsLocation, saveLocation)

    zipFilesTogether(saveLocation, supplierNumber)

    print('Data ready:')
    input(saveLocation+'\\DATPackage')