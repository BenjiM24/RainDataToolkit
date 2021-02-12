import os, sys
from os import listdir
import pandas as pd
from os.path import isfile, join
import xlsxwriter
from PIL import Image

def get_bool(prompt):
    while True:
        try:
           return {"true": True,
                   "y": True,
                   "1": True,
                   "yes": True,
                   "false": False,
                   "n": False,
                   "0": False,
                   "no": False}[input(prompt).lower()]
        except KeyError:
           print("Invalid input please enter True or False!")

def getFilePathInput(requestMessage):
    jobFolder = ''
    requestFilePath = True

    while requestFilePath:
        jobFolder = input(requestMessage)
        if os.path.exists(jobFolder):
            requestFilePath = False
        else:
            requestFilePath = True

    return jobFolder

def saveDataInExcel(saveLocation, filename, df):
    fileSavePath = saveLocation + '\\' + filename + '.xlsx'
    writer = pd.ExcelWriter(fileSavePath, engine='xlsxwriter')

    df.to_excel(writer, sheet_name=filename, startrow=1, header=False, index=False)

    # Get the xlsxwriter workbook and worksheet objects.
    workbook = writer.book
    worksheet = writer.sheets[filename]

    # Get the dimensions of the dataframe.
    (max_row, max_col) = df.shape

    # Create a list of column headers, to use in add_table().
    column_settings = []
    for header in df.columns:
        column_settings.append({'header': header})

    # Add the table.
    worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

    # Make the columns wider for clarity.
    worksheet.set_column(0, max_col - 1, 12)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    #print(f'{filename} successfully saved in Excel: {fileSavePath}')

def inputRequest(parameterRequest, parameterType=None):
    returnValue = None

    while True:
        try:
            if parameterType is None:
                returnValue = input(parameterRequest).split(',')
                break
            if parameterType == 'int':
                returnValue = int(input(parameterRequest))
                break
            if parameterType == 'bool':
                returnValue = get_bool(parameterRequest)
                break
                raise ValueError('Bool value not supplied')

            if parameterType == 'filepath':
                filePathTemp = input(parameterRequest)
                if os.path.exists(filePathTemp):
                    returnValue = filePathTemp
                    break
                else:
                    raise ValueError('Filepath does not exist')
        except ValueError:
            print(f'Please enter a valid {parameterType}')
            continue
        break

    return returnValue

def getFilenamesWithinFolder(folder):
    return [f for f in listdir(folder) if isfile(join(folder, f))]

def resizeImage(filePath):
    img = Image.open(filePath)
    new_img = img.resize((600, 400))
    new_img.save(filePath, "JPEG", optimize=True)
