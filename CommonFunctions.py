import os, sys
from os import listdir
import pandas as pd
from os.path import isfile, join
import xlsxwriter
from PIL import Image
import datetime

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

    df = df_column_uniquify(df)
    df.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)

    # Get the xlsxwriter workbook and worksheet objects.
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

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

def getTimeStamp():
    return datetime.datetime.now().strftime('%d-%m-%Y %H%M%S')


def df_column_uniquify(df):
    df_columns = df.columns
    new_columns = []
    for item in df_columns:
        newitem = item
        counter = 0
        while any(newitem.lower() == val.lower() for val in new_columns):
            #how many times has this already been added:
            counter += 1
            newitem = "{}_{}".format(item, counter)
        new_columns.append(newitem)
    df.columns = new_columns
    return df

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
