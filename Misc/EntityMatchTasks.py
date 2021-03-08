import CommonFunctions as cf
import CommonSQLFunctions as csf

def getMatchedWithUnmatched(sourceEntityTypeId, targetEntityTypeId, saveLocation, surrogateEntityTypeId):

    if surrogateEntityTypeId is None:
        data = csf.executeSQLWithResults(f'EXEC EntityMatch.dbo.FullEntityMatchWithUnmatched @EntityOne = {sourceEntityTypeId}'
                                  f', @EntityTwo = {targetEntityTypeId}')
    else:
        data = csf.executeSQLWithResults(f'EXEC EntityMatch.dbo.FullEntityMatchWithUnmatchedViaSurrogate '
                                  f'@EntityOne = {sourceEntityTypeId},'
                                  f'@EntityTwo = {targetEntityTypeId},'
                                  f'@SurrogateEntity = {surrogateEntityTypeId}')

    cf.saveDataInExcel(saveLocation, f'FullMatchedWithUnmatched {sourceEntityTypeId} vs {targetEntityTypeId} '
                                     f'{cf.getTimeStamp()}', data)

def start():
    while 1 == 1:
        options = {
            0: 'Full Matched With Unmatched - Report',
            1: 'Full Matched With Unmatched Via Surrogate - Report'
        }

        print('Please select from the following options:')
        for x in options:
            print(str(x) + ': ' + options[x])

        chosenOption = input()

        if chosenOption == '0' or chosenOption == '1' or chosenOption.lower() == '*':
            saveLocation = cf.getFilePathInput('Enter the save location for the report:')
            sourceEntityTypeId = cf.inputRequest('Enter the source Entity Type ID:', 'int')
            targetEntityTypeId = cf.inputRequest('Enter the target Entity Type ID:', 'int')

            surrogateEntityTypeId = None

            if chosenOption == '1':
                surrogateEntityTypeId = cf.inputRequest('Please enter the surrogate Entity Type ID', 'int')

            getMatchedWithUnmatched(sourceEntityTypeId, targetEntityTypeId, saveLocation, surrogateEntityTypeId)

