import CommonFunctions as cf
import CommonSQLFunctions as csf

def CleansedCoverageComparedToAutocatBackup(supplierId, brand):
    df = csf.executeSQLWithResults(f"EXEC CleansedCoverageComparedToAutocatBackup @SupplierId = {supplierId}, "
                                   f"@BrandSupplier = '{brand}'", 'CatalogueTecDocCleansed')

    saveLocation = cf.getFilePathInput('Enter save location')

    cf.saveDataInExcel(saveLocation, f'{supplierId} - {brand} - Proposed Coverage '
                                     f'vs. Existing Autocat Coverage {cf.getTimeStamp()}', df)


CleansedCoverageComparedToAutocatBackup(140, 'Corteco')