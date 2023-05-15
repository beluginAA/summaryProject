import pandas as pd  
import datetime
import warnings
import threading 
import sys
import columns

from datetime import datetime  
from loguru import logger 
from processing import Preproccessing, PostProcessing, ResultFiles
from functions import Functions

warnings.simplefilter(action='ignore', category=(FutureWarning, UserWarning))


class Tables:

    def __init__(self, databaseRoot:str, excelRoot:str):
        self.databaseRoot = databaseRoot
        self.excelRoot = excelRoot
        self.isSuccessUpdatedRD = False
        self.isSuccessUpdatedDocumentation = False
        self.isSuccessUpdatedStatus = False


    class RD:

        logger.remove()
        loggerRD = logger.bind(name = 'RDlogger').opt(colors = True)
        loggerRD.add(sink = sys.stdout, format = "<green>{time:HH:mm:ss}</green> | {message}", level = 'INFO')

        def __init__(self):
            self.databaseName = 'РД'
            connect = Preproccessing(Tables.databaseRoot, Tables.excelRoot)
            self.functions = Functions.RD()
            self.result = ResultFiles()
            self.msDatabase = connect.to_database(self.databaseName)
            self.msDatabase = connect.to_database(self.databaseName)
            self.excelDatabase = connect.to_excel()
            self.changedColumns = self.msDatabase.columns
        
        def workOnRD(self) -> bool:
            Tables.RD.loggerRD.info('Working on RD.')
            self.excelDatabase, self.msDatabase = self.clearingDataframes(self.excelDatabase, self.msDatabase)
            excelDatabaseCopy, msDatabaseCopy = self.findingMissingValues(self.excelDatabase, self.msDatabase)
            rdNameDf, rdKksDf = self.mergingTwoDataFrames(excelDatabaseCopy, msDatabaseCopy)
            self.findingMissedRows(rdNameDf, self.excelDatabase)
            rdKksDfCopy, rdNameDfCopy = self.changingDataframes(rdKksDf, rdNameDf)
            self.preparingChangedDataForLogFile(rdKksDfCopy, rdNameDfCopy)
            self.preparingFinalFiles(rdKksDf, rdNameDf)
            summaryDf = self.preparingFinalFiles(rdKksDf, rdNameDf)
            self.makingChangesToDatabase(summaryDf)

        @staticmethod
        def clearingDataframes(excelDb:pd.DataFrame, msDb:pd.DataFrame) -> list[pd.DataFrame]:
            Tables.RD.loggerRD.info('Clearing dataframes')
            excelDb = excelDb.dropna(subset=['Коды работ по выпуску РД'])
            excelDb['Разработчики РД (актуальные)'] = excelDb.apply(Tables.RD.functions.changing_developer, axis = 1)
            excelDb = excelDb[~excelDb['Коды работ по выпуску РД'].str.contains('\.C\.', regex=False)]
            excelDb['Объект'] = excelDb['Объект'].apply(lambda row: row[ : row.find(' ')])
            excelDb['WBS'] = excelDb['WBS'].apply(Tables.RD.functions.changing_wbs)
            excelDb['Код KKS документа'] = excelDb['Код KKS документа'].astype(str)
            excelDb = excelDb.loc[~excelDb['Код KKS документа'].str.contains('\.KZ\.|\.EK\.|\.TZ\.|\.KM\.|\.GR\.')]
            for column in columns.convert_columns[:4]:
                excelDb[column] = excelDb[column].apply(lambda row: '' if not isinstance(row, datetime) else row.strftime('%d-%m-%Y'))
                msDb[column] = msDb[column].apply(lambda row: '' if not isinstance(row, datetime) else row.strftime('%d-%m-%Y'))
            return excelDb, msDb
    
        @staticmethod
        def findingMissingValues(excelDb:pd.DataFrame, msDb:pd.DataFrame) -> list[pd.DataFrame]:
            Tables.RD.loggerRD.info('Finding missing values ​​in a report.')
            msDatabaseCopy = msDb.copy()
            excelDatabaseCopy = excelDb.copy()
            msDbJE = msDb.copy()
            msDbJE = msDbJE[['Коды работ по выпуску РД']]
            excelDbJE = excelDb.copy()
            excelDbJE = excelDbJE[excelDbJE['Коды работ по выпуску РД'].str.contains('\.J\.|\.E\.')].reset_index()[['Коды работ по выпуску РД']]
            excelDbJE = excelDbJE.apply(lambda df: Tables.RD.functions.missed_codes_excel(df, msDbJE), axis = 1)
            Tables.RD.result.to_logfile(excelDbJE.dropna().reset_index(drop = True), 'Пропущенные значения, которые есть в отчете, но нет в РД (J, E)')
            return excelDatabaseCopy, msDatabaseCopy
        
        @staticmethod
        def mergingTwoDataFrames(excelDbCopy:pd.DataFrame, msDbCopy:pd.DataFrame) -> list[pd.DataFrame]:
            Tables.RD.loggerRD.info('Merging two dataframes')
            rdKksDf = (pd.merge(excelDbCopy, msDbCopy, #m_df_1
                                    how='outer',
                                    on=['Коды работ по выпуску РД', 'Код KKS документа'],
                                    suffixes=['', '_new'], 
                                    indicator=True))
            dfPath =rdKksDf[rdKksDf['_merge'] == 'right_only'][columns.mdf1_columns] #tmp_df
            dfPath.columns = columns.new_columns

            rdNameDf = (dfPath.iloc[:, :-1].merge(excelDbCopy, # m_df_2
                                    how='outer',
                                    on=['Коды работ по выпуску РД', 'Наименование объекта/комплекта РД'],
                                    suffixes=['', '_new'],
                                    indicator=True))
            rdKksDf['Статус текущей ревизии_new'] = rdKksDf.apply(Tables.RD.functions.changing_status, axis = 1)
            rdNameDf['Статус текущей ревизии_new'] = rdNameDf.apply(Tables.RD.functions.changing_status, axis = 1)
            return rdNameDf, rdKksDf
        
        @staticmethod
        def findingMissedRows(rdNameDf:pd.DataFrame, excelDb:pd.Dataframe) -> None:
            Tables.RD.loggerRD.info('Finding missed rows.')
            missedRows = rdNameDf[rdNameDf['_merge'] == 'left_only'].reset_index()[columns.mdf2_columns]
            missedRows.columns = columns.new_columns
            missedJE = missedRows[missedRows['Коды работ по выпуску РД'].str.contains('\.J\.|\.E\.')].reset_index()[['Коды работ по выпуску РД', 'Наименование объекта/комплекта РД']]
            missedJE = missedJE.apply(lambda df: Tables.RD.functions.missed_codes(df, excelDb), axis = 1)
            missedJE = missedJE.dropna().reset_index(drop = True)
            Tables.RD.result.to_logfile(missedJE, 'Пропущенные значения, которые есть в РД, но нет в отчете (J, E)')
        
        @staticmethod
        def changingDataframes(rdKksDf:pd.DataFrame, rdNameDf:pd.DataFrame) -> list[pd.DataFrame]:
            Tables.RD.loggerRD.info('Changing dataframes')
            rdKksDf = rdKksDf[rdKksDf['_merge'] == 'both']
            rdNameDf = rdNameDf[rdNameDf['_merge'] == 'both']
            rdKksDf['Наименование объекта/комплекта РД'] = rdKksDf.apply(lambda row: Tables.RD.functions.changing_name(row), axis = 1)
            rdNameDf['Код KKS документа'] = rdNameDf.apply(lambda row: Tables.RD.functions.changing_code(row), axis = 1)
            for col in columns.clmns:
                rdKksDf[col] = rdKksDf.apply(lambda df: Tables.RD.functions.changing_data(df, col), axis = 1)
                rdNameDf[col] = rdNameDf.apply(lambda df: Tables.RD.functions.changing_data(df, col), axis = 1)
            rdKksDfCopy = rdKksDf.copy()
            rdNameDfCopy = rdNameDf.copy()
            rdKksDf = rdKksDf[columns.mdf1_columns]
            rdKksDf.columns = columns.new_columns
            rdNameDf = rdNameDf[columns.mdf2_columns]
            rdNameDf.columns = columns.new_columns
            return rdKksDfCopy, rdNameDfCopy

        @staticmethod
        def preparingChangedDataForLogFile(rdKksDfCopy:pd.DataFrame, rdNameDfCopy:pd.DataFrame) -> None:
            Tables.RD.loggerRD.info('Preparing changed data for log-file.')
            rdNameLogFile = rdNameDfCopy[columns.logFileColumns]
            rdKksLogFile = rdKksDfCopy[columns.logFileColumns]
            rdKks = rdKksLogFile.copy()
            rdName = rdNameLogFile.copy()
            rdName['Код KKS документа'] = rdName['Код KKS документа'].apply(Tables.RD.functions.find_row)
            changedLogfile = pd.concat([rdName, rdKks])
            Tables.RD.resultThread = threading.Thread(name = 'resultThread', target = Tables.RD.result.to_logfile, args = (changedLogfile.reset_index(drop = True), 'Измененные значения',))
            Tables.RD.resultThread.start()

        @staticmethod
        def preparingFinalFiles(rdKksDf, rdNameDf) -> pd.DataFrame:
            Tables.RD.loggerRD.info('Preparing the final files.')
            summaryDf = pd.concat([rdKksDf, rdNameDf])
            summaryDf = summaryDf[columns.base_columns]
            summaryDf = summaryDf.reset_index(drop = True)
            resultExcelDf = summaryDf.copy()
            resultExcelDf['Объект'] = resultExcelDf['Объект'].apply(lambda row: resultExcelDf['Коды работ по выпуску РД'].str.slice(0, 5) if pd.isna(row) else row)
            resultExcelDf['WBS'] = resultExcelDf['WBS'].apply(lambda row: row if ~pd.isna(row) else resultExcelDf['Коды работ по выпуску РД'].apply(lambda row: row[6 : row.find('.', 6)]))
            resultExcelDf.columns = Tables.RD.changedColumns
            for col in resultExcelDf.columns:
                resultExcelDf[col] = resultExcelDf.apply(lambda df: Tables.RD.functions.finding_empty_rows(df, col), axis = 1)
                summaryDf[col] = summaryDf.apply(lambda df: Tables.RD.functions.finding_empty_rows(df, col), axis = 1)
            Tables.RD.resultThread.join()
            Tables.RD.result.to_resultfile(resultExcelDf)
            return summaryDf

        @staticmethod
        def makingChangesToDatabase(summaryDf:pd.DataFrame) -> None:
            Tables.RD.loggerRD.info('Making changes to the database.')
            step = PostProcessing(Tables.databaseRoot)
            step.delete_table()
            step.create_table()
            step.insert_into_table(summaryDf)