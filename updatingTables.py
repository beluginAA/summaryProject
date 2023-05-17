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
from tkinter.filedialog import askopenfilename

warnings.simplefilter(action='ignore', category=(FutureWarning, UserWarning))


databaseRoot = askopenfilename(title='Select database', filetypes=[('*.mdb', '*.accdb')]).replace('/', '\\')
excelRoot = askopenfilename(title="Select file for compare", filetypes=[("Excel Files", "*.xlsx"), ("Excel Binary Workbook", "*.xlsb")])

isSuccessUpdatedRD = False
isSuccessUpdatedDocumentation = False
isSuccessUpdatedStatus = False

class RD:

    logger.remove()
    loggerRD = logger.bind(name = 'RDlogger').opt(colors = True)
    loggerRD.add(sink = sys.stdout, format = "<green>{time:HH:mm:ss}</green> | {message}", level = 'INFO')

    def __init__(self):
        self.databaseName = 'РД'
        connect = Preproccessing(databaseRoot, excelRoot)
        self.functions = Functions.RD()
        self.result = ResultFiles()
        self.msDatabase = connect.to_database(self.databaseName)
        self.excelDatabase = connect.to_excel()
        self.changedColumns = self.msDatabase.columns
    
    def updateRD(self) -> None:
        RD.loggerRD.info('Working on RD:')   
        self._clearingDataframes()
        excelDatabaseCopy, msDatabaseCopy = self._findingMissingValues()
        self._mergingTwoDataFrames(excelDatabaseCopy, msDatabaseCopy)
        self._findingMissedRows(self.excelDatabase)
        rdKksDfCopy, rdNameDfCopy = self._changingDataframes()
        self._preparingChangedDataForLogFile(rdKksDfCopy, rdNameDfCopy)
        summaryDf = self._preparingFinalFiles()
        self._makingChangesToDatabase(summaryDf)

    @staticmethod
    def _clearingDataframes() -> list[pd.DataFrame]:
        RD.loggerRD.info('  Clearing dataframes') 
        RD.excelDatabase = RD.excelDatabase.dropna(subset=['Коды работ по выпуску РД'])
        RD.excelDatabase['Разработчики РД (актуальные)'] = RD.excelDatabase.apply(RD.functions.changing_developer, axis = 1)
        RD.excelDatabase = RD.excelDatabase[~RD.excelDatabase['Коды работ по выпуску РД'].str.contains('\.C\.', regex=False)]
        RD.excelDatabase['Объект'] = RD.excelDatabase['Объект'].apply(lambda row: row[ : row.find(' ')])
        RD.excelDatabase['WBS'] = RD.excelDatabase['WBS'].apply(RD.functions.changing_wbs)
        RD.excelDatabase['Код KKS документа'] = RD.excelDatabase['Код KKS документа'].astype(str)
        RD.excelDatabase = RD.excelDatabase.loc[~RD.excelDatabase['Код KKS документа'].str.contains('\.KZ\.|\.EK\.|\.TZ\.|\.KM\.|\.GR\.')]
        for column in columns.convert_columns[:4]:
            RD.excelDatabase[column] = RD.excelDatabase[column].apply(lambda row: '' if not isinstance(row, datetime) else row.strftime('%d-%m-%Y'))
            RD.msDatabase[column] = RD.msDatabase[column].apply(lambda row: '' if not isinstance(row, datetime) else row.strftime('%d-%m-%Y'))

    @staticmethod
    def _findingMissingValues() -> list[pd.DataFrame]:
        RD.loggerRD.info('  Finding missing values ​​in a report.')
        msDatabaseCopy = RD.msDatabase.copy()
        excelDatabaseCopy = RD.excelDatabase.copy()
        msDbJE =RD.msDatabase.copy()
        msDbJE = msDbJE[['Коды работ по выпуску РД']]
        excelDbJE = RD.excelDatabase.copy()
        excelDbJE = excelDbJE[excelDbJE['Коды работ по выпуску РД'].str.contains('\.J\.|\.E\.')].reset_index()[['Коды работ по выпуску РД']]
        excelDbJE = excelDbJE.apply(lambda df: RD.functions.missed_codes_excel(df, msDbJE), axis = 1)
        # RD.result.to_logfile(excelDbJE.dropna().reset_index(drop = True), 'Пропущенные значения, которые есть в отчете, но нет в РД (J, E)')
        return excelDatabaseCopy, msDatabaseCopy
    
    @staticmethod
    def _mergingTwoDataFrames(excelDbCopy:pd.DataFrame, msDbCopy:pd.DataFrame):
        RD.loggerRD.info('  Merging two dataframes')
        RD.rdKksDf = (pd.merge(excelDbCopy, msDbCopy, #m_df_1
                                how='outer',
                                on=['Коды работ по выпуску РД', 'Код KKS документа'],
                                suffixes=['', '_new'], 
                                indicator=True))
        dfPath = RD.rdKksDf[RD.rdKksDf['_merge'] == 'right_only'][columns.mdf1_columns] #tmp_df
        dfPath.columns = columns.new_columns

        RD.rdNameDf = (dfPath.iloc[:, :-1].merge(excelDbCopy, # m_df_2
                                how='outer',
                                on=['Коды работ по выпуску РД', 'Наименование объекта/комплекта РД'],
                                suffixes=['', '_new'],
                                indicator=True))
        RD.rdKksDf['Статус текущей ревизии_new'] = RD.rdKksDf.apply(RD.functions.changing_status, axis = 1)
        RD.rdNameDf['Статус текущей ревизии_new'] = RD.rdNameDf.apply(RD.functions.changing_status, axis = 1)
    
    @staticmethod
    def _findingMissedRows(excelDb:pd.DataFrame) -> None:
        RD.loggerRD.info('  Finding missed rows.')
        missedRows = RD.rdNameDf[RD.rdNameDf['_merge'] == 'left_only'].reset_index()[columns.mdf2_columns]
        missedRows.columns = columns.new_columns
        missedJE = missedRows[missedRows['Коды работ по выпуску РД'].str.contains('\.J\.|\.E\.')].reset_index()[['Коды работ по выпуску РД', 'Наименование объекта/комплекта РД']]
        missedJE = missedJE.apply(lambda df: RD.functions.missed_codes(df, excelDb), axis = 1)
        missedJE = missedJE.dropna().reset_index(drop = True)
        # RD.result.to_logfile(missedJE, 'Пропущенные значения, которые есть в РД, но нет в отчете (J, E)')
    
    @staticmethod
    def _changingDataframes() -> list[pd.DataFrame]:
        RD.loggerRD.info('  Changing dataframes')
        RD.rdKksDf = RD.rdKksDf[RD.rdKksDf['_merge'] == 'both']
        RD.rdNameDf = RD.rdNameDf[RD.rdNameDf['_merge'] == 'both']
        RD.rdKksDf['Наименование объекта/комплекта РД'] = RD.rdKksDf.apply(lambda row: RD.functions.changing_name(row), axis = 1)
        RD.rdNameDf['Код KKS документа'] = RD.rdNameDf.apply(lambda row: RD.functions.changing_code(row), axis = 1)
        for col in columns.clmns:
            RD.rdKksDf[col] = RD.rdKksDf.apply(lambda df: RD.functions.changing_data(df, col), axis = 1)
            RD.rdNameDf[col] = RD.rdNameDf.apply(lambda df: RD.functions.changing_data(df, col), axis = 1)
        rdKksDfCopy = RD.rdKksDf.copy()
        rdNameDfCopy = RD.rdNameDf.copy()
        RD.rdKksDf = RD.rdKksDf[columns.mdf1_columns]
        RD.rdKksDf.columns = columns.new_columns
        RD.rdNameDf = RD.rdNameDf[columns.mdf2_columns]
        RD.rdNameDf.columns = columns.new_columns
        return rdKksDfCopy, rdNameDfCopy

    @staticmethod
    def _preparingChangedDataForLogFile(rdKksDfCopy:pd.DataFrame, rdNameDfCopy:pd.DataFrame) -> None:
        RD.loggerRD.info('  Preparing changed data for log-file.')
        rdNameLogFile = rdNameDfCopy[columns.logFileColumns]
        rdKksLogFile = rdKksDfCopy[columns.logFileColumns]
        rdKks = rdKksLogFile.copy()
        rdName = rdNameLogFile.copy()
        rdName['Код KKS документа'] = rdName['Код KKS документа'].apply(RD.functions.find_row)
        changedLogfile = pd.concat([rdName, rdKks])
        RD.resultThread = threading.Thread(name = 'resultThread', target = RD.result.to_logfile, args = (changedLogfile.reset_index(drop = True), 'Измененные значения',))
        RD.resultThread.start()

    @staticmethod
    def _preparingFinalFiles() -> pd.DataFrame:
        RD.loggerRD.info('  Preparing the final files.')
        summaryDf = pd.concat([RD.rdKksDf, RD.rdNameDf])
        summaryDf = summaryDf[columns.base_columns]
        summaryDf = summaryDf.reset_index(drop = True)
        resultExcelDf = summaryDf.copy()
        resultExcelDf['Объект'] = resultExcelDf['Объект'].apply(lambda row: resultExcelDf['Коды работ по выпуску РД'].str.slice(0, 5) if pd.isna(row) else row)
        resultExcelDf['WBS'] = resultExcelDf['WBS'].apply(lambda row: row if ~pd.isna(row) else resultExcelDf['Коды работ по выпуску РД'].apply(lambda row: row[6 : row.find('.', 6)]))
        resultExcelDf.columns = RD.changedColumns
        for col in resultExcelDf.columns:
            resultExcelDf[col] = resultExcelDf.apply(lambda df:RD.functions.finding_empty_rows(df, col), axis = 1)
            summaryDf[col] = summaryDf.apply(lambda df: RD.functions.finding_empty_rows(df, col), axis = 1)
        RD.resultThread.join()
        RD.result.to_resultfile(resultExcelDf)
        return summaryDf

    @staticmethod
    def _makingChangesToDatabase(summaryDf:pd.DataFrame) -> None:
        RD.loggerRD.info('  Making changes to the database.')
        step = PostProcessing(databaseRoot, RD.databaseName)
        step.delete_table()
        step.create_table()
        if step.insert_into_table(summaryDf):
            isSuccessUpdatedRD = True
        
    