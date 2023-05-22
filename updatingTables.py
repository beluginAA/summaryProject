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

isSuccessUpdatedRD = True
isSuccessUpdatedDocumentation = True
isSuccessUpdatedStatus = False

class RD:

    logger.remove()
    loggerRD = logger.bind(name = 'RDlogger').opt(colors = True)
    loggerRD.add(sink = sys.stdout, format = "<green>{time:HH:mm:ss}</green> | {message}", level = 'INFO')

    def __init__(self):
        RD.loggerRD.info('Working on RD.')
        self.databaseName = 'РД'
        connect = Preproccessing(databaseRoot, excelRoot)
        self.functions = Functions.RD()
        self.result = ResultFiles(self.databaseName)
        self.msDatabase = connect.to_database(self.databaseName)
        self.excelDatabase = connect.to_excel()
        self.columns = columns.RD()
        self.changedColumns = self.msDatabase.columns
    
    def done(self) -> None: 
        self._clearingDataframes(self)
        self._findingMissingValues(self)
        self._mergingTwoDataFrames(self)
        self._findingMissedRows(self)
        self._changingDataframes(self)
        self._preparingChangedDataForLogFile(self)
        summaryDf = self._preparingFinalFiles(self)
        self._makingChangesToDatabase(self, summaryDf)

    @staticmethod
    def _clearingDataframes(self) -> None:
        RD.loggerRD.info('  Clearing dataframes') 
        self.excelDatabase = self.excelDatabase.dropna(subset=['Коды работ по выпуску РД'])
        self.excelDatabase['Разработчики РД (актуальные)'] = self.excelDatabase.apply(self.functions.changing_developer, axis = 1)
        self.excelDatabase = self.excelDatabase[~self.excelDatabase['Коды работ по выпуску РД'].str.contains('\.C\.', regex=False)]
        self.excelDatabase['Объект'] = self.excelDatabase['Объект'].apply(lambda row: row[ : row.find(' ')])
        self.excelDatabase['WBS'] = self.excelDatabase['WBS'].apply(self.functions.changing_wbs)
        self.excelDatabase['Код KKS документа'] = self.excelDatabase['Код KKS документа'].astype(str)
        self.excelDatabase = self.excelDatabase.loc[~self.excelDatabase['Код KKS документа'].str.contains('\.KZ\.|\.EK\.|\.TZ\.|\.KM\.|\.GR\.')]
        for column in self.columns.convert_columns[:4]:
            self.excelDatabase[column] = self.excelDatabase[column].apply(lambda row: '' if not isinstance(row, datetime) else row.strftime('%d-%m-%Y'))
            self.msDatabase[column] = self.msDatabase[column].apply(lambda row: '' if not isinstance(row, datetime) else row.strftime('%d-%m-%Y'))

    @staticmethod
    def _findingMissingValues(self) -> None:
        RD.loggerRD.info('  Finding missing values ​​in a report.')
        self.msDatabaseCopy = self.msDatabase.copy()
        self.excelDatabaseCopy = self.excelDatabase.copy()
        msDbJE =self.msDatabase.copy()
        msDbJE = msDbJE[['Коды работ по выпуску РД']]
        excelDbJE = self.excelDatabase.copy()
        excelDbJE = excelDbJE[excelDbJE['Коды работ по выпуску РД'].str.contains('\.J\.|\.E\.')].reset_index()[['Коды работ по выпуску РД']]
        excelDbJE = excelDbJE.apply(lambda df: self.functions.missed_codes_excel(df, msDbJE), axis = 1)
        # self.result.to_logfile(excelDbJE.dropna().reset_index(drop = True), 'Пропущенные значения, которые есть в отчете, но нет в РД (J, E)')
    
    @staticmethod
    def _mergingTwoDataFrames(self) -> None:
        RD.loggerRD.info('  Merging two dataframes')
        self.rdKksDf = (pd.merge(self.excelDatabaseCopy, self.msDatabaseCopy, #m_df_1
                                how='outer',
                                on=['Коды работ по выпуску РД', 'Код KKS документа'],
                                suffixes=['', '_new'], 
                                indicator=True))
        dfPath = self.rdKksDf[self.rdKksDf['_merge'] == 'right_only'][self.columns.mdf1_columns] #tmp_df
        dfPath.columns = self.columns.new_columns

        self.rdNameDf = (dfPath.iloc[:, :-1].merge(self.excelDatabaseCopy, # m_df_2
                                how='outer',
                                on=['Коды работ по выпуску РД', 'Наименование объекта/комплекта РД'],
                                suffixes=['', '_new'],
                                indicator=True))
        self.rdKksDf['Статус текущей ревизии_new'] = self.rdKksDf.apply(self.functions.changing_status, axis = 1)
        self.rdNameDf['Статус текущей ревизии_new'] = self.rdNameDf.apply(self.functions.changing_status, axis = 1)
    
    @staticmethod
    def _findingMissedRows(self) -> None:
        RD.loggerRD.info('  Finding missed rows.')
        missedRows = self.rdNameDf[self.rdNameDf['_merge'] == 'left_only'].reset_index()[self.columns.mdf2_columns]
        missedRows.columns = self.columns.new_columns
        missedJE = missedRows[missedRows['Коды работ по выпуску РД'].str.contains('\.J\.|\.E\.')].reset_index()[['Коды работ по выпуску РД', 'Наименование объекта/комплекта РД']]
        missedJE = missedJE.apply(lambda df: self.functions.missed_codes(df, self.excelDatabase), axis = 1)
        missedJE = missedJE.dropna().reset_index(drop = True)
        # RD.result.to_logfile(missedJE, 'Пропущенные значения, которые есть в РД, но нет в отчете (J, E)')
    
    @staticmethod
    def _changingDataframes(self) -> None:
        RD.loggerRD.info('  Changing dataframes')
        self.rdKksDf = self.rdKksDf[self.rdKksDf['_merge'] == 'both']
        self.rdNameDf = self.rdNameDf[self.rdNameDf['_merge'] == 'both']
        self.rdKksDf['Наименование объекта/комплекта РД'] = self.rdKksDf.apply(lambda row: self.functions.changing_name(row), axis = 1)
        self.rdNameDf['Код KKS документа'] = self.rdNameDf.apply(lambda row: self.functions.changing_code(row), axis = 1)
        for col in self.columns.clmns:
            self.rdKksDf[col] = self.rdKksDf.apply(lambda df: self.functions.changing_data(df, col), axis = 1)
            self.rdNameDf[col] = self.rdNameDf.apply(lambda df: self.functions.changing_data(df, col), axis = 1)
        self.rdKksDfCopy = self.rdKksDf.copy()
        self.rdNameDfCopy = self.rdNameDf.copy()
        self.rdKksDf = self.rdKksDf[self.columns.mdf1_columns]
        self.rdKksDf.columns = self.columns.new_columns
        self.rdNameDf = self.rdNameDf[self.columns.mdf2_columns]
        self.rdNameDf.columns = self.columns.new_columns

    @staticmethod
    def _preparingChangedDataForLogFile(self) -> None:
        RD.loggerRD.info('  Preparing changed data for log-file.')
        rdNameLogFile = self.rdNameDfCopy[self.columns.logFileColumns]
        rdKksLogFile = self.rdKksDfCopy[self.columns.logFileColumns]
        rdKks = rdKksLogFile.copy()
        rdName = rdNameLogFile.copy()
        rdName['Код KKS документа'] = rdName['Код KKS документа'].apply(self.functions.find_row)
        changedLogfile = pd.concat([rdName, rdKks])
        self.resultThread = threading.Thread(name = 'resultThread', target = self.result.to_logfile, args = (changedLogfile.reset_index(drop = True), 'Измененные значения',))
        self.resultThread.start()

    @staticmethod
    def _preparingFinalFiles(self) -> pd.DataFrame:
        RD.loggerRD.info('  Preparing the final files.')
        summaryDf = pd.concat([self.rdKksDf, self.rdNameDf])
        summaryDf = summaryDf[self.columns.base_columns]
        summaryDf = summaryDf.reset_index(drop = True)
        resultExcelDf = summaryDf.copy()
        resultExcelDf['Объект'] = resultExcelDf['Объект'].apply(lambda row: resultExcelDf['Коды работ по выпуску РД'].str.slice(0, 5) if pd.isna(row) else row)
        resultExcelDf['WBS'] = resultExcelDf['WBS'].apply(lambda row: row if ~pd.isna(row) else resultExcelDf['Коды работ по выпуску РД'].apply(lambda row: row[6 : row.find('.', 6)]))
        resultExcelDf.columns = self.changedColumns
        for col in resultExcelDf.columns:
            resultExcelDf[col] = resultExcelDf.apply(lambda df:self.functions.finding_empty_rows(df, col), axis = 1)
            summaryDf[col] = summaryDf.apply(lambda df: self.functions.finding_empty_rows(df, col), axis = 1)
        self.resultThread.join()
        self.result.to_resultfile(resultExcelDf)
        return summaryDf

    @staticmethod
    def _makingChangesToDatabase(self, summaryDf:pd.DataFrame) -> None:
        RD.loggerRD.info('  Making changes to the database.')
        step = PostProcessing(databaseRoot, self.databaseName)
        # step.delete_table()
        # step.create_table()
        # if step.insert_into_table(summaryDf):
        #     isSuccessUpdatedRD = True


class Documentation:
    
    logger.remove()
    StatusLogger = logger.bind(name = 'DocumentationLogger').opt(colors = True)
    StatusLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self):
        Documentation.StatusLogger.info('Working on Documentation.')
        self.databaseName = 'Документация'
        connect = Preproccessing(databaseRoot, excelRoot)
        self.rdDatabase, self.docDatabase = connect.to_database('РД', self.databaseName, True)
        self.functions = Functions.Documentation()
        self.result = ResultFiles(self.databaseName)
        self.columns = columns.Documentation()
    
    def done(self) -> None:
        if isSuccessUpdatedRD:
            self._clearingDataframes(self)
            self._makingCopyOfOriginalDataframes(self)
            self._mergingTwoDataframes(self)
            self._makingCopyOfAlreadyJoinedDataframes(self)
            self._preparingMergingDataframesForSummaryDataframe(self)
            summaryDf = self._creatingSummaryTable(self)
            summaryDf = self._preparingFinalFileAndWritingToDatabase(self, summaryDf)
            self._makingChangedToDatabase(self, summaryDf)
            Documentation.StatusLogger.info('  Database updated.')

    @staticmethod
    def _clearingDataframes(self) -> None:
        Documentation.StatusLogger.info('  Clearing dataframes.')
        self.rdDatabase = self.rdDatabase[list(self.columns.rdColumns)]
        for col in self.columns.rdColumns:
            self.rdDatabase[col] = self.rdDatabase.apply(lambda df: self.functions.finding_empty_rows(df, col), axis = 1)
        for col in self.columns.doc_columns:
            self.docDatabase[col] = self.docDatabase.apply(lambda df: self.functions.finding_empty_rows(df, col), axis = 1)
        self.rdDatabase['Ревизия'] = self.rdDatabase['Ревизия'].apply(lambda row: None if row == '' else row)
        self.docDatabase['Срок'] = self.docDatabase['Срок'].apply(lambda row: row if row in ['в производстве', 'В производстве', None] else datetime.strptime(row, '%d.%m.%Y').date().strftime('%d-%m-%Y'))
        self.empty_rows_df = self.docDatabase[(pd.isna(self.docDatabase['Шифр'])) | (self.docDatabase['Вид'] != 'Проектная документация') | (self.docDatabase['Разработчик'] != 'Атомэнергопроект')]
        self.docDatabase = self.docDatabase[(~pd.isna(self.docDatabase['Шифр'])) & (self.docDatabase['Вид'] == 'Проектная документация') & (self.docDatabase['Разработчик'] == 'Атомэнергопроект')]

    @staticmethod
    def _makingCopyOfOriginalDataframes(self):
        Documentation.StatusLogger.info('  Making copy of original dataframes.')
        docDatabaseCopy = self.docDatabase.copy()
        rdDatabaseCopy = self.rdDatabase.copy()
        missed = self.functions.prepare_missed_rows(docDatabaseCopy, rdDatabaseCopy)
        self.result.to_resultfile(missed)

    @staticmethod
    def _mergingTwoDataframes(self) -> None:
        Documentation.StatusLogger.info('  Merging two dataframes.')
        self.cipherDf = pd.merge(self.docDatabase, self.rdDatabase,
                                how = 'left',
                                on = 'Шифр',
                                suffixes=['', '_new'],
                                indicator = True)
        leftOnly = self.cipherDf[self.cipherDf['_merge'] == 'left_only'][self.docDatabase.columns]
        self.cipherCodeDf = pd.merge(leftOnly, self.rdDatabase,
                            how = 'left',
                            left_on = 'Шифр',
                            right_on = 'Код',
                            suffixes=['', '_new'],
                            indicator=True)

    @staticmethod
    def _makingCopyOfAlreadyJoinedDataframes(self) -> None:
        Documentation.StatusLogger.info('  Making copies of already joined dataframes.')
        self.cipherDf = self.cipherDf[self.cipherDf['_merge'] == 'both'].copy()
        cipherCodeDfCopy = self.cipherCodeDf[self.cipherCodeDf['_merge'] == 'both'].copy()
        logDf = self.functions.prepare_data_for_logfile(self.cipherDf, cipherCodeDfCopy)
        self.result.to_logfile(logDf, 'Итоговые значения')

    @staticmethod
    def _preparingMergingDataframesForSummaryDataframe(self) -> None:
        Documentation.StatusLogger.info('  Preparing merging dataframes for summary dataframe.')
        self.resultCipherDf = self.cipherDf[self.cipherDf['_merge'] == 'both'].copy()
        self.resultCipherCodeDf = self.cipherCodeDf[self.cipherCodeDf['_merge'] == 'both'].copy()
        self.resultCipherDf['Тип'] = self.resultCipherDf.apply(self.functions.change_type, axis = 1)
        self.resultCipherCodeDf['Тип'] = self.resultCipherCodeDf.apply(self.functions.change_type, axis = 1)

    @staticmethod
    def _creatingSummaryTable(self) -> pd.DataFrame:
        Documentation.StatusLogger.info('  Creating a summary table.')
        partDf = self.cipherCodeDf[self.cipherCodeDf['_merge'] == 'left_only'][self.docDatabase.columns]
        summaryDf = self.resultCipherDf[list(self.columns.CipherDfColumns)]
        summaryDf.columns = self.docDatabase.columns
        summaryDf = pd.concat([partDf, summaryDf])
        partDf = self.resultCipherCodeDf.copy()
        partDf['Новый шифр'] = partDf.apply(self.functions.change_code, axis = 1)
        partDf = partDf[list(self.columns.CipherCodeDfColumns)]
        partDf.columns = self.docDatabase.columns
        summaryDf = pd.concat([partDf, summaryDf])
        return summaryDf

    @staticmethod
    def _preparingFinalFileAndWritingToDatabase(self, summaryDf:pd.DataFrame) -> pd.DataFrame:
        Documentation.StatusLogger.info('  Preparing the final file and writing it to the database.')
        summaryDf = pd.concat([summaryDf, self.empty_rows_df]).sort_index()
        summaryDf['Статус'] = summaryDf.apply(self.functions.change_status, axis = 1)
        for column in self.columns.noneColumns:
            summaryDf[column] = summaryDf.apply(lambda df: self.functions.change_none(df, column), axis = 1)
        return summaryDf

    @staticmethod
    def _makingChangedToDatabase(self, summaryDf:pd.DataFrame) -> None:
        Documentation.StatusLogger.info('  Making changes to the database.')
        attempt = PostProcessing(databaseRoot, self.databaseName)
        # attempt.delete_table()
        # attempt.create_table()
        # if attempt.insert_into_table(summaryDf):
        #     isSuccessUpdatedDocumentation = True


class Status:
        
    logger.remove()
    StatusLogger = logger.bind(name = 'StatusLogger').opt(colors = True)
    StatusLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self):
        Status.StatusLogger.info('Working on Documentation.')
        self.databaseName = 'Документация'
        connect = Preproccessing(databaseRoot, excelRoot)
        self.statusDf, self.docDf = connect.to_database('Переданные_РД', self.databaseName, True)
        self.functions = Functions.Status()
        self.result = ResultFiles('Статус')
    
    def done(self) -> None:
        if isSuccessUpdatedRD and isSuccessUpdatedDocumentation:
            self._preparingDataForMerging(self)
            self._mergingTwoDataframes(self)
            summaryDf = self._preparingSummaryDataframe(self)
            self._makingChangesToDatabase(self, summaryDf)
            Status.StatusLogger.info('  Database updated.')


    @staticmethod
    def _preparingDataForMerging(self) -> None:
        Status.StatusLogger.info('  Preparing data for merging.')
        self.statusDf['Ревизия'] = self.statusDf['Ревизия'].apply(lambda row: row  if pd.isna(row) or row == 0 else f'C0{row}')
        self.docDf['Ревизия_новая'] = self.docDf['Ревизия'].apply(lambda row: row[:3]  if '(есть только в 1С)' in str(row) else row)
    
    @staticmethod
    def _mergingTwoDataframes(self) -> None:
        Status.StatusLogger.info('  Merging two databases.')
        self.mergedDf = pd.merge(self.docDf, self.statusDf,
                            how = 'outer',
                            left_on = ['Шифр', 'Ревизия_новая'],
                            right_on = ['Шифр', 'Ревизия'],
                            suffixes = ['', '_new'],
                            indicator = True)
        self.mergedDf['Сервер'] = self.mergedDf.apply(self.functions.get_status_server, axis = 1)

    @staticmethod
    def _preparingSummaryDataframe(self) -> pd.DataFrame:
        Status.StatusLogger.info('  Preparing summary dataframe.')
        summaryDf = self.mergedDf[self.mergedDf['_merge'].isin(['both', 'left_only'])]
        summaryDf = summaryDf[self.docDf.columns[:-1]]
        return summaryDf

    @staticmethod
    def _makingChangesToDatabase(self, summaryDf:pd.DataFrame) -> None:
        Status.StatusLogger.info('  Making changes to the database.')
        attempt = PostProcessing(databaseRoot, self.databaseName)
        attempt.delete_table()
        attempt.create_table()
        if attempt.insert_into_table(summaryDf):
            isSuccessUpdatedStatus = True


        
    