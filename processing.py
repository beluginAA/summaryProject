import pandas as pd
import pyodbc
import sys
import pyxlsb
import columns
import os
import colorama
import xlsxwriter

from loguru import logger
from datetime import datetime


class Preproccessing:

    preLogger = logger.bind(name = 'preLogger').opt(colors = True)
    preLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self, databaseRoot:str, excelRoot:str):
        self.connStr = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                fr'DBQ={databaseRoot};'
                )
        self.excelRoot = excelRoot

    def to_database(self, databaseName:str) -> pd.DataFrame :
        Preproccessing.preLogger.info('Trying to connect to a database.')
        try:
            with pyodbc.connect(self.connStr) as cnxn:
                query = f'SELECT * FROM [{databaseName}]'
                msDatabase = pd.read_sql(query, cnxn)
        except Exception as e:
            Preproccessing.preLogger.error(f"An error occurred while connecting to the database {databaseName}: {e}")
        else:
            Preproccessing.preLogger.info(f'--The connection to the database {databaseName} was successful.--')
            msDatabase.columns = columns.base_columns
            return msDatabase
    
    def to_excel(self) -> pd.DataFrame:

        Preproccessing.preLogger.info('Trying to get the data from excel.')
        try:
            if '.xlsb' in self.excelRoot:
                with pyxlsb.open_workbook(self.excelRoot) as wb:
                    with wb.get_sheet(1) as sheet:
                        data = []
                        for row in sheet.rows():
                            data.append([item.v for item in row])
                excelDatabase = pd.DataFrame(data[1:], columns=data[0])
            else: 
                excelDatabase = pd.read_excel(self.excelRoot, engine='openpyxl')
        except Exception as e:
            Preproccessing.preLogger.error(f"An error occurred while retrieving data from Excel: {e}")
        else:
            Preproccessing.preLogger.info('--Data from excel received successfully.--')
            return excelDatabase
        


class PostProcessing:

    postLogger = logger.bind(name = 'postLogger').opt(colors = True)
    postLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self, databaseRoot:str):
        self.connStr = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                fr'DBQ={databaseRoot};'
                )
        self.isSuccessDeleteTable = False
        self.isSuccessCreateTable = False

    def delete_table(self, databaseName:str) -> None:
        PostProcessing.postLogger.info('Trying to delete an old table.')
        try:
            with pyodbc.connect(self.connStr) as connection:
                cursor = connection.cursor()
                cursor.execute(f"DROP TABLE [{databaseName}]")
                cursor.commit()
        except Exception as e:
            PostProcessing.postLogger.error(f"An error occurred while deleting the table {databaseName}: {e}")
        else:
            PostProcessing.postLogger.info(f'--An old table {databaseName} has been successfully deleted.--')
            self.isSuccessDeleteTable = True
    
    def create_table(self, databaseName:str) -> None:
        PostProcessing.postLogger.info('Trying to create a new table.')
        createTableQuery = f'''CREATE TABLE [{databaseName}] ([Система] VARCHAR(200), 
                                [Наименование] VARCHAR(200),
                                [Код] VARCHAR(200),
                                [Тип] VARCHAR(200),
                                [Пакет] VARCHAR(200),
                                [Шифр] VARCHAR(200),
                                [Итог_статус] VARCHAR(200),
                                [Ревизия] VARCHAR(200), 
                                [Рев_статус] VARCHAR(200), 
                                [Дата_план] VARCHAR(200), 
                                [Дата_граф] VARCHAR(200), 
                                [Рев_дата] VARCHAR(200), 
                                [Дата_ожид] VARCHAR(200), 
                                [Письмо] VARCHAR(200), 
                                [Источник] VARCHAR(200), 
                                [Разработчик] VARCHAR(200), 
                                [Объект] VARCHAR(200), 
                                [WBS] VARCHAR(200), 
                                [КС] VARCHAR(200), 
                                [Примечания] VARCHAR(200))'''
        try:
            with pyodbc.connect(self.connStr) as connection:
                cursor = connection.cursor()
                cursor.execute(createTableQuery)
                cursor.commit()
        except Exception as e:
            PostProcessing.postLogger.error(f"An error occurred while creating the table {databaseName}: {e}")
        else:
            PostProcessing.postLogger.info(f'--An old table {databaseName} has been successfully created.--')
            self.isSuccessCreateTable = True
    
    def insert_into_table(self, databaseName:str, dataframe:pd.DataFrame) -> None:
        PostProcessing.postLogger.info('Trying to insert new data into new table.')
        if self.isSuccessCreateTable and self.isSuccessDeleteTable:
            try:
                with pyodbc.connect(self.connStr) as connection:
                    cursor = connection.cursor()
                    for row in dataframe.itertuples(index=False):
                        insertQuery = f'''INSERT INTO [{databaseName}] ([Система], [Наименование], [Код], 
                                                            [Тип], [Пакет], [Шифр], 
                                                            [Итог_статус], [Ревизия], [Рев_статус], 
                                                            [Дата_план], [Дата_граф], [Рев_дата], 
                                                            [Дата_ожид], [Письмо], [Источник], 
                                                            [Разработчик], [Объект], [WBS], 
                                                            [КС], [Примечания]) VALUES ({",".join(f"'{x}'" for x in row)})'''
                        cursor.execute(insertQuery)
                    cursor.commit()
            except Exception as e:
                PostProcessing.postLogger.error(f"An error occurred while inserting the data into table {databaseName}: {e}")
            else:
                PostProcessing.postLogger.info(f'--Data was successfully added to the table {databaseName}.--')



class ResultFiles:

    resultFileLogger = logger.bind(name = 'resultFileLogger').opt(colors = True)
    resultFileLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self):
        self.outputLogLileName = 'log-RD-' + str(datetime.now().isoformat(timespec='minutes')).replace(':', '_')
        self.outputResultFileName = 'result' + str(datetime.now().isoformat(timespec='minutes')).replace(':', '_')
    
    def to_logfile(self, dataframe:pd.DataFrame, header:str) -> None:
        ResultFiles.resultFileLogger.info('Trying to write data to log-file.')

        try:
            maxLenRow = [max(dataframe[row].apply(lambda x: len(str(x)) if x else 0)) for row in dataframe.columns]
            maxLenName = [len(row) for row in dataframe.columns]
            maxLen = [col_len if col_len > row_len else row_len for col_len, row_len in zip(maxLenName, maxLenRow)]
            with open(f'{self.outputLogLileName}.txt', 'a',  encoding='cp1251') as logFile:
                logFile.write(f'{header}:\n')
                logFile.write('\n')
                fileWrite = ' ' * (len(str(dataframe.index.max())) + 3)
                for column, col_len in zip(dataframe.columns, maxLen):
                    fileWrite += f"{column:<{col_len}}|"
                logFile.write(fileWrite)
                logFile.write('\n')
                for index, row in dataframe.iterrows():
                    columnValue = ''
                    for i in range(len(dataframe.columns)):
                        columnValue += f"{str(row[dataframe.columns[i]]) if row[dataframe.columns[i]] else '-':<{maxLen[i]}}|"
                    logFile.write(f"{index: <{len(str(dataframe.index.max()))}} | {columnValue}\n")
                logFile.write('\n')
        except Exception as e:
            ResultFiles.resultFileLogger.error(f"An error occurred while writing data to log-file: {e}")
        else:
            ResultFiles.resultFileLogger.info('--Writing data to log-file was successful.--')
    
    def to_resultfile(self, dataframe:pd.DataFrame) -> None:
        ResultFiles.resultFileLogger.info('Trying to write the final data to an excel file.')

        comfRen = input('Use standard file name (y/n): ')
        while comfRen not in 'YyNn':
            comfRen = input('For next work choose <y> or <n> simbols): ')
        if comfRen not in 'Yy':
            self.outputResultFileName = input('Input result file name: ')
        try:
            dataframe.to_excel(f'./{self.outputResultFileName}.xlsx', index = False)
            styler = dataframe.style
            styler.set_properties(**{'border': '1px solid black', 'border-collapse': 'collapse'})
            writer = pd.ExcelWriter(f'./{self.outputResultFileName}.xlsx', engine='xlsxwriter')
            styler.to_excel(writer, sheet_name='Итоговый результат', encoding='cp1251', index=False)
            workbook = writer.book
            worksheet = writer.sheets['Итоговый результат']
            worksheet.autofilter(0, 0, len(dataframe.index), len(dataframe.columns) - 1)
            for i, column in enumerate(dataframe.columns):
                column_width = max(dataframe[column].astype(str).map(len).max(), len(column))
                worksheet.set_column(i, i, column_width)
            writer._save()
        except Exception as e:
            ResultFiles.resultFileLogger.error(f"An error occurred while writing the final data to an excel file: {e}")
        else:
            ResultFiles.resultFileLogger.info('Writing data to excel file was successful.')