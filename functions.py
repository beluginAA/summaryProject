import sys
import columns
import pandas as pd

from loguru import logger


class Functions:

    class RD:

        def changing_code(self, df:pd.DataFrame) -> str:
            date_expected, date_release = df['Код KKS документа'], df['Код KKS документа_new']
            if  not pd.isna(date_expected) and not pd.isna(date_release):
                return f'Смена кода с <{date_expected}> на <{date_release}>'
            else:
                return '-'
            
        def finding_empty_rows(self, df:pd.DataFrame, column:str) -> str:
            if df[column] in ['nan', 'None', '0'] or pd.isna(df[column]):
                return ''
            else:
                return df[column]

        def changing_name(self, df:pd.DataFrame) -> str:
            date_expected, date_release = df['Наименование объекта/комплекта РД'], df['Наименование объекта/комплекта РД_new']
            if  not pd.isna(date_expected) and not pd.isna(date_release) and date_expected != date_release:
                return date_release
            else:
                return date_expected

        def changing_developer(self, df:pd.DataFrame) -> str:
            if ~pd.isna(df['Разработчики РД (актуальные)']):
                return df['Разработчик РД']
            else:
                return df['Разработчики РД (актуальные)']

        def changing_status_for_name(self, df:pd.DataFrame) -> str:
            if isinstance(df['Статус текущей ревизии_new'], float) or df['Статус текущей ревизии_new'] is None:
                return df['Статус РД в 1С']
            else:
                return df['Статус текущей ревизии_new']
        
        def changing_status_for_kks(self, df:pd.DataFrame) -> str:
            if isinstance(df['Статус текущей ревизии'], float) or df['Статус текущей ревизии'] is None:
                return df['Статус РД в 1С']
            else:
                return df['Статус текущей ревизии']

        def changing_data(self, df:pd.DataFrame, column:str) -> str:
            if isinstance(df[column], float) or df[column] is None:
                df[column] = ''
            if isinstance(df[f'{column}_new'], float) or df[f'{column}_new'] is None:
                df[f'{column}_new'] = ''
            if (df[column] == df[f'{column}_new']) or (pd.isna(df[column]) and pd.isna(df[f'{column}_new'])):
                return None
            else:
                return f'Смена {column.lower()} с <{df[column]}> на <{df[f"{column}_new"]}>'
            
        def changing_wbs(self, row:str) -> str:
            split_row = row.split()
            if len(split_row) > 1:
                if split_row[0].upper() == split_row[0] and split_row[1] == '-':
                    return split_row[0]
                else:
                    return row
            else:
                return row

        def missed_codes(self, df:pd.DataFrame, anotherDf:pd.DataFrame) -> str:
            if df['Коды работ по выпуску РД'] not in list(anotherDf['Коды работ по выпуску РД']):
                return df
            else:
                return None

        def missed_codes_excel(self, df:pd.DataFrame, anotherDf:pd.DataFrame) -> str:
            if df['Коды работ по выпуску РД'] not in list(anotherDf['Коды работ по выпуску РД']):
                return df
            else:
                return None

        def find_row(self, row:str) -> str:
            if 'Смена' in row:
                return row
            else:
                return '-'
    

    class Documentation:

        def prepare_missed_rows(self, docDf:pd.DataFrame, rdDf:pd.DataFrame) -> pd.DataFrame:

            logger.remove()
            missedRowsLogger = logger.bind(name = 'missed_rows_logger').opt(colors = True)
            missedRowsLogger.add(sink = sys.stdout, format =  "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

            missedRowsLogger.info("  Finding information with missing data")
            cipherDf = pd.merge(docDf, rdDf,
                                    how = 'outer',
                                    on = 'Шифр',
                                    suffixes=['', '_new'],
                                    indicator = True)
            
            pathDf = cipherDf[cipherDf['_merge'] == 'right_only'][columns.Documentation.pathDfColumns]
            pathDf.columns = columns.Documentation.missedColumnsNew

            cipherCodeDf = pd.merge(docDf, pathDf,
                                how = 'outer',
                                left_on = 'Шифр',
                                right_on = 'Код',
                                suffixes=['', '_new'],
                                indicator=True)

            missedRows = cipherCodeDf[cipherCodeDf['_merge'] == 'right_only'][columns.Documentation.missedColumns]
            missedRows = missedRows.loc[missedRows['Система_new'].isin(list(set(docDf['Система'])))]
            missedRows = missedRows.dropna(subset = ['Система_new'])
            missedRows.columns = columns.Documentation.missedColumnsNew
            missedRowsLogger.info('  Missing value search finished')
            return missedRows

        def prepare_data_for_logfile(self, cipDf:pd.DataFrame, cipherCodeDf:pd.DataFrame) -> pd.DataFrame:

            logger.remove()
            logFileLogger = logger.bind(name = 'log_file_logger').opt(colors = True)
            logFileLogger.add(sink = sys.stdout, format =  "<green> {time:HH:mm:ss} </green> | {message}", level = "DEBUG")
            
            logFileLogger.info('  Preparing data for log-file')

            def change_type_new(df:pd.DataFrame) -> str:
                if pd.isna(df['Тип'])  and ~pd.isna(df['Тип_new']):
                    return f'Смена типа c на {df["Тип_new"]}'
                else:
                    return df['Тип'] 
            
            def change_status_new(df:pd.DataFrame) -> str:
                if pd.isna(df['Итог_статус']):
                    return 'Отсутствует'
                elif 'ВК+' in df['Итог_статус'] or 'Выдан в производство' in df['Итог_статус']:
                    return 'Утвержден'
                else:
                    return 'На согласовании'
                
            def change_columns(df:pd.DataFrame, column:str) -> str:
                if column[0] != f'{column[1]}_new':
                    if df[column[0]] == df[column[1]]:
                        return '-'
                    else:
                        if df[column[0]] is None and df[column[1]] in [None, '']:
                            return '-'
                        elif df[column[0]] == None:
                            return f'Смена {column[0].lower()} c на {df[column[1]]}'
                        else:
                            return f'Смена {column[0].lower()} c {df[column[0]]} на {df[column[1]]}'
                else:
                    if df[column[0]] == df[column[1]]:
                        return '-'
                    else:
                        return f'Смена {column[0].lower()} c {df[column[0]]} на {df[column[1]]}' 
            
            def change_code_new(df:pd.DataFrame, column:str) -> str:
                if df['Шифр_new'] != '':
                    return f'Смена шифра c {df[column[0]]} на {df[column[1]]}'
                else:
                    return '-'

            cipDf['Тип'] = cipDf.apply(change_type_new, axis = 1)
            cipherCodeDf['Тип'] = cipherCodeDf.apply(change_type_new, axis = 1)
            cipDf['Итог_статус'] = cipDf.apply(change_status_new, axis = 1)
            cipherCodeDf['Итог_статус'] = cipherCodeDf.apply(change_status_new, axis = 1)

            for column in columns.Documentation.changedColumns:
                if 'Код' not in column:
                    cipDf[column[0]] = cipDf.apply(lambda row: change_columns(row, column), axis=1)
                    cipherCodeDf[column[0]] = cipherCodeDf.apply(lambda row: change_columns(row, column), axis=1)
                else:
                    cipDf['Шифр'] = '-'
                    cipherCodeDf['Шифр'] = cipherCodeDf.apply(lambda row: change_code_new(row, column), axis = 1)
            logDf = pd.concat([cipDf[list(columns.Documentation.logFileColumns)], cipherCodeDf[list(columns.Documentation.logFileColumns)]])
            logDf = logDf.reset_index()[list(columns.Documentation.logFileColumns)]

            logFileLogger.info('  Log-file ready')
            return logDf


        def change_code(self, df:pd.DataFrame) -> str:
            if df['Шифр_new'] != '':
                return df['Шифр_new']
            else:
                return df['Код']
        
        def finding_empty_rows(self, df:pd.DataFrame, column:str) -> str:
            if df[column] in ['nan', 'None', '0', None] or pd.isna(df[column]):
                return None
            else:
                return df[column]

        def change_status(self, df:pd.DataFrame) -> str:
            if pd.isna(df['Статус']):
                return 'Отсутствует'
            elif 'ВК+' in df['Статус'] or 'Выдан в производство' in df['Статус']:
                return 'Утвержден'
            else:
                return 'На согласовании'

        def change_type(self, df:pd.DataFrame) -> str:
            if  pd.isna(df['Тип']):
                return df['Тип_new']
            else:
                return df['Тип']
            
        def change_none(self, df:pd.DataFrame, column:str) -> str:
            if df[column] == 'None' or  df[column] is None:
                return ''
            else:
                return df[column]


    class Status:

        def get_status_server(self, df:pd.DataFrame) -> str:
            if df['_merge'] == 'both':
                return 'Выложен'
            else:
                return 'Не выложен'


