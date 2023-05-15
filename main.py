import sys

from loguru import logger
from processing import Preproccessing, PostProcessing, ResultFiles
from tkinter import askopenfilename
from updatingTables import Tables


mainLogger = logger.bind(name = 'mainLogger').opt(colors = True)
mainLogger.add(sink = sys.stdout, format ='<green> {time:HH:MM:SS} </green> {message}', level = 'INFO')

databaseRoot = askopenfilename(title='Select database', filetypes=[('*.mdb', '*.accdb')]).replace('/', '\\')
excelRoot = askopenfilename(title="Select file for compare", filetypes=[("Excel Files", "*.xlsx"), ("Excel Binary Workbook", "*.xlsb")])
xlsbFind = True if '.xlsb' in excelRoot else False

tablename = 'РД'

mainLogger.info(f'Working on table {tablename}.')
connect= Preproccessing(databaseRoot, excelRoot)
connect.to_database(tablename)
