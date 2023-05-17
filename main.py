import sys

from loguru import logger
from tkinter.filedialog import askopenfilename
from updatingTables import RD

mainLogger = logger.bind(name = 'mainLogger').opt(colors = True)
mainLogger.add(sink = sys.stdout, format ='<green> {time:HH:MM:SS} </green> {message}', level = 'INFO')

start = RD()
start.updateRD()


