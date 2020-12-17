from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import traceback, sys
import queue
import random
import json

# Import siteparsers
from siteparser.amazon_parser import AmScraper
from siteparser.flipkart_parser import FkScraper

# Jinn Code here
import os
import numpy as np
import pandas as pd
import time

JINN_PATH = os.getcwd()

with open('CONFIG', 'r') as f:
    CONFIG = json.load(f)
    
def ArgBatch(batchfile):
    with open(batchfile, 'r') as f:
        search_terms = f.read().splitlines()
    
    argthread = BatchThread()
    argthread.search_terms = search_terms
    argthread.start()
        
    
#~~~~~~~~~~~~~~~~~~
# Single thread 
#~~~~~~~~~~~~~~~~~~

class CloneThread(QThread):
    signal = pyqtSignal('PyQt_PyObject')
    
    def __init__(self):
        QThread.__init__(self)
        self.search_term = ""
        
    def run(self):

        save_images = ui.saveImages.isChecked()
        print(self.search_term)
        
        if ui.scrapeAmazon.isChecked() == True:
            ui.aboutLabel.setText(f'Scraping Amazon: {self.search_term}')
            print(f'Scraping Amazon{self.search_term}')
            am = AmScraper()
            am.Scrape(self.search_term, save_images)
            print(f'Finished Amazon {self.search_term}')
            
        if ui.scrapeFlipkart.isChecked() == True:
            ui.aboutLabel.setText(f'Scraping Flipkart: {self.search_term}')
            print(f'Scraping Flipkart {self.search_term}')
            fk = FkScraper()
            fk.Scrape(self.search_term, save_images)
            print(f'Finished Flipkart {self.search_term}')
        
        self.signal.emit(f'Finished scraping: {self.search_term}')

#~~~~~~~~~~~~~~~~~~
# Batch thread 
#~~~~~~~~~~~~~~~~~~

class BatchThread(QThread):
    signal = pyqtSignal('PyQt_PyObject')
    
    def __init__(self):
        QThread.__init__(self)
        self.search_terms = ""
        
    def run(self):
        
        MAX_THREADS = CONFIG["MAX_THREADS"]
        theQueue = queue.Queue()
        
        pool = QThreadPool()
        pool.setMaxThreadCount(MAX_THREADS)
        
        for task in range(MAX_THREADS): 
            sig = Signal()
            sig.sig.connect(result_callback)
            pool.start(Worker(theQueue, sig))
        
        for i in self.search_terms: # Sending more values than there are threads
            theQueue.put(i)
            print('queue term:',i)

        # Tell the threads in the pool to finish
        for i in range(MAX_THREADS):
            theQueue.put(None)

        pool.waitForDone()
        self.signal.emit(f'Finished batch scraping')

#~~~~~~~~~~~~~~~~~~
# Batch Scraping 
#~~~~~~~~~~~~~~~~~~
# Signals
class Signal(QObject):
    sig = pyqtSignal('PyQt_PyObject')

# Worker thread
class Worker(QRunnable):
    def __init__(self, theQueue, sig):
        QRunnable.__init__(self)
        self.theQueue = theQueue
        self.signal = sig

    def run(self):
        while True:
            search_term = self.theQueue.get()
            if search_term is None:
                self.theQueue.task_done()
                return
                
            # ~~~~~~ The task code ~~~~~~#
            save_images = ui.saveImages.isChecked()
            print("Scraping for search term:", search_term)
            
            if ui.scrapeAmazon.isChecked() == True:
                ui.aboutLabel.setText(f'Scraping Amazon: {search_term}')
                print(f'Scraping Amazon: {search_term}')
                am = AmScraper()
                am.Scrape(search_term, save_images)
                # time.sleep(random.randint(1,10))
                print(f'Finished Amazon: {search_term}')
                
            if ui.scrapeFlipkart.isChecked() == True:
                ui.aboutLabel.setText(f'Scraping Flipkart: {search_term}')
                print(f'Scraping Flipkart: {search_term}')
                fk = FkScraper()
                fk.Scrape(search_term, save_images)
                # time.sleep(random.randint(1,10))
                print(f'Finished Flipkart: {search_term}')            
                
            print(self.theQueue.unfinished_tasks, 'Remaining tasks')
            # ~~~~~~~~~~~~~~ #
            self.signal.sig.emit(search_term)
            self.theQueue.task_done()

def result_callback(result):
    print("Got {}".format(result))

    
#-------------------------------------------------------------------------#

class Ui_Dialog(QMainWindow):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        # Dialog.resize(480, 160)
        Dialog.setFixedSize(563, 195)
        Dialog.setWindowTitle("Jinn")
        Dialog.setStyleSheet("background-color: #dddddd;font-family:Arial; color:#444444")
        self.gridLayout = QtWidgets.QGridLayout(Dialog)
        self.gridLayout.setObjectName("gridLayout")

        # Add Logo 
        self.label = QLabel()
        pixmap = QPixmap('Jinn.png')
        self.label.setPixmap(pixmap)
        self.gridLayout.addWidget(self.label, 0, 3, 3, 1)

        # Create Plain Text Box
        self.plainTextEdit = QtWidgets.QPlainTextEdit(Dialog)
        self.plainTextEdit.setObjectName("plainTextEdit")
        self.plainTextEdit.setStyleSheet("background-color: white")
        self.gridLayout.addWidget(self.plainTextEdit, 0, 0, 2, 2)

        # Scrape Product Info Button
        self.scrapeButton = QPushButton('Scrape Product Info')
        self.scrapeButton.setObjectName("BrowseFile")
        self.scrapeButton.setStyleSheet("font-weight:bold; height: 50%")
        self.gridLayout.addWidget(self.scrapeButton, 0, 2, 1, 1)
        self.scrapeButton.clicked.connect(self.start_scrape)
        self.scrape_thread = CloneThread()
        self.scrape_thread.signal.connect(self.finished)

        # Batch Scrape Button
        self.batchButton = QPushButton('Batch Scrape')
        self.batchButton.setObjectName("batchButton")
        self.batchButton.setStyleSheet("font-weight:bold;  height: 50%")
        self.gridLayout.addWidget(self.batchButton, 1, 2, 1, 1)        
        self.batchButton.clicked.connect(self.start_batch)
        self.batch_thread = BatchThread()
        self.batch_thread.signal.connect(self.finish_batch)
                


        # Add About label
        self.aboutLabel = QLabel()
        self.aboutLabel.setText("Select the websites you want to scrape")
        self.aboutLabel.setStyleSheet("color: #555555")
        self.gridLayout.addWidget(self.aboutLabel, 3, 0, 1, 2)

        # Scrape Flipkart Check Box
        self.scrapeFlipkart = QCheckBox("Flipkart")
        self.scrapeFlipkart.setChecked(True)
        self.gridLayout.addWidget(self.scrapeFlipkart , 2, 0, 1, 1)

        
        # Scrape Amazon Check Box
        self.scrapeAmazon = QCheckBox("Amazon")
        self.scrapeAmazon.setChecked(True)
        self.gridLayout.addWidget(self.scrapeAmazon , 2, 1, 1, 1)

        # Save Product Images Check Box
        self.saveImages = QCheckBox("Save Product Images")
        self.saveImages.setChecked(False)
        self.gridLayout.addWidget(self.saveImages , 2, 2, 1, 1)
        
        # Multithreading section        
    
    # def launch_threads(self):
        # search_terms = self.openCsvFilesDialog()
        
        # MAX_THREADS = 2
        # theQueue = queue.Queue()
        
        # pool = QThreadPool()
        # pool.setMaxThreadCount(MAX_THREADS)
        
        # for task in range(MAX_THREADS): 
            # sig = Signal()
            # sig.sig.connect(result_callback)
            # pool.start(Worker(theQueue, sig))
        
        # for i in search_terms: # Sending more values than there are threads
            # theQueue.put(i)

        # # Tell the threads in the pool to finish
        # for i in range(MAX_THREADS):
            # theQueue.put(None)

        # pool.waitForDone()
        
        
    def start_scrape(self):
        self.scrape_thread.search_term = self.plainTextEdit.toPlainText()
        self.scrapeButton.setEnabled(False)
        self.aboutLabel.setText("Started the thread")
        self.scrape_thread.start()
        
    def finished(self, result):
        self.aboutLabel.setText(f"Done. {result}")
        self.scrapeButton.setEnabled(True)
        
    def start_batch(self):
        files, _ = QFileDialog.getOpenFileNames(QFileDialog(), "Open txt files", "", "TXT Files (*.TXT)")
        if files:
            with open(files[0],'r') as f:
                search_terms = f.read().splitlines()
        else:
            return
            
        self.batch_thread.search_terms = search_terms
        self.batchButton.setEnabled(False)
        self.aboutLabel.setText("Started the thread")
        self.batch_thread.start()
                
    def finish_batch(self, result):
        self.aboutLabel.setText(f"End of Batch. {result}")
        self.batchButton.setEnabled(True)
        
        
    # Batch Scraping Function
    def openCsvFilesDialog(self):
        files, _ = QFileDialog.getOpenFileNames(QFileDialog(), "Open txt files", "", "TXT Files (*.TXT)")

        if files:
            with open(files[0],'r') as f:
                search_terms = f.read().splitlines()
                return search_terms
            
            # self.threads = []
            # for search_term in search_terms:
                # print(search_term)
                # thread = CloneThread(search_term)
                # self.threads.append(thread)
                # thread.start()
                

# Stand alone code #
if __name__ == "__main__":
    import sys

        
    app = QtWidgets.QApplication(sys.argv)
    Dialog = QtWidgets.QDialog()
    ui = Ui_Dialog()
    ui.setupUi(Dialog)
    Dialog.setWindowFlags(
        Dialog.windowFlags()|
        QtCore.Qt.WindowMinimizeButtonHint |
        QtCore.Qt.WindowSystemMenuHint
    )
    
    # If batch is supplied as a argument, then run the batch search, Else run as an app
    if len(sys.argv) > 1:
        batchfile = (sys.argv[1])
        with open(batchfile, 'r') as f:
            search_terms = f.read().splitlines()
        
        argthread = BatchThread()
        argthread.search_terms = search_terms
        argthread.start()

    Dialog.show()
    sys.exit(app.exec_())
