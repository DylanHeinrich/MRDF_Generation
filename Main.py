'''
TODO List:
- Add a Field called 2D Bardcode to the final sorted list.
    -Make sure that nothing else gets changed in the data list
- Build out the UI 
- Do couple different test
- Check for bugs

'''

import csv
import pandas as pd
from datetime import datetime
import queue
import logging
import signal
import time
import threading
import ctypes
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk, VERTICAL, HORIZONTAL, N, S, E, W, filedialog
import ttkbootstrap as ttk
import os
import numpy as np
import codecs


PROGRAM_LOCATION = os.getcwd()

logger = logging.getLogger(__name__)

csv_path = None

JobID = None
PieceID = None
PlannedMailPieceCount = None

t1 = None

class QueueHandler(logging.Handler):
    """Class to send logging records to a queue

    It can be used from different threads
    The ConsoleUi class polls this queue to display records in a ScrolledText widget
    """
    # Example from Moshe Kaplan: https://gist.github.com/moshekaplan/c425f861de7bbf28ef06
    # (https://stackoverflow.com/questions/13318742/python-logging-to-tkinter-text-widget) is not thread safe!
    # See https://stackoverflow.com/questions/43909849/tkinter-python-crashes-on-new-thread-trying-to-log-on-main-thread

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(record)


class ConsoleUi:
    """Poll messages from a logging queue and display them in a scrolled text widget"""

    def __init__(self, frame):
        self.frame = frame
        # Create a ScrolledText wdiget
        self.scrolled_text = ScrolledText(frame, state='disabled', height=12)
        self.scrolled_text.grid(row=0, column=0, sticky=(N, S, W, E)) # type: ignore
        self.scrolled_text.configure(font='TkFixedFont')
        self.scrolled_text.tag_config('INFO', foreground='black')
        self.scrolled_text.tag_config('DEBUG', foreground='gray')
        self.scrolled_text.tag_config('WARNING', foreground='orange')
        self.scrolled_text.tag_config('ERROR', foreground='red')
        self.scrolled_text.tag_config('CRITICAL', foreground='red', underline=True)
        # Create a logging handler using a queue
        self.log_queue = queue.Queue()
        self.queue_handler = QueueHandler(self.log_queue)
        formatter = logging.Formatter('%(asctime)s: %(message)s', '%m/%d/%Y %H:%M:%S')
        self.queue_handler.setFormatter(formatter)
        logger.addHandler(self.queue_handler)
        # Start polling messages from the queue
        self.frame.after(100, self.poll_log_queue)

    def display(self, record):
        msg = self.queue_handler.format(record)
        self.scrolled_text.configure(state='normal')
        self.scrolled_text.insert(tk.END, msg + '\n', record.levelname)
        self.scrolled_text.configure(state='disabled')
        # Autoscroll to the bottom
        self.scrolled_text.yview(tk.END)

    def poll_log_queue(self):
        # Check every 100ms if there is a new message in the queue to display
        while True:
            try:
                record = self.log_queue.get(block=False)
            except queue.Empty:
                break
            else:
                self.display(record)
        self.frame.after(100, self.poll_log_queue)


class MainUi:

    def __init__(self, frame):
        self.frame = frame
        self.landingLocationLabel = tk.Label(self.frame, text= 'Select sorted list: ', width= 50, height=1, fg='white', bg='gray')
        ttk.Button(self.frame, text='Sorted File', width=25, command= lambda: self.browseFolder(self.landingLocationLabel), bootstyle = 'outline').pack(pady=5)
        ttk.Button(self.frame, text='Generate', command=self.generate_File, bootstyle = 'outline').pack(pady= 5)
        
    def browseFolder(self, label):
        global csv_path
        self.csv_path = filedialog.askopenfilename(title='File Location', filetypes = [('CSV', '*.csv')])
        logger.log(logging.INFO, msg = f'Path Selected = {self.csv_path}')
        label.configure(text= self.csv_path)
        csv_path = self.csv_path

    def generate_File(self):
        start_generation()


class App:

    def __init__(self, root):
        self.root = root
        if os.path.exists(f'{PROGRAM_LOCATION}/myapp.conf'):
            with open(f'{PROGRAM_LOCATION}/myapp.conf', 'r') as file:
                postion = file.read()
            file.close()
        self.root.geometry(postion)
        root.resizable(False, False)
        root.title('MRDF Generator')
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        # Create the panes and frames
        vertical_pane = ttk.PanedWindow(self.root, orient=VERTICAL)
        vertical_pane.grid(row=0, column=0, sticky="nsew")
        horizontal_pane = ttk.PanedWindow(vertical_pane, orient=HORIZONTAL)
        vertical_pane.add(horizontal_pane)
        console_frame = ttk.Labelframe(vertical_pane, text="Console")
        console_frame.columnconfigure(0, weight=1)
        console_frame.rowconfigure(0, weight=1)
        vertical_pane.add(console_frame, weight=1)
        main_frame = ttk.Labelframe(horizontal_pane, text="Generator")
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(0,weight=1)
        horizontal_pane.add(main_frame, weight=1)
        # Initialize all frames
        self.form = MainUi(main_frame)
        self.console = ConsoleUi(console_frame)
        self.root.protocol('WM_DELETE_WINDOW', self.quit)
        self.root.bind('<Control-q>', self.quit)
        signal.signal(signal.SIGINT, self.quit)

    def quit(self, *args):
        self.root.destroy()

class mrdf_thread_with_exception(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
             
    def run(self):
 
        # target function of the thread class
        try:
            while True:
                main_generator()
        finally:
            pass
          
    def get_id(self):
 
        # returns id of the respective thread
        if hasattr(self, '_thread_id'):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                return id
  
    def raise_exception(self):
        thread_id = self.get_id()
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id,
              ctypes.py_object(SystemExit))
        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
            print('Exception raise failure')


def remove_non_utf8(input_file, output_file): 
    with open(input_file, 'r',  encoding='unicode_escape') as input_csv, open(output_file, 'w', encoding='utf-8', newline= '') as output_csv: 
        reader = csv.reader(input_csv) 
        writer = csv.writer(output_csv) 
        rows = []
        for row in reader: 
            cleaned_row = [cell.encode('utf-8', 'ignore').decode('unicode_escape') for cell in row] 
            rows.append(cleaned_row) 
        writer.writerows(rows)


def main_generator():
    global csv_path, JobID, PieceID, PlannedMailPieceCount

    path_location = os.path.dirname(csv_path)


    csv_file_name = str(os.path.basename(csv_path)).split('.', 1)[0]
    orderId = csv_file_name.split('_',1)[0]

    MRDF = open(f'{path_location}/{orderId}.txt','a', errors = 'replace')

    output = f'{path_location}/{csv_file_name}.csv'

    with open(output, encoding= 'utf-8', errors = 'replace') as f:
        csv_input = pd.read_csv(f, dtype ='str')

    if 'numb_piece' not in csv_input.columns and 'order_numb' in csv_input.columns:
        if 'first' in csv_input.columns:
            try:
                csvFile = pd.read_csv(output, header=0, usecols= ["first", "last", "company","first2", "address", "address2", "city", "st","order_numb"], encoding='utf-8', encoding_errors='ignore')
            except Exception as e:
                logger.log(logging.ERROR, msg = f'{e}')
                logger.log(logging.ERROR, msg='Could not find the first or company field name. Did you choose the right file.')
                t1.raise_exception()
                t1.join()
        elif 'company' in csv_input.columns:
            try:
                csvFile = pd.read_csv(csv_path, header=0, usecols= ["company", "address", "address2", "city", "st","order_numb"])
            except:
                logger.log(logging.ERROR, msg='Could not find the first or company field name. Did you choose the right file.')
                t1.raise_exception()
                t1.join()
    elif 'order_numb' in csv_input.columns:
        if 'first' in csv_input.columns:
            try:
                csvFile = pd.read_csv(csv_path, header=0, usecols= ["first", "last", "company","first2", "address", "address2", "city", "st","order_numb", "numb_piece"])
            except:
                logger.log(logging.ERROR, msg='Could not find the first or company field name. Did you choose the right file.')
                t1.raise_exception()
                t1.join()
        elif 'company' in csv_input.columns:
            try:
                csvFile = pd.read_csv(csv_path, header=0, usecols= ["company", "address", "address2", "city", "st","order_numb", "numb_piece"])
            except:
                logger.log(logging.ERROR, msg='Could not find first or company field name. Did you choose the right file.')
                t1.raise_exception()
                t1.join()
    else:
        if 'first' in csv_input.columns:
            try:
                csvFile = pd.read_csv(csv_path, header=0, usecols= ["first", "last", "address", "address2", "city", "st"])
            except:
                logger.log(logging.ERROR, msg='Could not find the first or company field name. Did you choose the right file.')
                t1.raise_exception()
                t1.join()
        elif 'company' in csv_input.columns:
            try:
                csvFile = pd.read_csv(csv_path, header=0, usecols= ["company", "address", "address2", "city", "st","order_numb"])
            except:
                logger.log(logging.ERROR, msg='Could not find first or company field name. Did you choose the right file.')
                t1.raise_exception()
                t1.join()
    

    total_number_records = len(csvFile.index)
    
    headerrow = create_header_row(orderId, total_number_records)
    MRDF.write(headerrow + '\n')

    if 'numb_piece' not in csv_input.columns:
        single_generation(csvFile, orderId, MRDF, csv_input, path_location, csv_file_name, total_number_records)
    else:
        variable_generation(csvFile, orderId, MRDF, csv_input, path_location, csv_file_name, total_number_records)


    MRDF.close()
    logger.log(logging.INFO, msg= f'MRDF and the new csv have been generated\nLocation: {path_location}')
    t1.raise_exception()
    t1.join()

def single_generation(csv_pd, order, mrdf, csv_input, file_location, file_name, total):
    csvFile = csv_pd
    barcode_row = []
    barcodeHR_row = []

    record_number = 1

    #print(csvFile.head())

    for index, row in csvFile.iterrows():
        if 'order_numb' in row:
            if 'first' in row:
                last = row['last']
                if row['last'] != None:
                    last = str(row['last']).lower()
                    first = row['first']
                    if last == 'nan':
                        row['first'] = f'{first}'
                    else:
                        row['first'] = f'{first} {last}'
                if 'address2' in row:
                    if 'nan' in str(row['address2']):
                        row['address2'] = ''
                if row['first'] == None and row['company'] != None:
                    row['company'] = str(row['company'])[0:40]
                    row['address'] = str(row['address'])[0:40]
                    row['address2'] = str(row['address2'])[0:40]
                    recordRow = row["company"], row['address'], row['address2'], row['city'], row['st'], row['order_numb']
                else:
                    row['first'] = str(row['first'])[0:40]
                    row['address'] = str(row['address'])[0:40]
                    row['address2'] = str(row['address2'])[0:40]
                    recordRow = row["first"], row['address'], row['address2'], row['city'], row['st'], row['order_numb']
            elif 'company' in row:
                row['company'] = str(row['company'])[0:40]
                row['address'] = str(row['address'])[0:40]
                row['address2'] = str(row['address2'])[0:40]
                recordRow = row["company"], row['address'], row['address2'], row['city'], row['st'], row['order_numb']

        numb_pieces = 1
        finish_record = create_record_row(recordRow, order, record_number, numb_pieces)

        mrdf.write(finish_record + '\n')
        
        barcode = f'{JobID}{PieceID}0101'
        barcodeHR = f'JobID {JobID}, PieceID {PieceID}, Page {record_number} of {total}'
        barcode = f'{barcode}'.ljust(27, '0')
        barcode_row.append(barcode)
        barcodeHR_row.append(barcodeHR)


        record_number += 1

    csv_input['2DBarcode'] = barcode_row
    csv_input['2DBarcodeHR'] = barcodeHR_row
    csv_input.to_csv(f'{file_location}/{file_name}_2DBarcode.csv', index=False, quoting = csv.QUOTE_ALL)

def variable_generation(csv_pd, order, mrdf, csv_input, file_location, file_name, total):
    csvFile = csv_pd
    barcode_row = []
    barcodeHR_row = []

    record_number = 1

    #print(csvFile.head())
    
    for index, row in csvFile.iterrows():
        if 'order_numb' in row:
            if 'first' in row:
                if row['last'] != None:
                    last = str(row['last']).lower()
                    first = row['first']
                    last = row['last']
                    if last == 'nan':
                        row['first'] = f'{first}'
                    else:
                        row['first'] = f'{first} {last}'
                if 'address2' in row:
                    if 'nan' in str(row['address2']):
                        row['address2'] = ''
                if row['first'] == None and row['company'] != None:
                    row['company'] = str(row['company'])[0:40]
                    recordRow = row["company"], row['address'], row['address2'], row['city'], row['st'], row['order_numb'], row['numb_piece']
                else:
                    row['first'] = str(row['first'])[0:40]
                    recordRow = row["first"], row['address'], row['address2'], row['city'], row['st'], row['order_numb'], row['numb_piece']
            elif 'company' in row:
                row['company'] = str(row['company'])[0:40]
                recordRow = row["company"], row['address'], row['address2'], row['city'], row['st'], row['order_numb'], row['numb_piece']


        finish_record = create_record_row(recordRow, order, record_number, row['numb_piece'])

        mrdf.write(finish_record + '\n')
        
        barcode = f'{JobID}{PieceID}0101'
        barcodeHR = f'JobID {JobID}, PieceID {PieceID}, Page {record_number} of {total}'
        barcode = f'{barcode}'.ljust(27, '0')
        barcode_row.append(barcode)
        barcodeHR_row.append(barcodeHR)


        record_number += 1

    csv_input['2DBarcode'] = barcode_row
    csv_input['2DBarcodeHR'] = barcodeHR_row

    csv_input_new = pd.DataFrame(columns = csv_input.columns)
    for row in csv_input.values:
        number_peices = row[0]
        new_df = pd.DataFrame([row] * number_peices, columns = csv_input.columns)
        csv_input_new = pd.concat([csv_input_new, new_df], join = 'inner')


    csv_input_new.to_csv(f'{file_location}/{file_name}_2DBarcode.csv', index=False)

def start_generation():
    global t1
    msg = 'Generating MRDF and CSV files'
    logger.log(logging.INFO, msg=msg)
    t1 = mrdf_thread_with_exception('MRDF Generator')
    t1.start()

def create_header_row(orderId, record_total):
    global PlannedMailPieceCount

    creatationDate = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
    cycleID = datetime.now().strftime('%m_%d')

    JobID = str(orderId[-6:]).ljust(6, ' ')
    RunID = str(JobID).ljust(15, ' ')
    CreationDate = f'{creatationDate}'.ljust(19, ' ')
    GroupID = ''.ljust(15)
    JobType = 'CHECK1'.ljust(15, ' ')
    ApplicationID = 'ShippingMgr'.ljust(15, ' ')
    CycleID = f'{cycleID}'.ljust(5, ' ')
    ClientID = ''.ljust(15, ' ')
    SLADueDate = ''.ljust(19, ' ')
    SLAWarningOffset = ''.ljust(5,'0')
    ReprintDueDate = ''.ljust(19, ' ')
    ReprintWarningOffset = ''.ljust(5,'0')
    DataSetType = '2'.rjust(1, '0')
    PlannedMailPieceCount = f'{record_total}'.rjust(6,'0')
    PlannedSheetCount = f'{record_total}'.rjust(10,'0')
    InserterMode = ''.ljust(8, ' ')
    InserFeeder01Mode = '1'
    InserFeeder02Mode = '1'
    InserFeeder03Mode = '1'
    InserFeeder04Mode = '1'
    InserFeeder05Mode = '1'
    InserFeeder06Mode = '1'
    InserFeeder07Mode = '1'
    InserFeeder08Mode = '1'
    InserFeeder09Mode = '1'
    InserFeeder10Mode = '1'
    InserFeeder11Mode = '1'
    InserFeeder12Mode = '1'
    EnvelopeFeederDocID = ''.ljust(15, ' ')
    InserFeeder01DocID = ''.ljust(15, ' ')
    InserFeeder02DocID = ''.ljust(15, ' ')
    InserFeeder03DocID = ''.ljust(15, ' ')
    InserFeeder04DocID = ''.ljust(15, ' ')
    InserFeeder05DocID = ''.ljust(15, ' ')
    InserFeeder06DocID = ''.ljust(15, ' ')
    InserFeeder07DocID = ''.ljust(15, ' ')
    InserFeeder08DocID = ''.ljust(15, ' ')
    InserFeeder09DocID = ''.ljust(15, ' ')
    InserFeeder10DocID = ''.ljust(15, ' ')
    InserFeeder11DocID = ''.ljust(15, ' ')
    InserFeeder12DocID = ''.ljust(15, ' ')
    PrintJobName = ''.ljust(15, ' ')
    BREPrintJobName = ''.ljust(15, ' ')
    UserDefinedField1 = ''.ljust(30, ' ')
    UserDefinedField2 = ''.ljust(30, ' ')
    Filler = ''.ljust(438, ' ')
    EndOfRecordIndicator = 'X'



    #csvfile = csv.reader(open(f'{orderId}.csv', 'r'))
    headerrow = (JobID+RunID+CreationDate+GroupID+JobType+ApplicationID+CycleID+ClientID+
                SLADueDate+SLAWarningOffset+ReprintDueDate+ReprintWarningOffset+DataSetType+
                PlannedMailPieceCount+PlannedSheetCount+InserterMode+InserFeeder01Mode+
                InserFeeder02Mode+InserFeeder03Mode+InserFeeder04Mode+InserFeeder05Mode+
                InserFeeder06Mode+InserFeeder07Mode+InserFeeder08Mode+InserFeeder09Mode+
                InserFeeder10Mode+InserFeeder11Mode+InserFeeder12Mode+EnvelopeFeederDocID+
                InserFeeder01DocID+InserFeeder02DocID+InserFeeder03DocID+InserFeeder04DocID+
                InserFeeder05DocID+InserFeeder06DocID+InserFeeder07DocID+InserFeeder08DocID+
                InserFeeder09DocID+InserFeeder10DocID+InserFeeder11DocID+InserFeeder12DocID+
                PrintJobName+BREPrintJobName+UserDefinedField1+UserDefinedField2+Filler+
                EndOfRecordIndicator)
    

    return headerrow

def create_record_row(input_row, orderId, piece_number, number_of_pieces):
    global JobID, PieceID

    JobID = str(orderId[-6:]).ljust(6, ' ')
    PieceID = f'{piece_number}'.rjust(6, '0')
    TotalSheetsInputFdr1 = f'{number_of_pieces}'.rjust(2, '0')
    TotalSheetsInputFdr2 = f''.rjust(2, '0')
    SubsetSheetNumInptFdr1 = f''.rjust(2, '0')
    SubsetSheetNumInptFdr2 = f''.rjust(2, '0')
    AccountIdentifier = f'{orderId[-6:]}{input_row[5]}'.ljust(20, ' ')
    InsertFeed01 = '0'
    InsertFeed02 = '0'
    InsertFeed03 = '0'
    InsertFeed04 = '0'
    InsertFeed05 = '0'
    InsertFeed06 = '0'
    InsertFeed07 = '0'
    InsertFeed08 = '0'
    InsertFeed09 = '0'
    InsertFeed10 = '0'
    InsertFeed11 = '0'
    InsertFeed12 = '0'
    SelectiveAccessory1 = '0'
    SelectiveAccessory2 = '0'
    SelectiveAccessory3 = '0'
    SelectiveAccessory4 = '0'
    SelectiveAccessory5 = '0'
    SelectiveAccessory6 = '0'
    AccountPull = '0'
    QualityAudit = '0'
    AlertAndClear = '0'
    RecipientName = f'{input_row[0]}'.ljust(40, ' ') #Pull this from the sorted file
    RecipientAddress1 = f'{input_row[1]}'.ljust(40, ' ') #Pull from the sorted file
    RecipientAddress2 = f'{input_row[2]}'.ljust(40, ' ') #Pull frrom the sorted file
    RecipientAddress3 = f''.ljust(40, ' ') #Pull frrom the sorted file
    RecipientAddress4 = f''.ljust(40, ' ') #Pull frrom the sorted file
    RecipientAddress5 = f''.ljust(40, ' ') #Pull frrom the sorted file
    Zip5 = f''.rjust(5, '0')
    Zip4 = f''.rjust(4, '0')
    Zip2 = f''.rjust(2, '0')
    ZipCheckDigit = ''.ljust(1, ' ')
    BusinessReturnAddress1 = f''.ljust(40, ' ')
    BusinessReturnAddress2 = f''.ljust(40, ' ')
    BusinessReturnAddress3 = f''.ljust(40, ' ')
    BusinessReturnAddress4 = f''.ljust(40, ' ')
    BusinessReturnAddress5 = f''.ljust(40, ' ')
    LogoBitmapSelect1 = ''.ljust(8, ' ')
    LogoBitmapSelect2 = ''.ljust(8, ' ')
    MarketingTextMessage = ''.ljust(40, ' ')
    IntelligentMailBarcode = ''.ljust(31, ' ')
    ReprintIndex = ''.ljust(20, ' ')
    PrviousMailRunUniquelID = ''.ljust(30, ' ')
    PreviousMRDF = ''.ljust(15, ' ')
    PreviousPieceID = ''.rjust(6, '0')
    UserDefinedField1 = f'{input_row[5]}'.ljust(30, ' ')
    UserDefinedField2 = ''.ljust(30, ' ')
    UserDefinedField3 = ''.ljust(30, ' ')
    UserDefinedField4 = ''.ljust(30, ' ')
    UserDefinedField5 = ''.ljust(30, ' ')
    EndOfRecordIndicator = 'X'

    record = (JobID + PieceID + TotalSheetsInputFdr1 + TotalSheetsInputFdr2 + SubsetSheetNumInptFdr1 + SubsetSheetNumInptFdr2 + AccountIdentifier + InsertFeed01 +
              InsertFeed02 + InsertFeed03 + InsertFeed04 + InsertFeed05 + InsertFeed06 + InsertFeed07 + InsertFeed08 + InsertFeed09 + InsertFeed10 + InsertFeed11 + InsertFeed12 +
              SelectiveAccessory1 + SelectiveAccessory2 + SelectiveAccessory3 + SelectiveAccessory4 + SelectiveAccessory5 + SelectiveAccessory6 + AccountPull + QualityAudit +
              AlertAndClear + RecipientName + RecipientAddress1 + RecipientAddress2 + RecipientAddress3 + RecipientAddress4 + RecipientAddress5 + Zip4 + Zip5 + Zip2 + ZipCheckDigit +
              BusinessReturnAddress1 + BusinessReturnAddress2 + BusinessReturnAddress3 + BusinessReturnAddress4 + BusinessReturnAddress5 + LogoBitmapSelect1 + LogoBitmapSelect2 +
              MarketingTextMessage + IntelligentMailBarcode + ReprintIndex + PrviousMailRunUniquelID + PreviousMRDF + PreviousPieceID + UserDefinedField1 + UserDefinedField2 +
              UserDefinedField3 + UserDefinedField4 + UserDefinedField5 + EndOfRecordIndicator)
    

    return record


def main():
    logging.basicConfig(level=logging.DEBUG)
    root = tk.Tk()
    app = App(root)
    app.root.mainloop()

if __name__ == '__main__':
    main()