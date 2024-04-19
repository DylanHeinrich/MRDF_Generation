import csv
import pandas as pd
from datetime import datetime



orderId = input('Please enter in the Detail Number or Job number\n')

JobID = orderId[-6:]
RunID = JobID.ljust(15)
CreationDate = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
GroupID = ''.ljust(15)
JobType = 'CHECK1'.ljust(15)
ApplicationID = 'ShippingMgr'.ljust(15)
CycleID = datetime.now().strftime('%m_%d')
ClientID = ''.ljust(15)
SLADueDate = ''.ljust(19)
SLAWarningOffset = ''.ljust(5,'0')
ReprintDueDate = ''.ljust(19)
ReprintWarningOffset = ''.ljust(5,'0')
DataSetType = '2'
PlannedMailPieceCount = ''.ljust(6,'0')
PlannedSheetCount = ''.ljust(10,'0')
InserterMode = ''.ljust(8)
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
EnvelopeFeederDocID = ''.ljust(15)
InserFeeder01DocID = ''.ljust(15)
InserFeeder02DocID = ''.ljust(15)
InserFeeder03DocID = ''.ljust(15)
InserFeeder04DocID = ''.ljust(15)
InserFeeder05DocID = ''.ljust(15)
InserFeeder06DocID = ''.ljust(15)
InserFeeder07DocID = ''.ljust(15)
InserFeeder08DocID = ''.ljust(15)
InserFeeder09DocID = ''.ljust(15)
InserFeeder10DocID = ''.ljust(15)
InserFeeder11DocID = ''.ljust(15)
InserFeeder12DocID = ''.ljust(15)
PrintJobName = ''.ljust(15)
BREPrintJobName = ''.ljust(15)
UserDefinedField1 = ''.ljust(30)
UserDefinedField2 = ''.ljust(30)
Filler = ''.ljust(438)
EndOfRecordIndicator = 'X'






fileName = 'Test.csv'


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
             EndOfRecordIndicator+'\n')

MRDF = open(f'{orderId}.txt','a')
MRDF.write(headerrow)
MRDF.close()

#print(JobID+RunID+CreationDate+GroupID+JobType+ApplicationID+CycleID+ClientID+SLADueDate+SLAWarningOffset+ReprintDueDate+ReprintWarningOffset+DataSetType+PlannedMailPieceCount+PlannedSheetCount+InserterMode+InserFeeder01Mode+InserFeeder02Mode+InserFeeder03Mode+InserFeeder04Mode+InserFeeder05Mode+InserFeeder06Mode+InserFeeder07Mode+InserFeeder08Mode+InserFeeder09Mode+InserFeeder10Mode+InserFeeder11Mode+InserFeeder12Mode+EnvelopeFeederDocID+InserFeeder01DocID+InserFeeder02DocID+InserFeeder03DocID+InserFeeder04DocID+InserFeeder05DocID+InserFeeder06DocID+InserFeeder07DocID+InserFeeder08DocID+InserFeeder09DocID+InserFeeder10DocID+InserFeeder11DocID+InserFeeder12DocID+PrintJobName+BREPrintJobName+UserDefinedField1+UserDefinedField2+EndOfRecordIndicator)
