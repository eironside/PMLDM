'''
Created on Jan 20, 2017

@author: eric5946
'''


import arcpy
import arcpywmx
import re

arcpy.CheckOutExtension('JTX')

        
# #Get a list of Hold types in Workflow database
# hold_types = conn.config.getHoldTypes()
# 
# #Access a Workflow Job 
# job = conn.getJob(99999)
# 
# #Find the id of Budget Hold hold type and add a hold on the job
# for hold in hold_types:
#     if(hold.name=='Budget Hold'):
#         job.addHold(hold.id,comment="Insufficient funds for digitization.")

fc = r"G:\WorkflowManager\MetadataBoundaries.gdb\Nov2016_NAD83"
jtc = r'c:\ProgramData\ESRI\WMX\10.3\Database\WMXAdmin.jtc'

fieldProjName='ProjName'
fieldHorzProj='HorzProj'
fieldHorzUnit='HorzUnit'
fieldVertProj='VertProj'
fieldVertUnit='VertUnit'
fieldNPS='NPS'
fieldPtDenMeter='PtDenMeter'
fieldRMSE='RMSE'
fieldCellResolu='CellResolu'
fieldBeginDate='BeginDate'
fieldEndDate='EndDate'
fieldMetaXMLLin='MetaXMLLin'
fieldInMscServ='InMscServ'
fieldUSGS='USGS'
fieldElevUnit='ElevUnit'
fieldUserGuide='UserGuide'
fieldBoundary='Boundary'
fieldHDDateRcvd='HDDateRcvd'
fieldCopy2Deliv='Copy2Deliv'
fieldWorkflowNotes='WorkflowNotes'
fieldSqMiles='SqMiles'
fieldDSMstatus='DSMstatus'

conn = None
cursor = arcpy.SearchCursor(fc)
for row in cursor:
    print(row)

    loi = row.getValue("SHAPE")
    valueProjName = row.getValue(fieldProjName)
    valueHorzProj = row.getValue(fieldHorzProj)
    valueHorzUnit = row.getValue(fieldHorzUnit)
    valueVertProj = row.getValue(fieldVertProj)
    valueVertUnit = row.getValue(fieldVertUnit)
    valueNPS = row.getValue(fieldNPS)
    valuePtDenMeter = row.getValue(fieldPtDenMeter)
    valueRMSE = row.getValue(fieldRMSE)
    valueCellResolu = row.getValue(fieldCellResolu)
    valueBeginDate = row.getValue(fieldBeginDate)
    valueEndDate = row.getValue(fieldEndDate)
    valueMetaXMLLin = row.getValue(fieldMetaXMLLin)
    valueInMscServ = row.getValue(fieldInMscServ)
    valueUSGS = row.getValue(fieldUSGS)
    valueElevUnit = row.getValue(fieldElevUnit)
    valueUserGuide = row.getValue(fieldUserGuide)
    valueBoundary = row.getValue(fieldBoundary)
    valueHDDateRcvd = row.getValue(fieldHDDateRcvd)
    valueCopy2Deliv = row.getValue(fieldCopy2Deliv)
    valueWorkflowNotes = row.getValue(fieldWorkflowNotes)
    valueSqMiles = row.getValue(fieldSqMiles)
    valueDSMstatus = row.getValue(fieldDSMstatus)
    
    projectNameParts = str(valueProjName).split("_")
    year = projectNameParts[len(projectNameParts)-1]
    state = projectNameParts[0]
    alias = "Project"
    if len(projectNameParts) >2:
        alias = projectNameParts[1]
    
    aliasParts = re.findall('[A-Z][^A-Z]*', alias)
    if len(aliasParts)>1:
        alias = " ".join(aliasParts)
    print "\t\t'{}' '{}' '{}'".format(state, alias, year)
    
    desc = {'LOI': loi, 'assignedTo':'Steven.Nechero', 'assignedType': 'User', 'dataWorkspaceID':'{DB245005-0D1E-47B8-AE39-CC08530A6C9D}', 
            'jobTypeName':'LDM A Lidar Data Management',  'parentVersionName': 'DBO.Edit', 'prefix':'', 'versionName':''}
    
    if conn is None:
        #Establish a connection to a Workflow database
        conn = arcpywmx.Connect(jtc)

    #Create a Workflow Job of type Quality Control
    job = conn.createJob(job_type_description=desc)
    #Get the extended properties table associated with the job
    prop_table = job.getExtendedPropertyTable('WMXADMIN.JTXX_LDM_PROPS')
    
    #Update value of extended property fields
    prop_table['ProjectAlias'].data=alias
    prop_table['ProjectYear'].data=year
    prop_table['ProjectState'].data=state
    prop_table['ProjectParentDirectory'].data=r"\\aiotxftw6na01\SQL1\elevation"
    prop_table['ProjectArchiveDirectory'].data=r"\\aiotxftw3fp3\files\Archive_to_Tape\Lidar"
    job.save()
    
    job.markStepAsComplete (8838)
    job.markStepAsComplete (8837)
    job.markStepAsComplete (8810)
    job.markStepAsComplete (8811)
    
    job.executeStep(8812)
    job.setStepAsCurrent()
    
    break