'''
Created on Dec 21, 2015

@author: eric5946
'''
import arcpy
import os
import shutil
import string
import uuid
import platform
import sys
import platform

from pmdm import RunUtil
from ngce.folders.FoldersConfig import chars, repls
from ngce.raster import RasterConfig

arcpy.CheckOutExtension('JTX')
JobDataWorkspace = {}
shape = {}
inMemory = "in_memory"

def fileCounter(myPath, ext):
    fileCounter = 0
    firstFile = None
    for root, dirs, files in os.walk(myPath):  # @UnusedVariable
        for f in files:    
            if f.upper().endswith(ext.upper()):
                fileCounter += 1
                if firstFile is None:
                    firstFile = f
    
    return fileCounter, firstFile

def cleanString(alias):
    if alias is not None and len(str(alias)) > 0:
        for i, item in enumerate(chars):
            alias = string.replace(str(alias), item, repls[i])
    else:
        alias = None
    
    return alias

def printArguments(argNameList, argList, argSource=None):
    try:
        arcpy.AddMessage("Architecture='{} {}' Python='{}'".format(arcpy.GetInstallInfo()['ProductName'],platform.architecture()[0],sys.executable))

        if argSource is not None:
            arcpy.AddMessage("{}".format(argSource))
        for i, item in enumerate(argNameList):
            arcpy.AddMessage("{}. {} = '{}'".format(i, item, argList[i]))
    except:
        pass
        

    
def getExistingRecord(in_table, field_names, uidIndex, where_clause=None):
    try:
        desc = arcpy.Describe(arcpy.env.workspace)
        cp = desc.connectionProperties
        #arcpy.AddMessage('Environment workspace: workspaceType={}'.format(desc.workspaceType))
        #arcpy.AddMessage('Environment workspace: instance={} database={} version={}'.format(cp.instance, cp.database , cp.version))
    except:
        pass
    arcpy.AddMessage("Searching for row from {} where {}".format(in_table, where_clause))
    strUID = None
    row = None
##    try:
    fnindex = 0
##    for fn in field_names:
##        arcpy.AddMessage("Field {} = {} ".format(fnindex, fn))
##        fnindex = fnindex + 1
    for r in arcpy.da.SearchCursor(in_table, field_names, where_clause=where_clause):  # @UndefinedVariable
        #arcpy.AddMessage("1")
        if uidIndex >= 0:
            #arcpy.AddMessage("2")
            if r[uidIndex] is not None and len(r[uidIndex]) > 0:
                #arcpy.AddMessage("3")
                row = r
                strUID = r[uidIndex]
            #arcpy.AddMessage("4")
            
        else:
            #arcpy.AddMessage("5")
            row = r
##    except:
##        
##        tb = sys.exc_info()[2]
##        tbinfo = traceback.format_tb(tb)[0]
##        arcpy.AddError(tbinfo)
##        arcpy.AddError(str(sys.exc_info()[1]))
##        for m in arcpy.GetMessages(2):
##            arcpy.AddError(m)
##        print tbinfo
##        print str(sys.exc_info()[1])
##        for m in arcpy.GetMessages(2):
##            print m
##        raise Exception("Failed")
    
    arcpy.AddMessage("Found row {} with UID {}".format(row, strUID))
    return row, strUID


def updateRecord(in_table, field_names, rowValueList, where_clause=None, editSession=True):
    arcpy.AddMessage('Updating table {} with record {} where {}'.format(in_table, rowValueList, where_clause))
    if editSession:
        edit = startEditingSession()
    cursor_u = arcpy.da.UpdateCursor(in_table, field_names, where_clause=where_clause)  # @UndefinedVariable
    for urow in cursor_u:
        arcpy.AddMessage('Updating record {}'.format(urow))
        # if more than one row, make them all the same
        cursor_u.updateRow(rowValueList)
    
    stopEditingSession(edit)
    del cursor_u
    arcpy.AddMessage('Updated record {}'.format(rowValueList))

def addOrUpdateRecord(in_table, field_names, uidIndex, rowValueList, where_clause=None, editSession=True):
    row, strUid = getExistingRecord(in_table=in_table, field_names=field_names, uidIndex=uidIndex, where_clause=where_clause)    
    
    if row is None:
        edit = None
        if editSession:
            edit = startEditingSession()
        arcpy.AddMessage('Created record {}'.format(in_table))
        arcpy.AddMessage('Created record {}'.format(field_names))
    
        cursor_i = arcpy.da.InsertCursor(in_table, field_names)  # @UndefinedVariable
        cursor_i.insertRow(rowValueList)
        
        arcpy.AddMessage('Created record {}'.format(rowValueList))
        stopEditingSession(edit)
        del cursor_i
    else:
        updateRecord(in_table, field_names, rowValueList, where_clause)
    
    
    return strUid

def createUid(strUID):
    if strUID is None:
        strUID = "{" + str(uuid.uuid4()) + "}"
    return strUID

## 32BIT ONLY ##
def setWMXJobDataAsEnvironmentWorkspace(jobId):
    if jobId not in JobDataWorkspace.keys():
        arch = platform.architecture()[0]
        if arch == '64bit':
            arcpy.AddMessage("_______32BIT_________")
            JobDataWorkspace[jobId] = RunUtil.runTool(r'ngce\Utility32bit.py', ['setWMXJobDataAsEnvironmentWorkspace', '{}'.format(jobId)], True)
            JobDataWorkspace[jobId] = str(JobDataWorkspace[jobId]).rstrip('\n').rstrip('\r').rstrip('\n')
            arcpy.AddMessage("Workspace = '{}'".format(JobDataWorkspace[jobId]))
            arcpy.AddMessage("_______32BIT_________")
        else:
            JobDataWorkspace[jobId] = str(arcpy.GetJobDataWorkspace_wmx(jobId,os.path.join(os.path.dirname(os.path.abspath(__file__)),'WMXAdmin.jtc')))  # @UndefinedVariable
    arcpy.env.workspace = JobDataWorkspace[jobId] 
    arcpy.AddMessage("Environment workspace: '{}'".format(arcpy.env.workspace))
    try:
        desc = arcpy.Describe(JobDataWorkspace[jobId])
        cp = desc.connectionProperties
        arcpy.AddMessage('Environment workspace: workspaceType={}'.format(desc.workspaceType))
        arcpy.AddMessage('Environment workspace: instance={} database={} version={}'.format(cp.instance, cp.database , cp.version))
    except:
        pass
## 32 BIT ONLY ##
    
def startEditingSession():
    arcpy.AddMessage('Starting edit session...')
    edit = arcpy.da.Editor(arcpy.env.workspace)  # @UndefinedVariable
    edit.startEditing(True, True)
    edit.startOperation()
    arcpy.AddMessage('Edit session started')
     
    return edit
     
def stopEditingSession(edit):
    if edit is not None:
        arcpy.AddMessage('Stopping edit session...')
        edit.stopOperation()
        edit.stopEditing(True)
        arcpy.AddMessage('Edit session stopped')
        del edit

## 32 BIT ONLY ##
def getJobAoi(jobId):
    if jobId not in shape.keys():
        arch = platform.architecture()[0]
        if arch == '64bit':
            arcpy.AddMessage("_______32BIT_________")
            shape[jobId] = RunUtil.runTool(r'"Q:\elevation\WorkflowManager\Tools\ngce\Utility32bit.py"', ['"getJobAoi"', '"{}"'.format(jobId)], True)
            arcpy.AddMessage("_______32BIT_________")
        else:
            shape[jobId] = arcpy.CopyFeatures_management(arcpy.GetJobAOI_wmx(jobId), arcpy.Geometry())[0]  # @UndefinedVariable
    arcpy.AddMessage("Job AOI: {}".format(shape[jobId]))
    return shape[jobId]
## 32 BIT ONLY ##



def getSpatialReferenceInfo(sr):
    output = []
    output.append(['MDomain', sr.MDomain])
    output.append(['MFalseOriginAndUnits', sr.MFalseOriginAndUnits])
    output.append(['MResolution', sr.MResolution])
    output.append(['MTolerance', sr.MTolerance])
    output.append(['XYResolution', sr.XYResolution])
    output.append(['XYTolerance', sr.XYTolerance])
    output.append(['ZDomain', sr.ZDomain])
    output.append(['ZFalseOriginAndUnits', sr.ZFalseOriginAndUnits])
    output.append(['ZResolution', sr.ZResolution])
    output.append(['ZTolerance', sr.ZTolerance])
    output.append(['abbreviation', sr.abbreviation])
    output.append(['alias', sr.alias])
    output.append(['domain', sr.domain])
    output.append(['factoryCode', sr.factoryCode])
    output.append(['falseOriginAndUnits', sr.falseOriginAndUnits])
    output.append(['hasMPrecision', sr.hasMPrecision])
    output.append(['hasXYPrecision', sr.hasXYPrecision])
    output.append(['hasZPrecision', sr.hasZPrecision])
    output.append(['isHighPrecision', sr.isHighPrecision])
    output.append(['name', sr.name])
    output.append(['remarks', sr.remarks])
    output.append(['type', sr.type])
    output.append(['usage', sr.usage])
    output.append(['PCSCode', sr.PCSCode])
    output.append(['PCSName', sr.PCSName])
    output.append(['azimuth', sr.azimuth])
    output.append(['centralMeridian', sr.centralMeridian])
    output.append(['centralMeridianInDegrees', sr.centralMeridianInDegrees])
    output.append(['centralParallel', sr.centralParallel])
    output.append(['classification', sr.classification])
    output.append(['falseEasting', sr.falseEasting])
    output.append(['falseNorthing', sr.falseNorthing])
    output.append(['latitudeOf1st', sr.latitudeOf1st])
    output.append(['latitudeOf2nd', sr.latitudeOf2nd])
#     output.append(['latitudeOfOrigin', sr.latitudeOfOrigin])
    output.append(['linearUnitCode', sr.linearUnitCode])
    output.append(['linearUnitName', sr.linearUnitName])
    output.append(['longitude', sr.longitude])
    output.append(['longitudeOf1st', sr.longitudeOf1st])
    output.append(['longitudeOf2nd', sr.longitudeOf2nd])
    output.append(['longitudeOfOrigin', sr.longitudeOfOrigin])
#     output.append(['metersPerUnit', sr.metersPerUnit])
    output.append(['projectionCode', sr.projectionCode])
    output.append(['projectionName', sr.projectionName])
    output.append(['scaleFactor', sr.scaleFactor])
    output.append(['standardParallel1', sr.standardParallel1])
    output.append(['standardParallel2', sr.standardParallel2])
    output.append(['GCSCode', sr.GCSCode])
    output.append(['GCSName', sr.GCSName])
    output.append(['angularUnitCode', sr.angularUnitCode])
    output.append(['angularUnitName', sr.angularUnitName])
    output.append(['datumCode', sr.datumCode])
    output.append(['datumName', sr.datumName])
    output.append(['flattening', sr.flattening])
    output.append(['longitude', sr.longitude])
    output.append(['primeMeridianCode', sr.primeMeridianCode])
    output.append(['primeMeridianName', sr.primeMeridianName])
    output.append(['radiansPerUnit', sr.radiansPerUnit])
    output.append(['semiMajorAxis', sr.semiMajorAxis])
    output.append(['semiMinorAxis', sr.semiMinorAxis])
    output.append(['spheroidCode', sr.spheroidCode])
    output.append(['spheroidName', sr.spheroidName])

    output = ["SPATIALREFERECE", output]
    
    return output


FieldType_TEXT = "TEXT"
FieldType_LONG = "LONG"
FieldType_DOUBLE = "DOUBLE"
FieldType_FLOAT = "FLOAT"
FieldType_GUID = "GUID"
FieldType_DATE = "DATE"

def addAndCalcFieldGUID(dataset_path, field_name, field_value=None, field_alias="", add_index=False):
    addAndCalcField(dataset_path, FieldType_GUID, field_name, field_alias, "500", field_value, add_index)
    
def addAndCalcFieldText(dataset_path, field_name, field_length, field_value=None, field_alias="", code_block="", add_index=False):
    addAndCalcField(dataset_path, FieldType_TEXT, field_name, field_alias, field_length, field_value, code_block, add_index)
    
def addAndCalcFieldLong(dataset_path, field_name, field_value=None, field_alias="", add_index=False):
    addAndCalcField(dataset_path, FieldType_LONG, field_name, field_alias, "", field_value, add_index)

def addAndCalcFieldDouble(dataset_path, field_name, field_value=None, field_alias="", add_index=False):
    addAndCalcField(dataset_path, FieldType_DOUBLE, field_name, field_alias, "", field_value, add_index)    

def addAndCalcFieldFloat(dataset_path, field_name, field_value=None, field_alias="", add_index=False):
    addAndCalcField(dataset_path, FieldType_FLOAT, field_name, field_alias, "", field_value, add_index)

def addAndCalcFieldDate(dataset_path, field_name, field_value=None, field_alias="", add_index=False):
    addAndCalcField(dataset_path, FieldType_DATE, field_name, field_alias, "", field_value, add_index)
    
def addAndCalcField(dataset_path, field_type, field_name, field_alias="", field_length="", field_value=None, code_block="", add_index=False):
    arcpy.AddMessage("Adding {} field '{}({})' and setting value to '{}'".format(field_type, field_name, field_length, field_value))
    arcpy.AddField_management(dataset_path, field_name, field_type, field_precision="", field_scale="", field_length=field_length, field_alias=field_alias, field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")
    addToolMessages()
    
    if add_index:
        arcpy.AddIndex_management(dataset_path, fields=field_name, index_name=field_name, unique="NON_UNIQUE", ascending="ASCENDING")
        addToolMessages()
    
    if field_value is not None:
        arcpy.CalculateField_management(in_table=dataset_path, field=field_name, expression=field_value, expression_type="PYTHON_9.3", code_block=code_block)
        addToolMessages()

def addToolMessages():
    for message in arcpy.GetMessages(0).splitlines():
        arcpy.AddMessage(message)
        
    for message in arcpy.GetMessages(1).splitlines():
        arcpy.AddWarning(message)
        
    for message in arcpy.GetMessages(2).splitlines():
        arcpy.AddError(message)
        
def getFieldValues(in_table, field_name, where_clause=None, append_string=""):
    arcpy.AddMessage("Searching for field values from {} where {}".format(in_table, where_clause))
    values = []
    
    for r in arcpy.da.SearchCursor(in_table, [field_name], where_clause=where_clause):  # @UndefinedVariable
        values.append(r[0])
    
    arcpy.AddMessage("Found {} values in field {}".format(len(values), field_name))
    return values


        
# arcpy.env.overwriteOutput = True 
# 
# updateGeometries(r"C:\NRCS\Projects\DAS2\IN_AZTest11_2010\DERIVED\IN_AZTest11_2010.gdb\LAS_Boundary", r"C:\NRCS\Projects\DAS2\IN_AZTest11_2010\DERIVED\IN_AZTest11_2010.gdb\LAS_Footprints"
#                  , r"C:\NRCS\Projects\DAS2\IN_AZTest11_2010\DERIVED\IN_AZTest11_2010.gdb\LASFileInfo",
#                  r"C:\NRCS\Projects\DAS2\IN_AZTest11_2010\DERIVED\IN_AZTest11_2010.gdb\LASDStats",
#                  r"C:\NRCS\Projects\DAS2\IN_AZTest11_2010\DERIVED\IN_AZTest11_2010.gdb",
#                  r"\\erici2\C$\NRCS\Projects\DAS2\IN_AZTest11_2010\DELIVERED\LAS_CLASSIFIED\CR_NAD83UTM14N_NAVD88Meters.prj",
#                  "IN_AZTest11_2010.lasd"
#                  
#                  
#                  )


def getVertCSInfo(spatialReference):
    sr = spatialReference.exportToString()
    pos = 0
    count = 0
    openBr = []
    comma = []
    pcomma = []
    groups = []
    for char in sr:
        if char == ',':
            comma.append(pos)
        elif char == '[':
            openBr.append(pos)
            if len(comma) > 0:
                pcomma.append(comma.pop())
            else:
                pcomma.append(0)
            
            count = count + 1
        elif char == ']':
            openBr.pop()
            groups.append([count, [pcomma.pop(), pos]])
            count = count - 1
        pos = pos + 1
        
            
    #print groups
    #print openBr
    
    
    levels = []
    master = []
    for level, [start, end] in groups:
        if start == 0:
            start = -1
        bracketInfo = sr[start + 1:end]
        splits = bracketInfo.split('[')
        if len(splits) == 2:
            name = splits[0]
            value = splits[1].split(',')         
            master.append([name, value])
            levels.append(level)
        else:
            name = splits[0]
            value = splits[1].split(",")[0] 
            pvalues = []
            while len(levels) > 0 and levels[len(levels) - 1] > level:
                levels.pop()
                pvalues.append(master.pop())
             
            newp = [name, [value, pvalues]]
             
            master.append(newp)
            levels.append(level)
              
              
    #print master
    
    vert_cs_name = None
    vert_unit_name = None
    
    for cs in master:
        cstype = cs[0]
        if cstype.upper() == "VERTCS":
            vert_cs_name = cs[1][0]
            vert_unit_name = None
           
            for parameter in cs[1][1]:
                ptype = parameter[0]
                if ptype.upper() == "UNIT":
                    vert_unit_name = parameter[1][0]
    
    if vert_cs_name is not None:
        vert_cs_name = str(vert_cs_name).strip().replace("'", "") 
    if vert_unit_name is not None:
        vert_unit_name = str(vert_unit_name).strip().replace("'", "")         
    return vert_cs_name, vert_unit_name
    
def clearFolder(folder):
    if os.path.exists(folder):
        arcpy.AddMessage("Deleting existing output destination {}".format(folder))
        shutil.rmtree(folder)
    if not os.path.exists(folder):
        arcpy.AddMessage("Creating output destination {}".format(folder))
        os.mkdir(folder)
        
def getDomainValueList(workspace, domainName):
    domains = arcpy.da.ListDomains(workspace)  # @UndefinedVariable
    values = []
    for domain in domains:
        if domain.name.upper() == domainName.upper():
            coded_values = domain.codedValues
            for val, desc in coded_values.iteritems():
                values.append(val.strip().upper())
    
    return values

def setArcpyEnv(is_overwrite_output):
    arcpy.env.overwriteOutput = is_overwrite_output
    # Turn off pyramid creation because a GDAL bug with Floating-pt NoData values is causing issues
    arcpy.env.pyramid = RasterConfig.NONE
    arcpy.env.resamplingmethod = RasterConfig.BILINEAR
    arcpy.env.compression = RasterConfig.COMPRESSION_LZ77
    arcpy.env.tileSize = RasterConfig.TILE_SIZE_256
    arcpy.env.nodata = RasterConfig.NODATA_DEFAULT

def AddValueToDomain(workspace, domain, code, description):
    arcpy.env.workspace = workspace
    if not arcpy.Exists(domain):
        arcpy.CreateDomain_management(in_workspace=workspace, domain_name=domain, domain_description=domain, field_type="TEXT", domain_type="CODED", split_policy="DEFAULT", merge_policy="DEFAULT")
    
    arcpy.AddCodedValueToDomain_management(workspace, domain, code, description)

