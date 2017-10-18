'''
Created on Dec 21, 2015

@author: eric5946
'''
import arcpy
import datetime
from itertools import izip_longest
import os
import platform
import shutil
import string
import sys
import uuid

from ngce.folders.FoldersConfig import chars, repls
from ngce.raster import RasterConfig


# arcpy.CheckOutExtension('JTX')  # Need this?
JobDataWorkspace = {}
shape = {}
inMemory = "in_memory"

JTC_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'WMXAdmin.jtc')
SDE_WMX_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'wmx.sde')
SDE_CMDR_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cmdr.sde')
WMX_AOI_FC = "LDM_WMX.DBO.JTX_JOBS_AOI"
#WMX_AOI_FC = "NGCE_WMX.DBO.JTX_JOBS_AOI"

def fileCounter(myPath, ext=None):
    fileCounter = 0
    firstFile = None
    for root, dirs, files in os.walk(myPath):  # @UnusedVariable
        for f in files:
            if ext is None or f.upper().endswith(ext.upper()):
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
        arcpy.AddMessage("Architecture='{} {}' Python='{}'".format(arcpy.GetInstallInfo()['ProductName'], platform.architecture()[0], sys.executable))

        if argSource is not None:
            arcpy.AddMessage("{}".format(argSource))
        for i, item in enumerate(argNameList):
            arcpy.AddMessage("{}. {} = '{}'".format(i, item, argList[i]))
    except:
        pass



def getExistingRecord(in_table, field_names, uidIndex, where_clause=None):
    arcpy.AddMessage("Searching for row from {} where {}".format(in_table, where_clause))
    strUID = None
    row = None
    for r in arcpy.da.SearchCursor(in_table, field_names, where_clause=where_clause):  # @UndefinedVariable
        if uidIndex >= 0:
            if r[uidIndex] is not None and len(r[uidIndex]) > 0:
                row = r
                strUID = r[uidIndex]

        else:
            row = r

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


def setWMXJobDataAsEnvironmentWorkspace(jobId):
    # ## Swapped out WMX request for direct .sde file connection
    arcpy.env.workspace = SDE_CMDR_FILE_PATH  # JobDataWorkspace[jobId] 
    arcpy.AddMessage("Environment workspace: '{}'".format(SDE_CMDR_FILE_PATH))
    
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

    
def getJobAoi(jobId):
    if jobId not in shape.keys():
        in_table = os.path.join(SDE_WMX_FILE_PATH, WMX_AOI_FC)
        field_names = ["SHAPE@"]
        uidIndex = None
        where_clause = "{} = {}".format(arcpy.AddFieldDelimiters(in_table, "JOB_ID"), jobId)
        # arcpy.AddMessage(where_clause)
        aoi = getExistingRecord(in_table, field_names, uidIndex, where_clause)[0]
        # arcpy.AddMessage(aoi[0])
        shape[jobId] = aoi[0]
        
    arcpy.AddMessage("Job {} AOI: {}".format(jobId, shape[jobId]))
    return shape[jobId]




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

def addAndCalcFieldGUID(dataset_path, field_name, field_value=None, field_alias="", add_index=False, debug=False):
    addAndCalcField(dataset_path=dataset_path,
                    field_type=FieldType_GUID,
                    field_length="500",
                    field_name=field_name,
                    field_alias=field_alias,
                    field_value=field_value,
                    add_index=add_index,
                    debug=debug)
    
def addAndCalcFieldText(dataset_path, field_name, field_length, field_value=None, field_alias="", code_block="", add_index=False, debug=False):
    addAndCalcField(dataset_path=dataset_path,
                    field_type=FieldType_TEXT,
                    field_length=field_length,
                    field_name=field_name,
                    field_alias=field_alias,
                    field_value=field_value,
                    add_index=add_index,
                    code_block=code_block,
                    debug=debug
                    )
    
def addAndCalcFieldLong(dataset_path, field_name, field_value=None, field_alias="", add_index=False, code_block="", debug=False):
    addAndCalcField(dataset_path=dataset_path,
                    field_type=FieldType_LONG,
                    field_name=field_name,
                    field_alias=field_alias,
                    field_value=field_value,
                    code_block=code_block,
                    debug=debug)

def addAndCalcFieldDouble(dataset_path, field_name, field_value=None, field_alias="", add_index=False, code_block="", debug=False):
    addAndCalcField(dataset_path=dataset_path,
                    field_type=FieldType_DOUBLE,
                    field_name=field_name,
                    field_alias=field_alias,
                    field_value=field_value,
                    add_index=add_index)    

def addAndCalcFieldFloat(dataset_path, field_name, field_value=None, field_alias="", add_index=False, code_block="", debug=False):
    addAndCalcField(dataset_path=dataset_path,
                    field_type=FieldType_FLOAT,
                    field_name=field_name,
                    field_alias=field_alias,
                    field_value=field_value,
                    add_index=add_index,
                    code_block=code_block,
                    debug=debug
                    )

def addAndCalcFieldDate(dataset_path, field_name, field_value=None, field_alias="", add_index=False, code_block="", debug=False):
    addAndCalcField(dataset_path=dataset_path,
                    field_type=FieldType_DATE,
                    field_name=field_name,
                    field_alias=field_alias,
                    field_value=field_value,
                    add_index=add_index,
                    code_block=code_block,
                    debug=debug
                    )
    
def addAndCalcField(dataset_path, field_type, field_name, field_alias="", field_length="", field_value=None, code_block="", add_index=False, debug=False):
    if debug: 
        arcpy.AddMessage("Adding {} field '{}({})' and setting value to '{}'".format(field_type, field_name, field_length, field_value))

    arcpy.AddField_management(dataset_path, field_name, field_type, field_precision="", field_scale="", field_length=field_length, field_alias=field_alias, field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")
    if debug:
        addToolMessages()

    if add_index:
        arcpy.AddIndex_management(dataset_path, fields=field_name, index_name=field_name, unique="NON_UNIQUE", ascending="ASCENDING")
        if debug:
            addToolMessages()

    if field_value is not None:
        arcpy.CalculateField_management(in_table=dataset_path, field=field_name, expression=field_value, expression_type="PYTHON_9.3", code_block=code_block)
        if debug:
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


def getVertCSInfo(spatialReference):
    sr = spatialReference
    try:
        sr = spatialReference.exportToString()
    except:
        try:
            sr = arcpy.SpatialReference(spatialReference)
            sr = spatialReference.exportToString()
        except:
            try:
                sr = arcpy.SpatialReference()
                sr.loadFromString(spatialReference)
                sr = spatialReference.exportToString()
            except:
                sr = spatialReference
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


    # print master

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


def getString(str_value):
    result = None
    if str_value is not None:
        result = str(str_value).strip().upper()
        if len(result) <= 0:
            result = None
    
    return result
    
def getSRValues(spatial_ref):
    
    if str(spatial_ref).lower().endswith(".prj"):
        spatial_ref= arcpy.SpatialReference(spatial_ref)
        
    horz_cs_name = None
    horz_cs_unit_name = None
    horz_cs_factory_code = None
    vert_cs_name, vert_unit_name = None, None
    
    name = None
    try:
        name = spatial_ref.name
    except:
        pass
    
    if name is None:
        spatial_ref= arcpy.SpatialReference().loadFromString(spatial_ref)
    
    if spatial_ref is not None:
        horz_cs_name = getString(spatial_ref.name)
        horz_cs_unit_name = getString(spatial_ref.linearUnitName)
        horz_cs_factory_code = getString(spatial_ref.factoryCode)
        vert_cs_name, vert_unit_name = getVertCSInfo(spatial_ref)
        
        vert_cs_name = getString(vert_cs_name)
        vert_unit_name = getString(vert_unit_name)
    
    return horz_cs_name, horz_cs_unit_name, horz_cs_factory_code, vert_cs_name, vert_unit_name

    
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
            for val, desc in coded_values.iteritems():  # @UnusedVariable
                values.append(val.strip().upper())

    return values

def setArcpyEnv(is_overwrite_output):
    arcpy.env.overwriteOutput = is_overwrite_output
    # Turn off pyramid creation because a GDAL bug with Floating-pt NoData values is causing issues
    arcpy.env.pyramid = RasterConfig.NONE
    arcpy.env.resamplingmethod = RasterConfig.BILINEAR
    arcpy.env.compression = RasterConfig.NONE
    arcpy.env.tileSize = RasterConfig.TILE_SIZE_256
    arcpy.env.nodata = RasterConfig.NODATA_DEFAULT

def AddValueToDomain(workspace, domain, code, description):
    arcpy.env.workspace = workspace
    if not arcpy.Exists(domain):
        arcpy.CreateDomain_management(in_workspace=workspace, domain_name=domain, domain_description=domain, field_type="TEXT", domain_type="CODED", split_policy="DEFAULT", merge_policy="DEFAULT")

    arcpy.AddCodedValueToDomain_management(workspace, domain, code, description)

def doTime(a, msg):
    b = datetime.datetime.now()
    td = (b - a).total_seconds()
    arcpy.AddMessage("{} in {}".format(msg, td))

    return datetime.datetime.now()


def deleteFileIfExists(f_path, useArcpy=False):
    try:
        if useArcpy:
            if arcpy.Exists(f_path):
                arcpy.Delete_management(f_path)
        else:
            if os.path.exists(f_path):
                os.remove(f_path)
    except:
        pass
    
def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return izip_longest(*args, fillvalue=fillvalue)




def isSrValueValid(sr_value):
    result = True
    if sr_value is None or sr_value == 'UNKNOWN' or sr_value.upper() == 'NONE' or sr_value == '0':
        result = False
    return result


'''
---------------------------------------------
fix the field name
---------------------------------------------
'''
def alterField(in_table, field, new_field_name, new_field_alias):
    try:
        arcpy.AlterField_management(in_table=in_table, field=field, new_field_name=new_field_name, new_field_alias=new_field_alias)
    except:
        pass

'''
---------------------------------------------
field infos = [old_name, new_name, alias ]
---------------------------------------------
'''
def alterFields(alter_field_infos, table):
    a = datetime.datetime.now()
    if alter_field_infos is not None:
        for alter_field_info in alter_field_infos:
            try:
                alterField(table, alter_field_info[0], alter_field_info[1], alter_field_info[2])
            except:
                pass
        
    a = doTime(a, "\tAltered fields")
    return a


def isMatchingStringValue(val1, val2):
    if val1 is not None:
        val1 = str(val1).upper().strip()
    if val2 is not None:
        val2 = str(val2).upper().strip()
    
    return (val1 == val2 and val1 is not None)
    
    
    



