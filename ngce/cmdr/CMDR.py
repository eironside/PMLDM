'''
Created on Dec 21, 2015

@author: eric5946
'''

import arcpy
import os

from ngce import Utility
from ngce.cmdr import CMDRConfig
from ngce.cmdr.CMDRConfig import fields_Contract, uid_index_Contract, \
    fcName_Contract, field_Contract_ProjID, \
    field_Contract_SHAPE, field_Contract_UID, fcName_ProjectJob, \
    field_ProjectJob_WMXJobID, uid_index_ProjectJob, \
    fields_ProjectJob, uid_index_Deliver, fcName_QC, \
    field_QC_ProjID, fields_QC, uid_index_QC, fcName_Publish, fields_Deliver, \
    field_ProjectJob_ArchDir, field_ProjectJob_ProjDir, field_ProjectJob_ProjID, \
    field_ProjectJob_Alias, field_ProjectJob_AliasClean, \
    field_ProjectJob_ParentDir, field_ProjectJob_State, field_ProjectJob_Year, \
    field_ProjectJob_UID, field_ProjectJob_SHAPE, field_Publish_ProjID, \
    field_Deliver_Status, field_Deliver_Area, field_Deliver_HorzProj, \
    field_Deliver_HorzUnit, field_Deliver_HorzWKID, field_Deliver_VertProj, \
    field_Deliver_VertUnit, field_Deliver_VertWKID, \
    field_Deliver_DTM_PointSpacing, field_Deliver_DTM_PointDensity, \
    field_Deliver_DSM_PointSpacing, field_Deliver_DSM_PointDensity, \
    field_Deliver_Count_Las, field_Deliver_ValidZMin, field_Deliver_ValidZMax, \
    field_Deliver_DTM_Count_Points, field_Deliver_DSM_Count_Points, \
    field_Deliver_Las_Classified, fcName_Deliver, field_Deliver_DSM_Exists, \
    field_Deliver_DSM_CellRes, field_Deliver_DSM_CountRaster, \
    field_Deliver_DTM_Exists, field_Deliver_DTM_CellRes, \
    field_Deliver_DTM_CountRaster, field_Deliver_Count_Raster, \
    field_Deliver_BoundXMin, field_Deliver_BoundYMin, field_Deliver_BoundYMax, \
    field_Deliver_BoundXMax, uid_index_RasterFileStat, fields_RasterFileStat, \
    fcName_RasterFileStat, field_RasterFileStat_ProjID, \
    field_RasterFileStat_Name, field_RasterFileStat_ElevType, \
    field_RasterFileStat_Group, field_RasterFileStat_UID, \
    field_RasterFileStat_Path, field_RasterFileStat_Format, \
    field_RasterFileStat_NoData, field_RasterFileStat_PixelType, \
    field_RasterFileStat_ValueType, field_RasterFileStat_Min, \
    field_RasterFileStat_Max, field_RasterFileStat_Mean, \
    field_RasterFileStat_Std, field_RasterFileStat_Top, \
    field_RasterFileStat_Bottom, field_RasterFileStat_Right, \
    field_RasterFileStat_Left, field_RasterFileStat_CellSizeX, \
    field_RasterFileStat_CellSizeY, field_RasterFileStat_Columns, \
    field_RasterFileStat_Rows, field_RasterFileStat_Bands, \
    field_RasterFileStat_HorzProj, field_RasterFileStat_HorzUnit, \
    field_RasterFileStat_HorzWKID, field_RasterFileStat_VertProj, \
    field_RasterFileStat_VertUnit, field_RasterFileStat_VertWKID, \
    field_Deliver_ProjID, field_Deliver_Date, field_Deliver_Date_CollectionBegin, \
    field_Deliver_Date_CollectionEnd, fcName_LASDStatInfo, \
    field_LASFileInfo_ProjID, field_LASFileInfo_File_Name, \
    uid_index_LASDStatInfo, fields_LASDStatInfo, \
    field_LASDStatInfo_Category, field_LASDStatInfo_Item, \
    field_LASFileInfo_File_LAS_Class, field_MDMaster_WMXJobID, \
    field_MDMaster_ParentPath, field_MDMaster_Path, field_MDMaster_Name, \
    field_MDMaster_SHAPE, field_MDMaster_Folder, field_MDMaster_CellSize, \
    field_MDMaster_ConFile_Path


def getFieldValue(self, row, field_name):
    result = None
    index = self.fields.index(field_name)
    if row is not None and len(row) > index:
        result = row[index]
    return result

def setFieldValue(self, row, field_name, value):
    index = self.fields.index(field_name)
    if row is not None and len(row) > index:
        row[index] = value



def updateProjIDAOI(project_id, row, aoi, fclass, fields, uid_index):
    # Where WMXJobID = '<Job_ID>'
    where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(fclass, field_Contract_ProjID), project_id)

    index = fields.index(field_ProjectJob_SHAPE)
    row = list(row)
    if aoi is not None:
        row[index] = aoi

    Utility.addOrUpdateRecord(in_table=fclass, field_names=fields, uidIndex=uid_index, where_clause=where_clause, rowValueList=row)

    return row


class Publish(object):
    fclass = None
    uid_index = uid_index_QC
    fields = fields_QC

    def __init__(self):
        self.fclass = arcpy.ListFeatureClasses('*{}'.format(fcName_Publish))[0]


    def getPublish(self, project_id):
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_Publish_ProjID), project_id)

        row, UID = Utility.getExistingRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause)  # @UnusedVariable

        return row


    def addOrUpdateProject(self, project_Id, UID, project_AOI):
        # Where ProjectID = '<project_ID>'
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_QC_ProjID), project_Id)

        row = [UID,
               project_Id,
               project_AOI
               ]

        Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause, rowValueList=row)

        return row



    def updateAOI(self, row, aoi):
        project_id = row[self.fields.index(field_Publish_ProjID)]
        return updateProjIDAOI(project_id, row, aoi, self.fclass, self.fields, self.uid_index)



class QC(object):
    fclass = None
    uid_index = uid_index_QC
    fields = fields_QC

    def __init__(self):
        self.fclass = arcpy.ListFeatureClasses('*{}'.format(fcName_QC))[0]


    def getQC(self, project_id):
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_QC_ProjID), project_id)

        row, UID = Utility.getExistingRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause)  # @UnusedVariable

        return row



    def addOrUpdateProject(self, project_Id, UID, project_AOI):
        # Where ProjectID = '<project_ID>'
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_QC_ProjID), project_Id)

        row = [UID,
               project_Id,
               project_AOI
               ]

        Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause, rowValueList=row)

        return row

    def updateAOI(self, row, aoi):
        project_id = row[self.fields.index(field_QC_ProjID)]
        return updateProjIDAOI(project_id, row, aoi, self.fclass, self.fields, self.uid_index)


class Deliver(object):
    fclass = None
    uid_index = uid_index_Deliver
    fields = fields_Deliver
    fields_addProject = CMDRConfig.fields_Deliver_AddProject

    def __init__(self):
        self.fclass = arcpy.ListFeatureClasses('*{}'.format(fcName_Deliver))[0]

    def getDeliver(self, project_id):
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_Deliver_ProjID), project_id)

        row, UID = Utility.getExistingRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause)  # @UnusedVariable

        return row



    def addOrUpdateProject(self, project_Id, UID, project_AOI):
        # Where ProjectID = '<project_ID>'
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_Deliver_ProjID), project_Id)

        row = [UID,
               project_Id,
               project_AOI
               ]

        Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields_addProject, uidIndex=self.uid_index, where_clause=where_clause, rowValueList=row)

        return row

    def updateDeliver(self, row, project_Id):
        # Where ProjectID = '<project_ID>'
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_Deliver_ProjID), project_Id)
        row = list(row)
        Utility.updateRecord(in_table=self.fclass, field_names=self.fields, rowValueList=row, where_clause=where_clause)

        return row

    def updateAOI(self, row, aoi):
        project_id = row[self.fields.index(field_Deliver_ProjID)]
        return updateProjIDAOI(project_id, row, aoi, self.fclass, self.fields, self.uid_index)

    def setStatus(self, row, value):
        return setFieldValue(self, row, field_Deliver_Status, value)

    def setIsLASClassified(self, row, value):
        if value:
            value = "Yes"
        else:
            value = "No"
        return setFieldValue(self, row, field_Deliver_Las_Classified, value)

    def setDeliverArea(self, row, value):
        return setFieldValue(self, row, field_Deliver_Area, value)

    def setHorzSRName(self, row, value):
        return setFieldValue(self, row, field_Deliver_HorzProj, value)

    def setHorzUnit(self, row, value):
        return setFieldValue(self, row, field_Deliver_HorzUnit, value)

    def setHorzSRWKID(self, row, value):
        return setFieldValue(self, row, field_Deliver_HorzWKID, value)

    def setVertSRName(self, row, value):
        return setFieldValue(self, row, field_Deliver_VertProj, value)

    def setVertUnit(self, row, value):
        return setFieldValue(self, row, field_Deliver_VertUnit, value)

    def setVertSRWKID(self, row, value):
        return setFieldValue(self, row, field_Deliver_VertWKID, value)

    def setPointSpacingDTM(self, row, value):
        return setFieldValue(self, row, field_Deliver_DTM_PointSpacing, value)
    def setPointDensityDTM(self, row, value):
        return setFieldValue(self, row, field_Deliver_DTM_PointDensity, value)
    def setPointSpacingDSM(self, row, value):
        return setFieldValue(self, row, field_Deliver_DSM_PointSpacing, value)
    def setPointDensityDSM(self, row, value):
        return setFieldValue(self, row, field_Deliver_DSM_PointDensity, value)
    def setCountLasFiles(self, row, value):
        return setFieldValue(self, row, field_Deliver_Count_Las, value)
    def setCountRasterFiles(self, row, value):
        return setFieldValue(self, row, field_Deliver_Count_Raster, value)
    def setCountLasPointsDTM(self, row, value):
        return setFieldValue(self, row, field_Deliver_DTM_Count_Points, value)
    def setCountLasPointsDSM(self, row, value):
        return setFieldValue(self, row, field_Deliver_DSM_Count_Points, value)
    def setValidZMin(self, row, value):
        return setFieldValue(self, row, field_Deliver_ValidZMin, value)
    def setValidZMax(self, row, value):
        return setFieldValue(self, row, field_Deliver_ValidZMax, value)

    def setBoundXMin(self, row, value):
        return setFieldValue(self, row, field_Deliver_BoundXMin, value)
    def setBoundYMin(self, row, value):
        return setFieldValue(self, row, field_Deliver_BoundYMin, value)
    def setBoundXMax(self, row, value):
        return setFieldValue(self, row, field_Deliver_BoundXMax, value)
    def setBoundYMax(self, row, value):
        return setFieldValue(self, row, field_Deliver_BoundYMax, value)
    def setNotes(self, row, value):
        return setFieldValue(self, row, field_Deliver_Notes, value)
    def setVertRMSE(self, row, value):
        return setFieldValue(self, row, field_Deliver_VertRMSE, value)
    def setBoundaryExists(self, row, value):
        return setFieldValue(self, row, field_Deliver_Boundary_Exists, value)
    def setLinkMetadataXML(self, row, value):
        return setFieldValue(self, row, field_Deliver_Link_MetaXML, value)
    def setLinkUserGuide(self, row, value):
        return setFieldValue(self, row, field_Deliver_Link_UserGuide, value)

    


    def setDTMExists(self, row, value):
        return setFieldValue(self, row, field_Deliver_DTM_Exists, value)
    def setDTMCellResolution(self, row, value):
        return setFieldValue(self, row, field_Deliver_DTM_CellRes, value)
    def setDTMCountRaster(self, row, value):
        return setFieldValue(self, row, field_Deliver_DTM_CountRaster, value)

    def setDSMExists(self, row, value):
        return setFieldValue(self, row, field_Deliver_DSM_Exists, value)
    def setDSMCellResolution(self, row, value):
        return setFieldValue(self, row, field_Deliver_DSM_CellRes, value)
    def setDSMCountRaster(self, row, value):
        return setFieldValue(self, row, field_Deliver_DSM_CountRaster, value)
    def getValidZMin(self, row):
        return getFieldValue(self, row, field_Deliver_ValidZMin)
    def getValidZMax(self, row):
        return getFieldValue(self, row, field_Deliver_ValidZMax)
    def getDeliverDate(self, row):
        return getFieldValue(self, row, field_Deliver_Date)
    def setDeliverDate(self, row, value):
        return setFieldValue(self, row, field_Deliver_Date, value)
    def getCollectionBeginDate(self, row):
        return getFieldValue(self, row, field_Deliver_Date_CollectionBegin)
    def setCollectionBeginDate(self, row, value):
        return setFieldValue(self, row, field_Deliver_Date_CollectionBegin, value)
    def getCollectionEndDate(self, row):
        return getFieldValue(self, row, field_Deliver_Date_CollectionEnd)
    def setCollectionEndDate(self, row, value):
        return setFieldValue(self, row, field_Deliver_Date_CollectionEnd, value)
    def getIsClassified(self, row):
        result = getFieldValue(self, row, field_Deliver_Las_Classified)
        if result is not None:
            if result == 1 or result:
                result = "Yes"
            else:
                result = str(result).strip().upper()
                if result == "YES":
                    result = "Yes"
                else:
                    result = "No"
        else:
            result = "No"
        return result

    def getVertUnit(self, row):
        result = getFieldValue(self, row, field_Deliver_VertUnit)
        if result is not None:
            result = str(result).replace("'", "")
            result = result.strip().upper()

        return result

    def getValidDate(self, row):
        result = self.getCollectionEndDate(row)
        if result is None:
            result = self.getCollectionBeginDate(row)
        if result is None:
            result = self.getDeliverDate(row)
        if result is not None:
            # remove the time portion of the ProjectDate, if it exists
            result = "{}/{}/{}".format(result.day, result.month, result.year)
            if len(result) < 6:
                # something went wrong, don't use the date
                result = None
        arcpy.AddMessage("Project Date:{}".format(result))
        return result

class ProjectJob(object):

    fclass = None
    uid_index = uid_index_ProjectJob
    fields = fields_ProjectJob

    def __init__(self):
        try:
            self.fclass = arcpy.ListFeatureClasses('*{}'.format(fcName_ProjectJob))[0]
        except:
            pass
##        for fc in arcpy.ListFeatureClasses():
##            arcpy.AddMessage("Class: {}".format(fc))
##        arcpy.AddMessage("ProjectJob Feature Class = '{}'".format(self.fclass))


    def addOrUpdateProject(self, wmx_job_id, project_Id, alias, alias_clean, state , year , parent_dir, archive_dir, project_dir, UID, project_AOI):
        # Where WMXJobID = '<Job_ID>'
        where_clause = "{} = {}".format(arcpy.AddFieldDelimiters(self.fclass, field_ProjectJob_WMXJobID), wmx_job_id)

        row = [
               UID,  # field_ProjectJob_UID
               wmx_job_id,  # field_ProjectJob_WMXJobID,
               project_Id,  # field_ProjectJob_ProjID,
               alias,  # field_ProjectJob_Alias
               alias_clean,  # field_ProjectJob_AliasClean
               state ,  # field_ProjectJob_State
               year ,  # field_ProjectJob_Year
               parent_dir,  # field_ProjectJob_ParentDir
               archive_dir,  # field_ProjectJob_ArchDir
               project_dir,  # field_ProjectJob_ProjDir
               project_AOI  # field_ProjectJob_SHAPE
               ]


        Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause, rowValueList=row)

        return row

    def updateJobAOI(self, project_row, project_AOI):
        wmx_job_id = self.getWMXJobID(project_row)
        # Where WMXJobID = '<Job_ID>'
        where_clause = "{} = {}".format(arcpy.AddFieldDelimiters(self.fclass, field_ProjectJob_WMXJobID), wmx_job_id)
        project_row = list(project_row)
        index = self.fields.index(field_ProjectJob_SHAPE)
        project_row[index] = project_AOI

        Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause, rowValueList=project_row)

        return project_row

    def getProject(self, wmx_job_id):
        where_clause = "{} = {}".format(arcpy.AddFieldDelimiters(self.fclass, field_ProjectJob_WMXJobID), wmx_job_id)
        arcpy.AddMessage("Getting Project from '{}' where '{}'".format(self.fclass, where_clause))
        row, UID = Utility.getExistingRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause)

        return row, UID


    def getArchiveDir(self, row):
        return getFieldValue(self, row, field_ProjectJob_ArchDir)

    def getProjectDir(self, row):
        return getFieldValue(self, row, field_ProjectJob_ProjDir)

    def getProjectID(self, row):
        return getFieldValue(self, row, field_ProjectJob_ProjID)

    def getWMXJobID(self, row):
        return getFieldValue(self, row, field_ProjectJob_WMXJobID)

    def getAlias(self, row):
        return getFieldValue(self, row, field_ProjectJob_Alias)

    def getAliasClean(self, row):
        return getFieldValue(self, row, field_ProjectJob_AliasClean)

    def getParentDir(self, row):
        return getFieldValue(self, row, field_ProjectJob_ParentDir)

    def getState(self, row):
        return getFieldValue(self, row, field_ProjectJob_State)

    def getYear(self, row):
        return getFieldValue(self, row, field_ProjectJob_Year)

    def getUID(self, row):
        return getFieldValue(self, row, field_ProjectJob_UID)

    def getSHAPE(self, row):
        return getFieldValue(self, row, field_ProjectJob_SHAPE)

class Contract(object):

    fclass = None
    uid_index = uid_index_Contract
    fields = fields_Contract

    def __init__(self):
        self.fclass = arcpy.ListFeatureClasses('*{}'.format(fcName_Contract))[0]


    def getContract(self, project_id):
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_Contract_ProjID), project_id)

        row, UID = Utility.getExistingRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause)  # @UnusedVariable

        return row

    def addOrUpdateProject(self, project_ID, project_UID, project_AOI):
        # Where ProjectID = '<project_ID>'
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_Contract_ProjID), project_ID)
        arcpy.AddMessage("Checking to see if {} exists in CMDR Contracts".format(project_ID))
        row_Contracts, project_UID = Utility.getExistingRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause)
        if row_Contracts is None:
            project_UID = Utility.createUid(None)
            arcpy.AddMessage("Existing row not found, creating new one with UID {}".format(project_UID))
        else:
            if project_UID is None or len(project_UID) < 5:
                project_UID = Utility.createUid(None)
                arcpy.AddMessage("Found existing row {} with invalid UID. New UID {}".format(row_Contracts, project_UID))
            else:
                arcpy.AddMessage("Found existing row {} with UID {}".format(row_Contracts, project_UID))

        row_Contract = [project_UID,  # UID
                        project_ID,  # ProjectID
                        project_AOI]  # SHAPE@
        Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause, rowValueList=row_Contract)

        return row_Contract


    def updateAOI(self, row, aoi):
        project_id = row[self.fields.index(field_Contract_ProjID)]
        return updateProjIDAOI(project_id, row, aoi, self.fclass, self.fields, self.uid_index)

    def getProjectID(self, row_Contract):
        return getFieldValue(self, row_Contract, field_Contract_ProjID)

    def getProjectUID(self, row_Contract):
        return getFieldValue(self, row_Contract, field_Contract_UID)

    def getProjectAOI(self, row_Contract):
        return getFieldValue(self, row_Contract, field_Contract_SHAPE)



class RasterFileStat(object):

    fclass = None
    uid_index = uid_index_RasterFileStat
    fields = fields_RasterFileStat

    def __init__(self):
        self.fclass = arcpy.ListFeatureClasses('*{}'.format(fcName_RasterFileStat))[0]


    def getRasterFileStat(self, project_id):
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_RasterFileStat_ProjID), project_id)

        row, UID = Utility.getExistingRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, where_clause=where_clause)  # @UnusedVariable

        return row


    def updateAOI(self, row, aoi):
        project_id = row[self.fields.index(field_RasterFileStat_ProjID)]
        return updateProjIDAOI(project_id, row, aoi, self.fclass, self.fields, self.uid_index)


    def saveOrUpdateRasterFileStat(self, row, project_id, file_name, elevation_type, group):
        ''' NOTE: This depends on the fields being in the correct order!
            @see: CMDRConfig.fields_RasterFileStat for the correct order
        '''
        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_RasterFileStat_ProjID), project_id)
        where_clause = "{} and {} = '{}'".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_RasterFileStat_Name), file_name)
        where_clause = "{} and {} = '{}'".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_RasterFileStat_ElevType), elevation_type)
        where_clause = "{} and {} = '{}'".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_RasterFileStat_Group), group)
        return Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, rowValueList=row, where_clause=where_clause)

    def setProjID(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_ProjID, value)
    def getProjID(self, row):
        return getFieldValue(self, row, field_Deliver_ProjID)
    def setUID(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_UID, value)
    def getUID(self, row):
        return getFieldValue(self, row, field_RasterFileStat_UID)
    def setName(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Name, value)
    def getName(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Name)
    def setPath(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Path, value)
    def getPath(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Path)
    def setGroup(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Group, value)
    def getGroup(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Group)
    def setElevType(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_ElevType, value)
    def getElevType(self, row):
        return getFieldValue(self, row, field_RasterFileStat_ElevType)
    def setFormat(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Format, value)
    def getFormat(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Format)
    def setNoData(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_NoData, value)
    def getNoData(self, row):
        return getFieldValue(self, row, field_RasterFileStat_NoData)
    def setPixelType(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_PixelType, value)
    def getPixelType(self, row):
        return getFieldValue(self, row, field_RasterFileStat_PixelType)
    def setValueType(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_ValueType, value)
    def getValueType(self, row):
        return getFieldValue(self, row, field_RasterFileStat_ValueType)
    def setMin(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Min, value)
    def getMin(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Min)
    def setMax(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Max, value)
    def getMax(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Max)
    def setMean(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Mean, value)
    def getMean(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Mean)
    def setStd(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Std, value)
    def getStd(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Std)
    def setTop(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Top, value)
    def getTop(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Top)
    def setBottom(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Bottom, value)
    def getBottom(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Bottom)
    def setRight(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Right, value)
    def getRight(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Right)
    def setLeft(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Left, value)
    def getLeft(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Left)
    def setCellSizeX(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_CellSizeX, value)
    def getCellSizeX(self, row):
        return getFieldValue(self, row, field_RasterFileStat_CellSizeX)
    def setCellSizeY(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_CellSizeY, value)
    def getCellSizeY(self, row):
        return getFieldValue(self, row, field_RasterFileStat_CellSizeY)
    def setColumns(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Columns, value)
    def getColumns(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Columns)
    def setRows(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Rows, value)
    def getRows(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Rows)
    def setBands(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_Bands, value)
    def getBands(self, row):
        return getFieldValue(self, row, field_RasterFileStat_Bands)
    def setHorzProj(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_HorzProj, value)
    def getHorzProj(self, row):
        return getFieldValue(self, row, field_RasterFileStat_HorzProj)
    def setHorzUnit(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_HorzUnit, value)
    def getHorzUnit(self, row):
        return getFieldValue(self, row, field_RasterFileStat_HorzUnit)
    def setHorzWKID(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_HorzWKID, value)
    def getHorzWKID(self, row):
        return getFieldValue(self, row, field_RasterFileStat_HorzWKID)
    def setVertProj(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_VertProj, value)
    def getVertProj(self, row):
        return getFieldValue(self, row, field_RasterFileStat_VertProj)
    def setVertUnit(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_VertUnit, value)
    def getVertUnit(self, row):
        return getFieldValue(self, row, field_RasterFileStat_VertUnit)
    def setVertWKID(self, row, value):
        return setFieldValue(self, row, field_RasterFileStat_VertWKID, value)
    def getVertWKID(self, row):
        return getFieldValue(self, row, field_RasterFileStat_VertWKID)

class LASDStatInfo(object):

    fclass = None
    uid_index = uid_index_LASDStatInfo
    fields = fields_LASDStatInfo

    def __init__(self):
        self.fclass = arcpy.ListFeatureClasses('*{}'.format(fcName_LASDStatInfo))[0]

    def saveOrUpdate(self, row):
        project_id = row[self.fields.index(field_LASFileInfo_ProjID)]
        file_name = row[self.fields.index(field_LASFileInfo_File_Name)]
        category = row[self.fields.index(field_LASDStatInfo_Category)]
        item = row[self.fields.index(field_LASDStatInfo_Item)]


        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_ProjID), project_id)
        where_clause = "{} and {} = '{}'".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_File_Name), file_name)
        where_clause = "{} and {} = '{}'".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_LASDStatInfo_Category), category)
        where_clause = "{} and {} = '{}'".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_LASDStatInfo_Item), item)
        return Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, rowValueList=row, where_clause=where_clause)



class LASFileInfo(object):

    fclass = None
    uid_index = CMDRConfig.uid_index_LASFileInfo
    fields = CMDRConfig.fields_LASFileInfo

    def __init__(self):
        self.fclass = arcpy.ListFeatureClasses('*{}'.format(CMDRConfig.fcName_LASFileInfo))[0]

    def saveOrUpdate(self, row):
        project_id = row[self.fields.index(field_LASFileInfo_ProjID)]
        file_name = row[self.fields.index(field_LASFileInfo_File_Name)]
        las_class = row[self.fields.index(field_LASFileInfo_File_LAS_Class)]


        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_ProjID), project_id)
        where_clause = "{} and {} = '{}'".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_File_Name), file_name)
        if las_class is None:
            where_clause = "{} and {} is null".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_File_LAS_Class), las_class)
        else:
            where_clause = "{} and {} = {}".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_File_LAS_Class), las_class)

        return Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, rowValueList=row, where_clause=where_clause)



class LASFileSummary(object):

    fclass = None
    uid_index = CMDRConfig.uid_index_LASFileSum
    fields = CMDRConfig.fields_LASFileSum

    def __init__(self):
        self.fclass = arcpy.ListFeatureClasses('*{}'.format(CMDRConfig.fcName_LASFileSummary))[0]

    def saveOrUpdate(self, row):
        project_id = row[self.fields.index(field_LASFileInfo_ProjID)]
        file_name = row[self.fields.index(field_LASFileInfo_File_Name)]
        las_class = row[self.fields.index(field_LASFileInfo_File_LAS_Class)]


        where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_ProjID), project_id)
        where_clause = "{} and {} = '{}'".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_File_Name), file_name)
        if las_class is None:
            where_clause = "{} and {} is null".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_File_LAS_Class), las_class)
        else:
            where_clause = "{} and {} = {}".format(where_clause, arcpy.AddFieldDelimiters(self.fclass, field_LASFileInfo_File_LAS_Class), las_class)


        return Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, rowValueList=row, where_clause=where_clause)


class MDMaster(object):

    fclass = None
    uid_index = CMDRConfig.uid_index_MDMaster
    fields = CMDRConfig.fields_MDMaster

    def __init__(self):
        self.fclass = arcpy.ListFeatureClasses('*{}'.format(CMDRConfig.fcName_MDMaster))[0]

    def getMDMaster(self, jobID):
        where_clause = "{} = {}".format(arcpy.AddFieldDelimiters(self.fclass, field_MDMaster_WMXJobID), jobID)
        row, strUID = Utility.getExistingRecord(self.fclass, self.fields, self.uid_index, where_clause)  # @UnusedVariable
        return row

    def addOrUpdate(self, wmx_job_ID, parent_dir, master_name , masterCellSize_m, masterServerConnectionFilePath, masterServiceFolder, master_AOI):
        # Create project directory path
        masterDir = parent_dir
        if masterServiceFolder is not None and len(masterServiceFolder) > 0:
            masterDir = os.path.join(masterDir, masterServiceFolder)
        if master_name is not None and len(master_name) > 0:
            masterDir = os.path.join(masterDir, master_name)

        where_clause = "{} = {}".format(arcpy.AddFieldDelimiters(self.fclass, field_MDMaster_WMXJobID), wmx_job_ID)
        row = [
                master_AOI,
                wmx_job_ID,
                master_name,
                masterDir,
                parent_dir,
                masterCellSize_m,
                masterServerConnectionFilePath,
                masterServiceFolder
              ]
        return Utility.addOrUpdateRecord(in_table=self.fclass, field_names=self.fields, uidIndex=self.uid_index, rowValueList=row, where_clause=where_clause)

    def getMDParentPath(self, mdMaster_row):
        return mdMaster_row[self.fields.index(field_MDMaster_ParentPath)]

    def getMDPath(self, mdMaster_row):
        return mdMaster_row[self.fields.index(field_MDMaster_Path)]

    def getMDName(self, mdMaster_row):
        return mdMaster_row[self.fields.index(field_MDMaster_Name)]

    def MasterMDAOI(self, mdMaster_row):
        return mdMaster_row[self.fields.index(field_MDMaster_SHAPE)]

    def getMDServiceFolder(self, mdMaster_row):
        return mdMaster_row[self.fields.index(field_MDMaster_Folder)]

    def getMDCellSize(self, mdMaster_row):
        return mdMaster_row[self.fields.index(field_MDMaster_CellSize)]

    def getMDConFilePath(self, mdMaster_row):
        return mdMaster_row[self.fields.index(field_MDMaster_ConFile_Path)]

    def getExistingMDRow(self, wmx_job_ID):
        where_clause = "{} = {}".format(arcpy.AddFieldDelimiters(self.fclass, field_MDMaster_WMXJobID), wmx_job_ID)
        return Utility.getExistingRecord(in_table=self.fclass, field_names=self.fields, uidIndex=CMDRConfig.uid_index_MDMaster, where_clause=where_clause)[0]

    def getMDfgdbName(self, mdMaster_row, md_name):
        local_fgdb_name = "{}_{}.gdb".format(self.getMDName(mdMaster_row), md_name)
        arcpy.AddMessage("local_fgdb_name '{}'".format(local_fgdb_name))

    def getMDfgdbPath(self, mdMaster_row, md_name):
        local_fgdb_path = os.path.join(self.getMDPath(mdMaster_row), self.getMDfgdbName(mdMaster_row))
        arcpy.AddMessage("local_fgdb_path '{}'".format(local_fgdb_path))



