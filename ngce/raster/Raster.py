'''
Created on Feb 5, 2016

@author: eric5946
'''
import arcpy
from os import listdir
import os
from os.path import isfile, join

from ngce import Utility
from ngce.cmdr import CMDRConfig 
from ngce.raster import RasterConfig


boundary_interval = "{} Meters".format(RasterConfig.SIMPLIFY_INTERVAL)  # Length to simplify the boundary shapes
Intl_ft2mtrs_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.Intl_ft2mtrs_function_chain)
Us_ft2mtrs_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.US_ft2mtrs_function_chain)
Height_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.Height_1_function_chain)
Canopy_Density_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.Canopy_Density_function_chain)
    
arcpy.env.overwriteOutput = False
arcpy.env.pyramid = RasterConfig.NONE
arcpy.env.resamplingmethod = RasterConfig.BILINEAR
arcpy.env.compression = RasterConfig.COMPRESSION_LZ77
arcpy.env.tileSize = RasterConfig.TILE_SIZE_256
arcpy.env.nodata = RasterConfig.NODATA_DEFAULT

def getServerSideFunctions(folderPath):
    result = []
#     localPath = os.path.dirname(os.path.realpath(__file__))
#     toolIndex = localPath.lower().rfind("ngce")
#     localPath = localPath[toolIndex:]
#     uncPath = os.path.join(rootPath, "tools",localPath)
#     
    folderPath = os.path.join(str(folderPath))
    serverFunctionFiles = [f for f in listdir(folderPath) if isfile(join(str(folderPath), f))]
    for serverFunctionFile in serverFunctionFiles:
        if(serverFunctionFile.endswith(".xml")):
            result.append(os.path.join(folderPath, serverFunctionFile))
    return ";".join(result)
    
def getOverviewCellSize(cell_size):
    index = 0
    while RasterConfig.AGO_CELLSIZE_METERS[index] < cell_size:
        index = index + 1
    return  RasterConfig.AGO_CELLSIZE_METERS[index]

def getRasterSpatialReferenceInfo(inputRaster):
    spatialReference = arcpy.Describe(inputRaster).spatialReference
    
    horz_cs_wkid = spatialReference.PCSCode
    horz_cs_name = spatialReference.name
    horz_unit_name = spatialReference.linearUnitName
    if horz_cs_wkid is None or horz_cs_wkid <= 0:
        horz_cs_wkid = spatialReference.GCSCode
    
    vert_cs_name, vert_unit_name = Utility.getVertCSInfo(spatialReference)
    
    del spatialReference
    
    if horz_cs_wkid is None or horz_cs_wkid < 0:
        horz_cs_wkid = 0
    
    return horz_cs_name, horz_unit_name, horz_cs_wkid, vert_cs_name, vert_unit_name


def getRasterProperties(rasterObjectPath, newRow):
    cellSize = 0
    for PropertyType in CMDRConfig.Raster_PropertyTypes:
        try:
            propValue = arcpy.GetRasterProperties_management(rasterObjectPath, PropertyType)
            if propValue is not None:
                propValue = propValue[0]
            newRow.append(propValue)
            Utility.addToolMessages()
            if PropertyType == "CELLSIZEX":
                cellSize = newRow[len(newRow) - 1]
        except:
            Utility.addToolMessages()
            # Print error message if an error occurs
            newRow.append(None)
    
    
    return cellSize


def getRasterStats(ProjectUID, ProjectID, curr_raster, raster_path, group, elevation_type, raster_format, raster_PixelType, nodata, horz_cs_name, horz_unit_name, horz_cs_wkid, vert_cs_name, vert_unit_name, rows):
    # NOTE: Order here must match field list in CMDRConfig
    
    inMem_NameBound = "in_memory\MemBoundary"
    if arcpy.Exists(inMem_NameBound):
        arcpy.Delete_management(inMem_NameBound)
        Utility.addToolMessages()
    arcpy.RasterDomain_3d(raster_path, inMem_NameBound, "POLYGON")[0]
    Utility.addToolMessages()
    boundary = Utility.getExistingRecord(in_table=inMem_NameBound, field_names=[ 'SHAPE@'], uidIndex=-1)[0][0]
    
    newRow = [ProjectUID, ProjectID, boundary, curr_raster, raster_path, group, elevation_type, raster_format, nodata, raster_PixelType]
    
    arcpy.CalculateStatistics_management(in_raster_dataset=raster_path, skip_existing="OVERWRITE")
    cellSize = getRasterProperties(raster_path, newRow)
    
    newRow.append(horz_cs_name)
    newRow.append(horz_unit_name)
    newRow.append(horz_cs_wkid)
    newRow.append(vert_cs_name)
    newRow.append(vert_unit_name)
    newRow.append(None)  # Vert WKID, we can't know this in python
    
    rows.append(newRow)
    return cellSize

def addStandardMosaicDatasetFields(md_path):
    arcpy.AddMessage("Adding fields to Mosaic Dataset '{}'".format(md_path))
    # Add the required metadata fields to the Master/Project Mosaic Dataset
#     Utility.addAndCalcFieldText(dataset_path=md_path, field_name=CMDRConfig.PROJECT_ID, field_length="100", field_alias=CMDRConfig.PROJECT_ID.replace("_", " "), add_index=True)
    Utility.addAndCalcFieldDate(dataset_path=md_path, field_name=CMDRConfig.PROJECT_DATE, field_alias=CMDRConfig.PROJECT_DATE.replace("_", " "), add_index=True)
    Utility.addAndCalcFieldText(dataset_path=md_path, field_name=CMDRConfig.RASTER_PATH, field_length="800", field_alias=CMDRConfig.RASTER_PATH.replace("_", " "), add_index=True)
    Utility.addAndCalcFieldText(dataset_path=md_path, field_name=CMDRConfig.PROJECT_SOURCE, field_length="20", field_alias=CMDRConfig.PROJECT_SOURCE.replace("_", " "), add_index=True)
#     Utility.addAndCalcFieldText(dataset_path=md_path, field_name=CMDRConfig.PROJECT_SR_XY_NAME, field_length="100", field_alias=CMDRConfig.PROJECT_SR_XY_NAME.replace("_", " "), add_index=True)
#     Utility.addAndCalcFieldText(dataset_path=md_path, field_name=CMDRConfig.PROJECT_SR_XY_UNITS, field_length="20", field_alias=CMDRConfig.PROJECT_SR_XY_UNITS.replace("_", " "), add_index=True)
#     Utility.addAndCalcFieldText(dataset_path=md_path, field_name=CMDRConfig.PROJECT_SR_XY_CODE, field_length="100", field_alias=CMDRConfig.PROJECT_SR_XY_CODE.replace("_", " "), add_index=True)
#     Utility.addAndCalcFieldText(dataset_path=md_path, field_name=CMDRConfig.PROJECT_SR_Z_NAME, field_length="100", field_alias=CMDRConfig.PROJECT_SR_Z_NAME.replace("_", " "), add_index=True)
#     Utility.addAndCalcFieldText(dataset_path=md_path, field_name=CMDRConfig.PROJECT_SR_Z_UNITS, field_length="100", field_alias=CMDRConfig.PROJECT_SR_Z_UNITS.replace("_", " "), add_index=True)
#     Utility.addAndCalcFieldText(dataset_path=md_path, field_name=CMDRConfig.FILE_NAME, field_length="100", field_alias=CMDRConfig.FILE_NAME.replace("_", " "), add_index=True)
    
    arcpy.EnableEditorTracking_management(in_dataset=md_path, creator_field="created_user", creation_date_field="created_date", last_editor_field="last_edited_user", last_edit_date_field="last_edited_date", add_fields="ADD_FIELDS", record_dates_in="UTC")
    Utility.addToolMessages()


# Deptrecated in favor of a domain value in the WMX database
# def getServerRasterFunctionsPath(jobId):
#     Utility.setWMXJobDataAsEnvironmentWorkspace(jobId)
#     
#     domain_name = 'ServerFunctionPath'
#     table = r"in_memory/{}".format(domain_name)
#     # Create a table in memory to hold the codes and their descriptions for a particular domain.
#     fields = ['codeField', 'descriptionField']
#     found = False
#     serverFunctionPath = None
#     try:
#         cvdTable = arcpy.DomainToTable_management(arcpy.env.workspace, domain_name, table, fields[0], fields[1])  # @UndefinedVariable
#         Utility.addToolMessages()
#         # Create a cursor to loop through the table holding the domain and code info
#         rows = arcpy.SearchCursor(cvdTable)
#         # Loop through each row in the table.
#         for row in rows:
#             serverFunctionPath = row.codeField
#             del row
#             if serverFunctionPath is not None and len(serverFunctionPath) > 0:
#                 found = True
#                 arcpy.AddMessage("Using Server Raster Function Path {}".format(serverFunctionPath))
#         
#         del rows
#         arcpy.Delete_management(cvdTable)
#     except:
#         arcpy.AddError("Coded value Text Domain '{}' not found in CMDR database. Please add it.".format(domain_name))
#     if not found:
#         arcpy.AddError("Server Side Raster Function Path not found. In the CMDR database, please set Coded value Text Domain '{}' with a code that contains a UNC Path that your ArcGIS Server/WMX Tools can access".format(domain_name))
#     return serverFunctionPath
