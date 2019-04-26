'''
Created on Feb 5, 2016

@author: eric5946
'''
import arcpy
from datetime import datetime
from os import listdir
import os
from os.path import isfile, join

from ngce import Utility
from ngce.Utility import deleteFileIfExists, doTime
from ngce.cmdr import CMDRConfig
from ngce.raster import RasterConfig
from ngce.raster.RasterConfig import BAND_COUNT, COMP_TYPE, FORMAT, HAS_RAT, \
    HEIGHT, IS_INT, IS_TEMP, MAX, MEAN, MEAN_CELL_HEIGHT, MEAN_CELL_WIDTH, MIN, \
    NAME, NODATA_VALUE, PATH, PIXEL_TYPE, SPAT_REF, STAND_DEV, UNCOMP_SIZE, \
    WIDTH, V_NAME, V_UNIT, H_NAME, H_UNIT, H_WKID, XMIN, YMIN, XMAX, YMAX, \
    KEY_LIST


boundary_interval = "{} Meters".format(RasterConfig.SIMPLIFY_INTERVAL)  # Length to simplify the boundary shapes
Intl_ft2mtrs_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.Intl_ft2mtrs_function_chain)
Us_ft2mtrs_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.US_ft2mtrs_function_chain)
Height_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.Height_1_function_chain)
Canopy_Density_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.Canopy_Density_function_chain)
Contour_Meters_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.Contour_Meters_function_chain)
Contour_IntlFeet_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.Contour_IntlFeet_function_chain)
Contour_Feet_function_chain_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), RasterConfig.Contour_Feet_function_chain)

arcpy.env.overwriteOutput = False
arcpy.env.pyramid = RasterConfig.NONE
arcpy.env.resamplingmethod = RasterConfig.BILINEAR
arcpy.env.compression = RasterConfig.COMPRESSION_LZ77
arcpy.env.tileSize = RasterConfig.TILE_SIZE_256
arcpy.env.nodata = RasterConfig.NODATA_DEFAULT

def getServerSideFunctions(folderPath):
    arcpy.AddMessage(folderPath)
    result = []
#     localPath = os.path.dirname(os.path.realpath(__file__))
#     toolIndex = localPath.lower().rfind("ngce")
#     localPath = localPath[toolIndex:]
#     uncPath = os.path.join(rootPath, "tools",localPath)
#
    #folderPath = os.path.join(str(folderPath))
    #arcpy.AddMessage(folderPath)
    serverFunctionFiles = [f for f in listdir(folderPath) if str(f).endswith(".xml")]#isfile(join(str(folderPath), f))]
    for serverFunctionFile in serverFunctionFiles:
        #if(serverFunctionFile.endswith(".xml")):
        result.append(os.path.join(folderPath, serverFunctionFile))

    arcpy.AddMessage(result)
    return ";".join(result)

def getOverviewCellSize(cell_size):
    index = 0
    while RasterConfig.AGO_CELLSIZE_METERS[index] < 2*cell_size:
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



'''
--------------------------------------------------------------------------------
Exports the image file statistics into a .txt file
--------------------------------------------------------------------------------
'''
def createRasterDatasetStats(f_path, stat_file_path=None):
    a = datetime.now()

    try:
        # this no data value doesn't apply to all rasters, but easier to just try and move on
        arcpy.SetRasterProperties_management(
            f_path, data_type="#",
            statistics="#",
            stats_file="#",
            nodata="1 {}".format(RasterConfig.NODATA_DEFAULT)
            )
    except:
        pass


    try:
        arcpy.CalculateStatistics_management(
            in_raster_dataset=f_path,
            x_skip_factor="1",
            y_skip_factor="1",
            ignore_values="",
            skip_existing="OVERWRITE",
            area_of_interest="Feature Set"
            )

    except Exception as e:
        arcpy.AddMessage('Could Not Calculate Statistics')
        arcpy.AddMessage(e)


    raster_properties = {}

    rasterObject = arcpy.Raster(f_path)
    raster_properties[BAND_COUNT] = rasterObject.bandCount  # Integer - The number of bands in the referenced raster dataset.
    # raster_properties['catalogPath'] = rasterObject.catalogPath  # String - The full path and the name of the referenced raster dataset.
    raster_properties[COMP_TYPE] = rasterObject.compressionType  # String - The compression type. The following are the available types:LZ77,JPEG,JPEG 2000,PACKBITS,LZW,RLE,CCITT GROUP 3,CCITT GROUP 4,CCITT (1D),None.
    # raster_properties[EXTENT] = rasterObject.extent  # Extent - The extent of the referenced raster dataset.
    raster_properties[FORMAT] = rasterObject.format  # String - The raster format
    raster_properties[HAS_RAT] = rasterObject.hasRAT  # Boolean - Identifies if there is an associated attribute table: True if an attribute table exists or False if no attribute table exists.
    raster_properties[HEIGHT] = rasterObject.height  # Integer - The number of rows.
    raster_properties[IS_INT] = rasterObject.isInteger  # Boolean - The integer state: True if the raster dataset has integer type.
    raster_properties[IS_TEMP] = rasterObject.isTemporary  # Boolean - The state of the referenced raster dataset: True if the raster dataset is temporary or False if permanent.
    raster_properties[MAX] = rasterObject.maximum  # Double - The maximum value in the referenced raster dataset.
    raster_properties[MEAN] = rasterObject.mean  # Double - The mean value in the referenced raster dataset.
    raster_properties[MEAN_CELL_HEIGHT] = rasterObject.meanCellHeight  # Double - The cell size in the y direction.
    raster_properties[MEAN_CELL_WIDTH] = rasterObject.meanCellWidth  # Double - The cell size in the x direction.
    raster_properties[MIN] = rasterObject.minimum  # Double - The minimum value in the referenced raster dataset.
    #Added to bypass zmin = 'None' error 15 April 2019 BJN
    if raster_properties[MIN] is None or raster_properties[MIN] < -285:
        raster_properties[MIN] = 0  # Double - The minimum value in the referenced raster dataset.

    raster_properties[NAME] = rasterObject.name  # String - The name of the referenced raster dataset.
    raster_properties[NODATA_VALUE] = rasterObject.noDataValue  # Double - The NoData value of the referenced raster dataset.
    raster_properties[PATH] = rasterObject.path  # String - The full path and name of the referenced raster dataset.
    raster_properties[PIXEL_TYPE] = rasterObject.pixelType  # String - The pixel type of the referenced raster dataset.
    raster_properties[SPAT_REF] = rasterObject.spatialReference  # SpatialReference - The spatial reference of the referenced raster dataset.
    raster_properties[STAND_DEV] = rasterObject.standardDeviation  # Double - The standard deviation of the values in the referenced raster dataset.
    raster_properties[UNCOMP_SIZE] = rasterObject.uncompressedSize  # Double - The size of the referenced raster dataset on disk.
    raster_properties[WIDTH] = rasterObject.width  # Integer - The number of columns.

    raster_properties[V_NAME] = None
    raster_properties[V_UNIT] = None
    raster_properties[H_NAME] = None
    raster_properties[H_UNIT] = None
    raster_properties[H_WKID] = None

    if rasterObject.spatialReference is not None:
        raster_properties[V_NAME] , raster_properties[V_UNIT] = Utility.getVertCSInfo(rasterObject.spatialReference)
        raster_properties[H_NAME] = rasterObject.spatialReference.name
        raster_properties[H_UNIT] = rasterObject.spatialReference.linearUnitName
        raster_properties[H_WKID] = rasterObject.spatialReference.factoryCode

    raster_properties[XMIN] = rasterObject.extent.XMin
    raster_properties[YMIN] = rasterObject.extent.YMin
    raster_properties[XMAX] = rasterObject.extent.XMax
    raster_properties[YMAX] = rasterObject.extent.YMax

    valList = []
    for key in KEY_LIST:
        valList.append(raster_properties[key])

    keyList = ','.join(KEY_LIST)
    for i, value in enumerate(valList):
        valList[i] = str(value)
    valList = ','.join(valList)

#     arcpy.AddMessage("\t{}".format(keyList))
#     arcpy.AddMessage("\t{}".format(valList))

    if stat_file_path is not None:
        # Output file
        deleteFileIfExists(stat_file_path)
        OutFile = open(stat_file_path, 'w')

        OutFile.write('{}\n{}\n'.format(keyList, valList))
        OutFile.close()

    doTime(a, "\tCreated STATS {}".format(stat_file_path))
    doTime(a, "\tCreated STATS {}".format(raster_properties))

    return raster_properties

