'''
Created on Dec 21, 2015

@author: eric5946
'''

from ngce.raster import RasterConfig


status_deliver_INITIATED = "Initiated - Delivery Phase"

fcName_Contract = 'Contract'
fcName_ProjectJob = 'ProjectJob'
fcName_Deliver = 'Deliver'
fcName_QC = 'QC'
fcName_Publish = 'Publish'
fcName_RasterFileStat = 'RasterFileInfo'
fcName_LASFileInfo = "LASFileInfo"
fcName_LASFileSummary = "LASFileSummary"
fcName_LASDStatInfo = "LASDStatInfo"
fcName_MDMaster = "MDMaster"

DEFAULT_MDMASTER_NAME = "Elevation"

PROJECT_ID = 'Project_ID'  # <ST>_<Name>_<Year>
PROJECT_NAME = 'Project_Name'  # Clean alias
PROJECT_ALIAS = 'Project_Alias'
PROJECT_STATE = 'Project_State'
PROJECT_YEAR = 'Project_Year'
PROJECT_GUID = 'Project_GUID'
PROJECT_DIR = 'Project_Dir'
DIR_PARENT = 'Dir_Parent'
PROJECT_DIR_PARENT = 'Project_{}'.format(DIR_PARENT)
PROJECT_DIR_ARCHIVE = 'Project_Dir_Archive'
PROJECT_TASK = 'Project_Task'
PROJECT_TASK_OK = 'Project_Task_OK'
PROJECT_TASK_Message = 'Project_Task_Message'
PROJECT_DATE = "Project_Date"  # field_type="DATE"
PROJECT_SR_XY_NAME = "Project_SR_XY"  # field_type="TEXT", field_length="100"
PROJECT_SR_XY_CODE = "Project_SR_XY_Code"  # field_type="TEXT", field_length="20"
PROJECT_SR_XY_UNITS = "Project_SR_XY_Units"  # field_type="TEXT", field_length="20"
PROJECT_SR_Z_NAME = "Project_SR_Z"  # field_type="TEXT", field_length="100"
PROJECT_SR_Z_UNITS = "Project_SR_Z_Units"  # field_type="TEXT", field_length="20"


RASTER_PATH = "RasterPath"  # field_type="TEXT", field_length="512"
PROJECT_SOURCE = "Project_Source"  # field_type="TEXT", field_length="20"

TRANSMISSION_FIELDS = "Name;LowPS;CenterX;CenterY;{};{};{};{};{};{};{}".format(PROJECT_ID, PROJECT_DATE, PROJECT_SR_XY_NAME, PROJECT_SR_XY_CODE, PROJECT_SR_XY_UNITS, PROJECT_SR_Z_UNITS, PROJECT_SOURCE)
# "Name;MinPS;MaxPS;LowPS;HighPS;Tag;GroupName;ProductName;CenterX;CenterY;ZOrder;Shape_Length;Shape_Area;Project_ID;Project_Date;Porject_Source;Project_SR_XY;Project_SR_XY_Units;Project_SR_XY_Code;Project_SR_Z_Units"

NUM_ROWS = "NUMROWS"
STATUS = 'Status'
AREA = 'Area'
NAME = 'Name'
PIX_TYPE = 'PixelType'
NO_DATA = 'NoDataValue'
FORMAT = 'Format'
ELEV_TYPE = 'ElevationType'
GROUP = 'Group'
PATH = 'Path'
RASTER = 'Raster'

HORZ_PROJ = 'HorzProj'
HORZ_UNIT = 'HorzUnit'
HORZ_WKID = 'HorzWKID'
VERT_PROJ = 'VertProj'
VERT_UNIT = 'VertUnit'
VERT_WKID = 'VertWKID'

DTM = "DTM"
DSM = "DSM"
DHM = "DHM"
DLM = "DLM"
INT = "INTENSITY"
DATE = "Date"

OCS = "_OCS"

EXISTS = 'Exists'
EXTENT = 'Extent'

MIN_LON = 'MinLon'
MIN_LAT = 'MinLat'
MAX_LON = 'MaxLon'
MAX_LAT = 'MaxLat'

CELL_RESOLUTION = 'CellResolution'
PULSE_SPACING = 'PulseSpacing'
PULSE_DENSITY = 'PulseDensity'
POINT_SPACING = 'PointSpacing'
POINT_DENSITY = 'PointDensity'
COUNT_LAS = 'Count_LAS'
COUNT_POINT = 'PointCount'
COUNT_RASTER = 'Count_Raster'
VALID_Z_MIN = 'ValidZ_Min'
VALID_Z_MAX = 'ValidZ_Max'

LAS_CLASSIFIED = 'LAS_Classified'

COLLECTION_BEGIN = "CollectionBegin"
COLLECTION_END = "CollectionEnd"

CATEGORY = "Category"
ITEM = "Item"
PERCENT = "Percent" #SQL Keyword, don't use directly!
SYNTHETIC_PT_CNT = "Synthetic_Pt_Cnt"
RANGE_MIN = "Range_Min"
RANGE_MAX = "Range_Max"

FILE_NAME = "File_Name"
FILE_PATH = "File_Path"
FILE_LAS_CLASSIFIED = "File_{}".format(LAS_CLASSIFIED)
FILE_LAS_CLASS = "File_LAS_Class"
FILE_POINT_COUNT = "File_{}".format(COUNT_POINT)
FILE_POINT_SPACING = "File_{}".format(POINT_SPACING)
FILE_Z_MIN = "File_Z_Min"
FILE_Z_MAX = "File_Z_Max"
FILE_Z_RANGE = "File_Z_Range"
FILE_I_MIN = "File_I_Min"
FILE_I_MAX = "File_I_Max"
FILE_I_RANGE = "File_I_Range"
FILE_LINK_MAPSERVICE = "File_Link_MapService"

SHAPE = 'SHAPE@'
WMX_JOB_ID = 'WMX_JOB_ID'
NOTES = "Notes"
VERT_RMSE = "VertRMSE"
BOUNDARY = "Boundary"

LINK = "Link"
METADATA_XML = "MetadataXML"
USER_GUIDE = "UserGuide"

field_Contract_ProjID = PROJECT_ID
field_Contract_UID = PROJECT_GUID  
field_Contract_SHAPE = SHAPE


field_ProjectJob_WMXJobID = WMX_JOB_ID
field_ProjectJob_ProjID = PROJECT_ID
field_ProjectJob_Alias = PROJECT_ALIAS
field_ProjectJob_AliasClean = PROJECT_NAME
field_ProjectJob_State = PROJECT_STATE
field_ProjectJob_Year = PROJECT_YEAR
field_ProjectJob_ParentDir = PROJECT_DIR_PARENT
field_ProjectJob_ArchDir = PROJECT_DIR_ARCHIVE
field_ProjectJob_ProjDir = PROJECT_DIR
field_ProjectJob_UID = PROJECT_GUID  
field_ProjectJob_SHAPE = SHAPE
field_ProjectJob_Task = PROJECT_TASK 
field_ProjectJob_TaskOk = PROJECT_TASK_OK 
field_ProjectJob_TaskMsg = PROJECT_TASK_Message

field_Deliver_ProjID = PROJECT_ID
field_Deliver_UID = PROJECT_GUID  
field_Deliver_Status = "{}_{}".format(fcName_Deliver, STATUS)
field_Deliver_SHAPE = SHAPE
field_Deliver_Area = "{}_{}_M".format(fcName_Deliver, AREA)
field_Deliver_HorzProj = "{}_{}".format(fcName_Deliver, HORZ_PROJ)
field_Deliver_HorzUnit = "{}_{}".format(fcName_Deliver, HORZ_UNIT)
field_Deliver_HorzWKID = "{}_{}".format(fcName_Deliver, HORZ_WKID)
field_Deliver_VertProj = "{}_{}".format(fcName_Deliver, VERT_PROJ)
field_Deliver_VertUnit = "{}_{}".format(fcName_Deliver, VERT_UNIT)
field_Deliver_VertWKID = "{}_{}".format(fcName_Deliver, VERT_WKID)
field_Deliver_DTM_Exists = "{}_{}_{}".format(fcName_Deliver, EXISTS, DTM)
field_Deliver_DTM_CellRes = "{}_{}_{}".format(fcName_Deliver, DTM, CELL_RESOLUTION)
field_Deliver_DTM_CountRaster = "{}_{}_{}".format(fcName_Deliver, DTM, COUNT_RASTER)
field_Deliver_Date = "{}_{}".format(fcName_Deliver, DATE)
field_Deliver_Date_CollectionBegin = "{}_{}_{}".format(fcName_Deliver, DATE, COLLECTION_BEGIN)
field_Deliver_Date_CollectionEnd = "{}_{}_{}".format(fcName_Deliver, DATE, COLLECTION_END)


field_Deliver_DTM_PointSpacing = "{}_{}_{}".format(fcName_Deliver, DTM, POINT_SPACING)
field_Deliver_DTM_PointDensity = "{}_{}_{}".format(fcName_Deliver, DTM, POINT_DENSITY)
field_Deliver_DTM_Count_Points = "{}_{}_{}".format(fcName_Deliver, DTM, COUNT_POINT)
field_Deliver_DSM_Exists = "{}_{}_{}".format(fcName_Deliver, EXISTS, DSM)
field_Deliver_DSM_CellRes = "{}_{}_{}".format(fcName_Deliver, DSM, CELL_RESOLUTION)
field_Deliver_DSM_CountRaster = "{}_{}_{}".format(fcName_Deliver, DSM, COUNT_RASTER)

field_Deliver_DSM_PointSpacing = "{}_{}_{}".format(fcName_Deliver, DSM, POINT_SPACING)
field_Deliver_DSM_PointDensity = "{}_{}_{}".format(fcName_Deliver, DSM, POINT_DENSITY)
field_Deliver_DSM_Count_Points = "{}_{}_{}".format(fcName_Deliver, DSM, COUNT_POINT)
field_Deliver_Las_Classified = "{}_{}".format(fcName_Deliver, LAS_CLASSIFIED)
field_Deliver_Count_Las = "{}_{}".format(fcName_Deliver, COUNT_LAS)
field_Deliver_Count_Raster = "{}_{}".format(fcName_Deliver, COUNT_RASTER)  
field_Deliver_ValidZMin = "{}_{}".format(fcName_Deliver, VALID_Z_MIN)
field_Deliver_ValidZMax = "{}_{}".format(fcName_Deliver, VALID_Z_MAX)

field_Deliver_BoundXMin = "{}_{}_{}".format(fcName_Deliver, EXTENT, MIN_LON)
field_Deliver_BoundYMin = "{}_{}_{}".format(fcName_Deliver, EXTENT, MIN_LAT)
field_Deliver_BoundXMax = "{}_{}_{}".format(fcName_Deliver, EXTENT, MAX_LON)
field_Deliver_BoundYMax = "{}_{}_{}".format(fcName_Deliver, EXTENT, MAX_LAT)

field_Deliver_Notes = "{}_{}".format(fcName_Deliver,NOTES)
field_Deliver_VertRMSE = "{}_{}".format(fcName_Deliver,VERT_RMSE)
field_Deliver_Boundary_Exists = "{}_{}_{}".format(fcName_Deliver, EXISTS, BOUNDARY)
field_Deliver_Link_MetaXML = "{}_{}_{}".format(fcName_Deliver, LINK, METADATA_XML)
field_Deliver_Link_UserGuide ="{}_{}_{}".format(fcName_Deliver, LINK, USER_GUIDE)


field_QC_ProjID = PROJECT_ID
field_QC_UID = PROJECT_GUID  
field_QC_SHAPE = SHAPE

field_Publish_ProjID = PROJECT_ID
field_Publish_UID = PROJECT_GUID  
field_Publish_SHAPE = SHAPE

# Order matters for this class
field_RasterFileStat_ProjID = PROJECT_ID
field_RasterFileStat_UID = PROJECT_GUID
field_RasterFileStat_Name = "{}_{}".format(RASTER, NAME)
field_RasterFileStat_Path = "{}_{}".format(RASTER, PATH)
field_RasterFileStat_Group = "{}_{}".format(RASTER, GROUP)
field_RasterFileStat_ElevType = "{}_{}".format(RASTER, ELEV_TYPE)
field_RasterFileStat_Format = "{}_{}".format(RASTER, FORMAT)
field_RasterFileStat_NoData = "{}_{}".format(RASTER, NO_DATA)
field_RasterFileStat_HorzProj = "{}_{}".format(RASTER, HORZ_PROJ)
field_RasterFileStat_HorzUnit = "{}_{}".format(RASTER, HORZ_UNIT)
field_RasterFileStat_HorzWKID = "{}_{}".format(RASTER, HORZ_WKID)
field_RasterFileStat_VertProj = "{}_{}".format(RASTER, VERT_PROJ)
field_RasterFileStat_VertUnit = "{}_{}".format(RASTER, VERT_UNIT)
field_RasterFileStat_VertWKID = "{}_{}".format(RASTER, VERT_WKID)
field_RasterFileStat_PixelType = "{}_{}".format(RASTER, PIX_TYPE)
field_RasterFileStat_ValueType = RasterConfig.VALUETYPE
field_RasterFileStat_Min = RasterConfig.MINIMUM
field_RasterFileStat_Max = RasterConfig.MAXIMUM
field_RasterFileStat_Mean = RasterConfig.MEAN
field_RasterFileStat_Std = RasterConfig.STD
field_RasterFileStat_Unique = RasterConfig.UNIQUEVALUECOUNT
field_RasterFileStat_Top = "{}_Y".format(RasterConfig.TOP)
field_RasterFileStat_Bottom = "{}_Y".format(RasterConfig.BOTTOM)
field_RasterFileStat_Right = "{}_X".format(RasterConfig.RIGHT)
field_RasterFileStat_Left = "{}_X".format(RasterConfig.LEFT)
field_RasterFileStat_CellSizeX = RasterConfig.CELLSIZEX
field_RasterFileStat_CellSizeY = RasterConfig.CELLSIZEY
field_RasterFileStat_Columns = RasterConfig.COLUMNCOUNT
field_RasterFileStat_Rows = NUM_ROWS
field_RasterFileStat_Bands = RasterConfig.BANDCOUNT
field_RasterFileStat_SHAPE = SHAPE
# Order matters for this class

field_LASFileInfo_ProjID = PROJECT_ID
field_LASFileInfo_UID = PROJECT_GUID
field_LASFileInfo_File_Name = FILE_NAME
field_LASFileInfo_File_LAS_Class = FILE_LAS_CLASS
field_LASFileInfo_File_LAS_Classifed = FILE_LAS_CLASSIFIED
field_LASFileInfo_File_Path = FILE_PATH
field_LASFileInfo_File_PointCount = FILE_POINT_COUNT
field_LASFileInfo_File_PointSpacing = FILE_POINT_SPACING
field_LASFileInfo_File_Z_Max = FILE_Z_MAX
field_LASFileInfo_File_Z_Min = FILE_Z_MIN
field_LASFileInfo_File_Z_Range = FILE_Z_RANGE
field_LASFileInfo_File_Link_MapService = FILE_LINK_MAPSERVICE
field_LASFileInfo_File_I_Max = FILE_I_MAX
field_LASFileInfo_File_I_Min = FILE_I_MIN
field_LASFileInfo_File_I_Range = FILE_I_RANGE
field_LASFileInfo_SHAPE = SHAPE

field_LASFileSummary_File_LAS_Count = "File_{}".format(COUNT_LAS)

field_LASDStatInfo_SHAPE = SHAPE
field_LASDStatInfo_Category = CATEGORY
field_LASDStatInfo_Item = ITEM
field_LASDStatInfo_SyntheticPts = SYNTHETIC_PT_CNT
field_LASDStatInfo_Percent = "File_{}".format(PERCENT)
field_LASDStatInfo_RangeMin = RANGE_MIN
field_LASDStatInfo_RangeMax = RANGE_MAX

field_MDMaster_SHAPE = SHAPE
field_MDMaster_WMXJobID = WMX_JOB_ID
field_MDMaster_Name = "{}_{}".format(fcName_MDMaster, NAME)
field_MDMaster_Path = "{}_{}".format(fcName_MDMaster, PATH)
field_MDMaster_ParentPath = "{}_{}".format(fcName_MDMaster, DIR_PARENT)

field_MDMaster_CellSize = "{}_CellSize_M".format(fcName_MDMaster)
field_MDMaster_Folder = "{}_Folder".format(fcName_MDMaster)
field_MDMaster_ConFile_Path = "{}_ConFile_Path".format(fcName_MDMaster)


fields_Contract = [field_Contract_UID,
                   field_Contract_ProjID,
                   field_Contract_SHAPE 
                   ]
fields_ProjectJob = [field_ProjectJob_UID,
                     field_ProjectJob_WMXJobID,
                     field_ProjectJob_ProjID,
                     field_ProjectJob_Alias,
                     field_ProjectJob_AliasClean,
                     field_ProjectJob_State,
                     field_ProjectJob_Year,
                     field_ProjectJob_ParentDir,
                     field_ProjectJob_ArchDir,
                     field_ProjectJob_ProjDir,
                     field_ProjectJob_SHAPE
                     ]
fields_ProjectJob_Archive = [field_ProjectJob_WMXJobID,
                             field_ProjectJob_ProjID,
                             field_ProjectJob_ArchDir,
                             field_ProjectJob_ProjDir
                             ]
fields_Deliver_AddProject = [field_Deliver_UID,
                  field_Deliver_ProjID,
                  field_Deliver_SHAPE
                  ]
fields_Deliver = [field_Deliver_UID,
                  field_Deliver_ProjID,
                  field_Deliver_SHAPE,
                  field_Deliver_Status,
                  field_Deliver_Area,
                  field_Deliver_HorzProj,
                  field_Deliver_HorzUnit,
                  field_Deliver_HorzWKID,
                  field_Deliver_VertProj,
                  field_Deliver_VertUnit,
                  field_Deliver_VertWKID,
                  field_Deliver_DTM_Exists,
                  field_Deliver_DTM_CellRes,
                  field_Deliver_DTM_CountRaster,
                  field_Deliver_DTM_PointSpacing,
                  field_Deliver_DTM_PointDensity,
                  field_Deliver_DTM_Count_Points,
                  field_Deliver_DSM_Exists,
                  field_Deliver_DSM_CellRes,
                  field_Deliver_DSM_CountRaster,
                  field_Deliver_DSM_PointSpacing,
                  field_Deliver_DSM_PointDensity,
                  field_Deliver_DSM_Count_Points,
                  field_Deliver_Las_Classified,
                  field_Deliver_Count_Las,
                  field_Deliver_Count_Raster,
                  field_Deliver_ValidZMin,
                  field_Deliver_ValidZMax,
                  field_Deliver_BoundXMin,
                  field_Deliver_BoundYMin,
                  field_Deliver_BoundXMax,
                  field_Deliver_BoundYMax,
                  
                  
                  field_Deliver_Date_CollectionBegin,
                  field_Deliver_Date_CollectionEnd,
                  field_Deliver_Date,
                  field_Deliver_Notes,
                  field_Deliver_VertRMSE,
                  field_Deliver_Boundary_Exists,
                  field_Deliver_Link_MetaXML,
                  field_Deliver_Link_UserGuide
                  ]

fields_QC = [field_QC_UID,
             field_QC_ProjID,
             field_QC_SHAPE
             ]
fields_Publish = [field_QC_UID,
                  field_QC_ProjID,
                  field_QC_SHAPE
                 ]

fields_RasterFileStat = [
                        field_RasterFileStat_UID,
                        field_RasterFileStat_ProjID,
                        field_RasterFileStat_SHAPE,
                        field_RasterFileStat_Name,
                        field_RasterFileStat_Path,
                        field_RasterFileStat_Group,
                        field_RasterFileStat_ElevType,
                        field_RasterFileStat_Format,
                        field_RasterFileStat_NoData,
                        field_RasterFileStat_PixelType,
                        field_RasterFileStat_ValueType,
                        field_RasterFileStat_Min,
                        field_RasterFileStat_Max,
                        field_RasterFileStat_Mean,
                        field_RasterFileStat_Std,
                        # field_RasterFileStat_Unique,
                        field_RasterFileStat_Top,
                        field_RasterFileStat_Bottom,
                        field_RasterFileStat_Right,
                        field_RasterFileStat_Left,
                        field_RasterFileStat_CellSizeX,
                        field_RasterFileStat_CellSizeY,
                        field_RasterFileStat_Columns,
                        field_RasterFileStat_Rows,
                        field_RasterFileStat_Bands,
                        field_RasterFileStat_HorzProj,
                        field_RasterFileStat_HorzUnit,
                        field_RasterFileStat_HorzWKID,
                        field_RasterFileStat_VertProj,
                        field_RasterFileStat_VertUnit,
                        field_RasterFileStat_VertWKID
                         ]

fields_LASFileInfo = [
                        field_LASFileInfo_UID,
                        field_LASFileInfo_ProjID,
                        field_LASFileInfo_SHAPE,
                        field_LASFileInfo_File_Name,
                        field_LASFileInfo_File_LAS_Class,
                        field_LASFileInfo_File_LAS_Classifed,
                        field_LASFileInfo_File_Path,
                        field_LASFileInfo_File_PointCount,
                        field_LASFileInfo_File_PointSpacing,
                        field_LASFileInfo_File_Z_Max,
                        field_LASFileInfo_File_Z_Min,
                        field_LASFileInfo_File_Z_Range,
#                         field_LASFileInfo_File_Link_MapService
                     ]

fields_LASFileSum = [
                        field_LASFileInfo_UID,
                        field_LASFileInfo_ProjID,
                        field_LASFileInfo_SHAPE,
                        field_LASFileInfo_File_Name,
                        field_LASFileInfo_File_LAS_Class,
                        field_LASFileInfo_File_LAS_Classifed,
                        field_LASFileInfo_File_Path,
                        field_LASFileInfo_File_PointCount,
                        field_LASFileInfo_File_PointSpacing,
                        field_LASFileInfo_File_Z_Max,
                        field_LASFileInfo_File_Z_Min,
                        field_LASFileInfo_File_Z_Range,
#                         field_LASFileInfo_File_Link_MapService,
                        field_LASFileSummary_File_LAS_Count
                     ]

fields_LASDStatInfo = [
                        field_LASFileInfo_UID,
                        field_LASFileInfo_ProjID,
                        field_LASDStatInfo_SHAPE,
                        field_LASFileInfo_File_Name,
                        field_LASFileInfo_File_LAS_Classifed,
                        field_LASFileInfo_File_Path,
                        field_LASFileInfo_File_PointCount,
                        field_LASFileInfo_File_Z_Max,
                        field_LASFileInfo_File_Z_Min,
                        field_LASFileInfo_File_Z_Range,
                        field_LASFileInfo_File_I_Max,
                        field_LASFileInfo_File_I_Min,
                        field_LASFileInfo_File_I_Range,
                        field_LASDStatInfo_Category,
                        field_LASDStatInfo_Item,
                        field_LASDStatInfo_SyntheticPts,
                        field_LASDStatInfo_Percent,
                        field_LASDStatInfo_RangeMin,
                        field_LASDStatInfo_RangeMax
#                         field_LASFileInfo_File_Link_MapService
                     ]

fields_MDMaster = [
                    field_MDMaster_SHAPE,
                    field_MDMaster_WMXJobID,
                    field_MDMaster_Name,
                    field_MDMaster_Path,
                    field_MDMaster_ParentPath,
                    field_MDMaster_CellSize,
                    field_MDMaster_ConFile_Path,
                    field_MDMaster_Folder 
                   ]

uid_index_Contract = fields_Contract.index(field_Contract_UID)
uid_index_ProjectJob = fields_Contract.index(field_ProjectJob_UID)
uid_index_Deliver = fields_Contract.index(field_Deliver_UID)
uid_index_QC = fields_Contract.index(field_QC_UID)
uid_index_RasterFileStat = fields_RasterFileStat.index(field_RasterFileStat_UID)
uid_index_LASFileInfo = fields_LASFileInfo.index(field_LASFileInfo_UID)
uid_index_LASFileSum = fields_LASFileSum.index(field_LASFileInfo_UID)
uid_index_LASDStatInfo = fields_LASDStatInfo.index(field_LASFileInfo_UID)
uid_index_MDMaster = -1


Raster_PropertyTypes = RasterConfig.Raster_PropertyTypes
