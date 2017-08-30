'''
Created on Feb 5, 2016

@author: eric5946
'''
import os


NONE = "NONE"
BILINEAR = "BILINEAR"
COMPRESSION_LZ77 = "LZ77"
TILE_SIZE_256 = "256 256"

SIMPLIFY_INTERVAL = 3  # Meters

PROJECT_SOURCE_LAS = "LAS"
PROJECT_SOURCE_OVERVIEW = "OVR"
PROJECT_SOURCE_RASTER = "RAS"

ServerSideFunction_dir = "ServerSide_Functions"
MASTER_TEMP_RASTER_DIR="TempRaster"
MASTER_TEMP_RASTER_NAME="b3409805_ne_a.tif"

# Set the cell size of the first level overview according to the cell size of the Mosaic Dataset
# Do this by doubling cell size and finding the next ArcGIS Online cache scale
# Then, if caching is to be done on the Image Service it will be more efficient
# Note: to start building SO's at a smaller Cell Size (i.e. Larger scale) then reduce 2.5 to 2 
OVERVIEW_CELLSIZE_MULT = 2.5
AGO_CELLSIZE_METERS = [
                       0.597164,
                       1.194328,
                       2.388657,
                       4.777314,
                       9.554628,
                       19.10925,
                       38.21851,
                       76.43702,
                       152.8740,
                       305.74811
                       ]
INTERNAL_PART_AREA = 25  # percent



NODATA_340282346639E38 = "-3.40282346639e+38"
NODATA_340282306074E38 = "-3.40282306074e+38"
NODATA_DEFAULT = NODATA_340282346639E38 

Intl_ft2mtrs_function_chain = r"Intl_FeetToMeters.rft.xml"
US_ft2mtrs_function_chain = r"US_FeetToMeters.rft.xml"
Height_3_function_chain = r"Digital_Height_Model_3.rft.xml"
Height_1_function_chain = r"Digital_Height_Model_1.rft.xml"
Canopy_Density_function_chain = r"Canopy_Density.rft.xml"

Contour_Meters_function_chain = r"Prepare_Contour_Meters.rft.xml"
Contour_IntlFeet_function_chain = r"Prepare_Contour_IntlFeet.rft.xml"
Contour_Feet_function_chain = r"Prepare_Contour_USFeet.rft.xml"


SpatRef_WebMercator = "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0],AUTHORITY['EPSG',3857]],VERTCS['NAVD_1988',VDATUM['North_American_Vertical_Datum_1988'],PARAMETER['Vertical_Shift',0.0],PARAMETER['Direction',1.0],UNIT['Meter',1.0],AUTHORITY['EPSG',5703]];-20037700 -30241100 10000;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision"

# Don't Change these below, they come from the tool
VALUETYPE = 'VALUETYPE'
MINIMUM = 'MINIMUM'
MAXIMUM = 'MAXIMUM'
MEAN = 'MEAN'
STD = 'STD'

TOP = 'TOP'
LEFT = 'LEFT'
RIGHT = 'RIGHT'
BOTTOM = 'BOTTOM'
CELLSIZEX = 'CELLSIZEX'
CELLSIZEY = 'CELLSIZEY'
COLUMNCOUNT = 'COLUMNCOUNT'
ROWCOUNT = 'ROWCOUNT'
BANDCOUNT = 'BANDCOUNT'

ALLNODATA = "ALLNODATA"
ANYNODATA = "ANYNODATA"
SENSORNAME = "SENSORNAME"
PRODUCTNAME = "PRODUCTNAME"
ACQUISITIONDATE = "ACQUISITIONDATE"
SOURCETYPE = "SOURCETYPE"
CLOUDCOVER = "CLOUDCOVER"
SUNAZIMUTH = "SUNAZIMUTH"
SUNELEVATION = "SUNELEVATION"
SENSORAZIMUTH = "SENSORAZIMUTH"
SENSORELEVATION = "SENSORELEVATION"
OFFNADIR = "OFFNADIR"
WAVELENGTH = "WAVELENGTH"

UNIQUEVALUECOUNT = 'UNIQUEVALUECOUNT'
# Don't Change these above, they come from the tool
Raster_PropertyTypes = [VALUETYPE, MINIMUM, MAXIMUM, MEAN, STD, TOP, BOTTOM, RIGHT, LEFT, CELLSIZEX, CELLSIZEY, COLUMNCOUNT, ROWCOUNT, BANDCOUNT]

MASTER_MD_NAME = "MASTER"
MASTER_OVERVIEW_CELLSIZE = AGO_CELLSIZE_METERS[9]

MASTER_BOUNDARY_DIR = "US_BOUNDARY" 
MASTER_BOUNDARY_FDDB_NAME = "{}.gdb".format(MASTER_BOUNDARY_DIR)
MASTER_BOUNDARY_US_LOWER48 = "lower48WebMercator" 
MASTER_BOUNDARY_US_ALL50 = "USStatesPRWebMercator"
MASTER_BOUNDARY_US_ALL50_TER = ""  # NOT DEFINED YET. All states plus territories
MASTER_BOUNDARY_DEFAULT = MASTER_BOUNDARY_US_ALL50


MasterBoundaryFC = os.path.join(os.path.dirname(os.path.realpath(__file__)), MASTER_BOUNDARY_DIR, MASTER_BOUNDARY_FDDB_NAME, MASTER_BOUNDARY_DEFAULT)
MasterTempRaster = os.path.join(os.path.dirname(os.path.realpath(__file__)), MASTER_TEMP_RASTER_DIR, MASTER_TEMP_RASTER_NAME)

#@TODO: Need to determine what this should be
MOSAIC_Z_TOLERANCE = 100

STAT_LAS_FOLDER = os.path.join("STATS", "LAS")
STAT_RASTER_FOLDER = os.path.join("STATS", "RASTER")

STAT_FOLDER_ORG = "ORIGINAL"
STAT_FOLDER_DER = "DERIVED"
STAT_FOLDER_PUB = "PUBLISHED"

CANOPY_DENSITY = "MR_POINT_DENSITY"

SAMPLE_TYPE = "CELLSIZE"
CELL_SIZE = 10  # Meters
FOOTPRINT_BUFFER_DIST = 25  # Meters

PIXEL_TYPE_F32 = 'F32'
PIXEL_TYPE_D64 = 'D64'

BAND_COUNT = 'bandCount'  # Integer - The number of bands in the referenced raster dataset. 
COMP_TYPE = 'compressionType'  # String - The compression type. The following are the available types:LZ77,JPEG,JPEG 2000,PACKBITS,LZW,RLE,CCITT GROUP 3,CCITT GROUP 4,CCITT (1D),None. 
# EXTENT = 'extent'  # Extent - The extent of the referenced raster dataset.
HEIGHT = 'height'  # Integer - The number of rows.
WIDTH = 'width'  # Integer - The number of columns.
MEAN_CELL_HEIGHT = 'meanCellHeight'  # Double - The cell size in the y direction.
MEAN_CELL_WIDTH = 'meanCellWidth'  # Double - The cell size in the x direction.
IS_INT = 'isInteger'  # Boolean - The integer state: True if the raster dataset has integer type.
IS_TEMP = 'isTemporary'  # Boolean - The state of the referenced raster dataset: True if the raster dataset is temporary or False if permanent.
MEAN = 'mean'  # Double - The mean value in the referenced raster dataset.
MAX = 'maximum'  # Double - The maximum value in the referenced raster dataset.
MIN = 'minimum'  # Double - The minimum value in the referenced raster dataset.
STAND_DEV = 'standardDeviation'  # Double - The standard deviation of the values in the referenced raster dataset.
NODATA_VALUE = 'noDataValue'  # Double - The NoData value of the referenced raster dataset.
NAME = 'name'  # String - The name of the referenced raster dataset.
PATH = 'path'  # String - The full path and name of the referenced raster dataset.
# CAT_PATH = 'catalogPath'  # String - The full path and the name of the referenced raster dataset.
PIXEL_TYPE = 'pixelType'  # String - The pixel type of the referenced raster dataset.  
FORMAT = 'format'  # String - The raster format 
HAS_RAT = 'hasRAT'  # Boolean - Identifies if there is an associated attribute table: True if an attribute table exists or False if no attribute table exists.
SPAT_REF = 'spatialReference'  # SpatialReference - The spatial reference of the referenced raster dataset.
UNCOMP_SIZE = 'uncompressedSize'  # Double - The size of the referenced raster dataset on disk.

RANGE = "z_range"
AREA = "area"
XMIN = "XMin"
XMAX = "XMax"
YMIN = "YMin"
YMAX = "YMax"
IS_CLASSIFIED = "is_class"

V_NAME = "sr_v_name"
V_UNIT = "sr_v_unit"
H_NAME = "sr_h_name"
H_UNIT = "sr_h_unit"
H_WKID = "sr_h_wkid"

POINT_SPACING = "Pt_Spacing"
POINT_COUNT = "Pt_Cnt"
POINT_PERCENT = "Percent"

FIRST_RETURNS = "First"
SECOND_RETURNS = "Second"
THIRD_RETURNS = "Third"
FOURTH_RETURNS = "Fourth"
LAST_RETURNS = "Last"
SINGLE_RETURNS = "Single"
FIRST_OF_MANY_RETURNS = "First_of_Many"
LAST_OF_MANY_RETURNS = "Last_of_Many"
ALL_RETURNS = "All"
ELEV_TYPE = "Elevation_Type"

KEY_LIST = [NAME, PATH, MAX, MEAN, MIN, STAND_DEV, WIDTH, HEIGHT, MEAN_CELL_HEIGHT, MEAN_CELL_WIDTH, BAND_COUNT, COMP_TYPE, FORMAT, HAS_RAT, IS_INT, IS_TEMP, PIXEL_TYPE, UNCOMP_SIZE, XMIN, YMIN, XMAX, YMAX, V_NAME, V_UNIT, H_NAME, H_UNIT, H_WKID, NODATA_VALUE]

FIELD_INFO = {IS_CLASSIFIED: ["is_class", "Is Classified", "TEXT", "10"],
              AREA: ["area", "Area (sq meters)", "DOUBLE", ""],
              NAME : ["name", "Name", "TEXT", 100],
              PATH : ["path", "Path", "TEXT", 254],
              MAX : ["zmax", "Z (max)", "DOUBLE", ""],
              MEAN : ["zmean", "Z (mean)", "DOUBLE", ""],
              MIN : ["zmin", "Z (min)", "DOUBLE", ""],
              RANGE : ["zran", "Z (range)", "DOUBLE", ""],
              STAND_DEV : ["zdev", "Z (StDev)", "DOUBLE", ""],
              WIDTH : ["width", "Width", "DOUBLE", ""],
              HEIGHT : ["height", "Height", "DOUBLE", ""],
              MEAN_CELL_HEIGHT : ["cell_h", "Mean Cell Height", "DOUBLE", ""],
              MEAN_CELL_WIDTH : ["cell_w", "Mean Cell Width", "DOUBLE", ""],
              BAND_COUNT : ["bands", "Band Count", "SHORT", ""],
              COMP_TYPE : ["comp_type", "Compression", "TEXT", 50],
              FORMAT : ["format", "Format", "TEXT", 50],
              HAS_RAT : ["isRAT", "Has RAT", "TEXT", 50],
              IS_INT : ["isINT", "Is INT", "TEXT", 50],
              IS_TEMP : ["isTMP", "Is Temp", "TEXT", 50],
              PIXEL_TYPE : ["pixel", "Pixel Type", "TEXT", 100],
              UNCOMP_SIZE : ["unc_size", "Uncmpressed Size", "DOUBLE", ""],
              XMIN : ["xmin", "X (min)", "DOUBLE", ""],
              YMIN : ["ymin", "Y (min)", "DOUBLE", ""],
              XMAX : ["xmax", "X (max)", "DOUBLE", ""],
              YMAX : ["ymax", "Y (max)", "DOUBLE", ""],
              V_NAME : ["v_name", "Vertical CS Name", "TEXT", 100],
              V_UNIT : ["v_unit", "Vertical CS Unit", "TEXT", 100],
              H_NAME : ["h_name", "Horizontal CS Name", "TEXT", 100],
              H_UNIT : ["h_unit", "Horizontal CS Unit", "TEXT", 100],
              H_WKID : ["h_wkid", "Horizontal CS WKID", "TEXT", 100],
              NODATA_VALUE : ["nodata", "No Data Value", "DOUBLE", ""],
              
              
              
                FIRST_RETURNS : ["r1", "First Return", "DOUBLE", ""],
                SECOND_RETURNS : ["r2", "Second Return", "DOUBLE", ""],
                THIRD_RETURNS : ["r3", "Third Return", "DOUBLE", ""],
                FOURTH_RETURNS : ["r4", "Fourth Return", "DOUBLE", ""],
                LAST_RETURNS : ["rl", "Last Return", "DOUBLE", ""],
                SINGLE_RETURNS : ["rs", "Single Return", "DOUBLE", ""],
                FIRST_OF_MANY_RETURNS : ["rfm", "First of Many Return", "DOUBLE", ""],
                LAST_OF_MANY_RETURNS : ["rlm", "Last of Many Return", "DOUBLE", ""],
                ALL_RETURNS  : ["ra", "All Return", "DOUBLE", ""],
                
                POINT_SPACING   : ["pt_sp", "Point Spacing", "DOUBLE", ""],
                POINT_COUNT  : ["pt_ct", "Point Count", "DOUBLE", ""],
                POINT_PERCENT  : ["pt_pct", "Point Percent", "DOUBLE", ""],
                ELEV_TYPE  : ["el_type", "Elevation Type", "TEXT", "20"]



              }


