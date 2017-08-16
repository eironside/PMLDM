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


