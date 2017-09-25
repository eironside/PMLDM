'''
Created on Apr 14, 2016

@author: eric5946
'''
import os


SKIP_FACTOR = 10

CONTOUR_SCALES_LIST = [9027.9774109999998, 4513.9887049999998, 2256.994353, 1128.497176]
CONTOUR_SCALES_STRING = "9027.977411;4513.988705;2256.994353;1128.497176"
CONTOUR_SCALES_NUM = 4
CONTOUR_2FT_SERVICE_NAME = "CONT_2FT"
CACHE_INSTANCES = 6  # This should be increased based on server resources
CACHE_FOLDER = "E:/arcgisserver/directories/arcgiscache"

CONTOUR_NAME_OCS = 'Contours_OCS'
CONTOUR_NAME_WM = 'Contours_WM'

CONTOUR_INTERVAL = 2
CONTOUR_UNIT = "FOOT_US"
CONTOUR_SMOOTH_UNIT = 0.0001  # Decimal degrees. Larger values create smoother contours
DISTANCE_TO_CLIP_MOSAIC_DATASET = 200  # Meters. Note if too small, contours from different tiles wont smooth together
DISTANCE_TO_CLIP_CONTOURS = 5  # Meters. Note larger numbers will create too much overlap
CONTOUR_GDB_NAME = r"Contours.gdb"
CONTOUR_RESOURCE_FOLDER = 'contourResources'
CONTOUR_MXD_NAME = r"PublishContours.mxd"

MXD_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTOUR_RESOURCE_FOLDER, "PrepareContoursForPublishingTemplate.mxd")
MXD_ANNO_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTOUR_RESOURCE_FOLDER, "TileAnnotationTemplate.mxd")
MERGED_FGDB_NAME = r"{}_CONT.gdb"
CONTOUR_FC_WEBMERC = r"CONT_2FT_WM"
CONTOUR_BOUND_FC_WEBMERC = r"BOUNDARY_CONT_2FT_WM"


TILING_SCHEME = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTOUR_RESOURCE_FOLDER, "NRCS_tilingScheme.xml")
SYMBOLOGY_LAYER_NAME = r"MaskSymbology.lyr"
SYMBOLOGY_LAYER_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTOUR_RESOURCE_FOLDER, SYMBOLOGY_LAYER_NAME)

EMPTY_MASTER_MPK = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTOUR_RESOURCE_FOLDER, "emptyMaster.mpk")

WEB_AUX_SPHERE = "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0],AUTHORITY['EPSG',3857]],VERTCS['NAVD_1988',VDATUM['North_American_Vertical_Datum_1988'],PARAMETER['Vertical_Shift',0.0],PARAMETER['Direction',1.0],UNIT['Meter',1.0],AUTHORITY['EPSG',5703]];-20037700 -30241100 10000;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision"