'''
Created on Apr 14, 2016

@author: eric5946
'''
import os

CONTOUR_SCALES_LIST = [9027.9774109999998, 4513.9887049999998, 2256.994353, 1128.497176]
CONTOUR_SCALES_STRING = "9027.977411;4513.988705;2256.994353;1128.497176"
CONTOUR_SCALES_NUM = 4
CONTOUR_2FT_SERVICE_NAME = "CONT_2FT"
CACHE_INSTANCES = 2  # This should be increased based on server resources
CACHE_FOLDER = "E:/arcgisserver/directories/arcgiscache"

CONTOUR_INTERVAL = 2
CONTOUR_UNIT = "FEET"
CONTOUR_SMOOTH_UNIT = 0.0001  # Decimal degrees. Larger values create smoother contours
DISTANCE_TO_CLIP_MOSAIC_DATASET = 200  # Meters. Note if too small, contours from different tiles wont smooth together
DISTANCE_TO_CLIP_CONTOURS = 0.04  # Meters. Note larger numbers will create too much overlap
CONTOUR_GDB_NAME = r"Contours.gdb"
CONTOUR_RESOURCE_FOLDER = 'contourResources'
CONTOUR_MXD_NAME = r"PublishContours.mxd"

MXD_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTOUR_RESOURCE_FOLDER, "PrepareContoursForPublishingTemplate.mxd")
MERGED_FGDB_NAME = r"{}_CONT.gdb"
CONTOUR_FC_WEBMERC = r"CONT_2FT_WM"
CONTOUR_BOUND_FC_WEBMERC = r"BOUNDARY_CONT_2FT_WM"


TILING_SCHEME = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTOUR_RESOURCE_FOLDER, "NRCS_tilingScheme.xml")
SYMBOLOGY_LAYER_NAME = r"MaskSymbology.lyr"
SYMBOLOGY_LAYER_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTOUR_RESOURCE_FOLDER, SYMBOLOGY_LAYER_NAME)

EMPTY_MASTER_MPK = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTOUR_RESOURCE_FOLDER, "emptyMaster.mpk")
