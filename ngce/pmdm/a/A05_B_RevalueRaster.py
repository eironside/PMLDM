'''
Created on Jun 15, 2017

@author: eric5946
'''
import arcpy
from datetime import datetime
import os
import sys
import time

from ngce import Utility
from ngce.raster import RasterConfig


STAT_FOLDER = os.path.join("STATS", "RASTER")
STAT_FOLDER_ORG = "ORIGINAL"
STAT_FOLDER_DER = "DERIVED"
STAT_FOLDER_PUB = "PUBLISHED"

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


'''
--------------------------------------------------------------------------------
Determines if a file needs to be processed or not by looking at all the derivatives
--------------------------------------------------------------------------------
'''
def isProcessFile(f_path, elev_type, target_path, publish_path):
    process_file = False
    
    if f_path is not None and target_path is not None and publish_path is not None:
        f_name, target_f_path, publish_f_path, stat_out_folder, stat_file_path, bound_out_folder, vector_bound_path = getFilePaths(f_path, elev_type, target_path, publish_path)  # @UnusedVariable
        
        if not os.path.exists(stat_out_folder):
            process_file = True
            
        if not os.path.exists(vector_bound_path):
            process_file = True
            
        if not os.path.exists(stat_file_path):
            process_file = True
        
        if not os.path.exists(target_f_path):
            process_file = True
        
        if not os.path.exists(publish_f_path):
            process_file = True
                        
    return process_file

'''
----------------------------------------
Calculate all the paths related to the outputs of this script
Changes based on the elevation type (DTM, DSM) and the raster
version (original, derived, or published)

NOTE: GRIDs don't work well in a multiprocessing environment (conflicts with the scratch workspace).
Don't use them, always convert raster formats without a file extension
(FGDB or GRID) to .tif
----------------------------------------
'''
def getFilePaths(f_path, elev_type, target_path, publish_path, raster_version=STAT_FOLDER_ORG):
    # File names and paths
    root_f_path, target_f_ext = os.path.splitext(f_path)
    root_f_path, f_name = os.path.split(root_f_path)
    
    if target_f_ext is None or len(str(target_f_ext).strip()) <= 0:
        target_f_ext = ".tif"
    
    target_f_path = os.path.join(target_path, elev_type, "{}{}".format(f_name, target_f_ext))
    publish_f_path = os.path.join(publish_path, elev_type, "{}.TIF".format(f_name))
    
    bound_out_folder = os.path.join(target_path, STAT_FOLDER, elev_type)
    vector_bound_path = os.path.join(bound_out_folder, "B_{}.shp".format(f_name))
    
    try:
        # Make the STAT folder if it doesn't already exist
        if not os.path.exists(bound_out_folder):
            os.makedirs(bound_out_folder)
    except:
        # Another thread must have made it or there is a lock
        pass
    
    stat_out_folder = os.path.join(bound_out_folder, raster_version)
    stat_file_path = os.path.join(stat_out_folder, "S_{}.txt".format(f_name))
    
    try:
    # Make the STAT folder if it doesn't already exist
    if not os.path.exists(stat_out_folder):
        os.makedirs(stat_out_folder)
    except:
        # Another thread must have made it or there is a lock
        pass
    
    return f_name, target_f_path, publish_f_path, stat_out_folder, stat_file_path, bound_out_folder, vector_bound_path
    
def doTime(a, msg):
    b = datetime.now()
    td = (b - a).total_seconds()
    arcpy.AddMessage("{} in {}".format(msg, td))
    
    return datetime.now()



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
    
    
'''
--------------------------------------------------------------------------------
Creates the mosaic Dataset. Used in multiple places.
--------------------------------------------------------------------------------
'''
def createMosaicDataset(gdb_path, md_name, spatial_reference):
    a = datetime.now()
    
    md_path = os.path.join(gdb_path, md_name)
    deleteFileIfExists(md_path, useArcpy=True)
    # Create a MD in same SR as file
    arcpy.CreateMosaicDataset_management(in_workspace=gdb_path,
                                         in_mosaicdataset_name=md_name,
                                         coordinate_system=spatial_reference,
                                         num_bands="1",
                                         pixel_type="32_BIT_FLOAT",
                                         product_definition="NONE",
                                         product_band_definitions="#")
    
    doTime(a, "\tCreated MD {}".format(md_path))
    
    return md_path


'''
--------------------------------------------------------------------------------
Exports the image file statistics into a .txt file 
--------------------------------------------------------------------------------
'''
def createRasterDatasetStats(f_path, stat_file_path=None):
    a = datetime.now()
    
    try:
        # this no data value doesn't apply to all rasters, but easier to just try and move on
        arcpy.SetRasterProperties_management(f_path, data_type="#", statistics="#", stats_file="#", nodata="1 {}".format(RasterConfig.NODATA_DEFAULT))
    except:
        pass
    arcpy.CalculateStatistics_management(in_raster_dataset=f_path, x_skip_factor="1", y_skip_factor="1", ignore_values="", skip_existing="OVERWRITE", area_of_interest="Feature Set")
    
#     doTime(a, "\tCalculated STATS {}".format(stat_file_path))
#     a = datetime.now()
    
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

    return raster_properties



# '''
# --------------------------------------------------------------------------------
# Calculates a boundary around the LAS dataset. 
# REMOVED: Takes too long to run. Replaced by createVectorBoundaryB
# --------------------------------------------------------------------------------
# '''
# def createVectorBoundaryA(stat_out_folder, f_name, lasd_path, vector_bound_path):
#     a = datetime.now()
#     
#     raster_bound_path = os.path.join(stat_out_folder, "B_{}.tif".format(f_name))
#     vector_R_bound_path = os.path.join(stat_out_folder, "B_{}_R.shp".format(f_name))
#     vector_RB_bound_path = os.path.join(stat_out_folder, "B_{}_RB.shp".format(f_name))
#     vector_RBD_bound_path = os.path.join(stat_out_folder, "B_{}_RBD.shp".format(f_name))
#     vector_RBDE_bound_path = os.path.join(stat_out_folder, "B_{}_RBDE.shp".format(f_name))
#     vector_RBDES_bound_path = os.path.join(stat_out_folder, "B_{}_RBDES.shp".format(f_name))
#     
#     arcpy.RasterToPolygon_conversion(in_raster=raster_bound_path, out_polygon_features=vector_R_bound_path, simplify="SIMPLIFY", raster_field="Value")
#     
#     
#     arcpy.Buffer_analysis(in_features=vector_R_bound_path, out_feature_class=vector_RB_bound_path, buffer_distance_or_field="{} Meters".format(FOOTPRINT_BUFFER_DIST), line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
#     arcpy.Dissolve_management(in_features=vector_RB_bound_path, out_feature_class=vector_RBD_bound_path, multi_part="MULTI_PART", unsplit_lines="DISSOLVE_LINES")
#     arcpy.EliminatePolygonPart_management(in_features=vector_RBD_bound_path, out_feature_class=vector_RBDE_bound_path, condition="AREA", part_area="10000 SquareMiles", part_area_percent="0", part_option="CONTAINED_ONLY")
#     arcpy.SimplifyPolygon_cartography(in_features=vector_RBDE_bound_path, out_feature_class=vector_RBDES_bound_path, algorithm="BEND_SIMPLIFY", tolerance="{} Meters".format(FOOTPRINT_BUFFER_DIST), minimum_area="0 Unknown", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP", in_barriers="")
#     arcpy.Buffer_analysis(in_features=vector_RBDES_bound_path, out_feature_class=vector_bound_path, buffer_distance_or_field="-{} Meters".format(FOOTPRINT_BUFFER_DIST), line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
#     
#     arcpy.Delete_management(in_data=raster_bound_path, data_type="RasterDataset")
#     arcpy.Delete_management(in_data=vector_R_bound_path, data_type="ShapeFile")
#     arcpy.Delete_management(in_data=vector_RB_bound_path, data_type="ShapeFile")
#     arcpy.Delete_management(in_data=vector_RBD_bound_path, data_type="ShapeFile")
#     arcpy.Delete_management(in_data=vector_RBDE_bound_path, data_type="ShapeFile")
#     arcpy.Delete_management(in_data=vector_RBDES_bound_path, data_type="ShapeFile")
#     
#     doTime(a, "\tCreated BOUND {}".format(vector_bound_path))


# '''
# --------------------------------------------------------------------------------
# Calculates a boundary around the dataset using a mosaic dataset.
#  
# Note this is not perfect, as small slivers can be formed along the edges of the dataset. 
# But it performs 10x faster than the other 'A' method
# --------------------------------------------------------------------------------
# '''
# def createVectorBoundaryB(f_path, f_name, raster_props, stat_out_folder, vector_bound_path, minZ, maxZ, bound_path):
#     a = datetime.now()
#     
#     
#     gdb_path = os.path.join(stat_out_folder, "{}.gdb".format(f_name))
#     if os.path.exists(gdb_path):
#         arcpy.Delete_management(gdb_path)
#     arcpy.CreateFileGDB_management(out_folder_path=stat_out_folder, out_name=f_name, out_version="CURRENT")
#     doTime(a, "\tCreated temp fGDB {}".format(gdb_path))
#     
#     md_path = createMosaicDataset(gdb_path, f_name, raster_props[SPAT_REF])
#     arcpy.SetRasterProperties_management(md_path, data_type="ELEVATION", statistics="", stats_file="#", nodata="1 {}".format(NODATA_DEFAULT))
#     
#     a = datetime.now()
#     
#     # Add the files to the Mosaic Dataset and don't update the boundary yet.
#     # The cell size of the Mosaic Dataset is determined by the art.xml file chosen by the user.
#     arcpy.AddRastersToMosaicDataset_management(in_mosaic_dataset=md_path,
#                                                raster_type="Raster Dataset",
#                                                input_path=f_path,
#                                                spatial_reference=raster_props[SPAT_REF],
#                                                update_cellsize_ranges="NO_CELL_SIZES",
#                                                update_boundary="NO_BOUNDARY",
#                                                update_overviews="NO_OVERVIEWS",
#                                                maximum_pyramid_levels="0",
#                                                maximum_cell_size="0",
#                                                minimum_dimension="100",
#                                                filter="*",
#                                                sub_folder="NO_SUBFOLDERS",
#                                                duplicate_items_action="ALLOW_DUPLICATES",
#                                                build_pyramids="NO_PYRAMIDS",
#                                                calculate_statistics="NO_STATISTICS",
#                                                build_thumbnails="NO_THUMBNAILS",
#                                                operation_description="#",
#                                                force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE",
#                                                estimate_statistics="NO_STATISTICS",
#                                                aux_inputs="")
#     
#     a = doTime(a, "\tCompleted Add rasters to {}".format(f_name))
#     
#     # Build Footprints using min_region_size="20" and approx_num_vertices="200". Update the Boundary using the new footprints.
#     arcpy.BuildFootprints_management(in_mosaic_dataset=md_path,
#                                      where_clause="",
#                                      reset_footprint="RADIOMETRY",
#                                      min_data_value=(float(minZ) * .9),
#                                      max_data_value=(float(maxZ) * 1.1),
#                                      approx_num_vertices="-1",
#                                      shrink_distance="0",
#                                      maintain_edges="NO_MAINTAIN_EDGES",
#                                      skip_derived_images="SKIP_DERIVED_IMAGES",
#                                      update_boundary="UPDATE_BOUNDARY",
#                                      request_size="2750",
#                                      min_region_size="25",
#                                      simplification_method="NONE",
#                                      edge_tolerance="-1",
#                                      max_sliver_size="5",
#                                      min_thinness_ratio="0.05")
#     
#     doTime(a, "\tCompleted build footprints on {}".format(md_path))
#     
#     a = datetime.now()
#     deleteFileIfExists(vector_bound_path, useArcpy=True)
#     
#     vector_1_bound_path = os.path.join(stat_out_folder, "B1_{}.shp".format(f_name))
#     deleteFileIfExists(vector_1_bound_path, useArcpy=True)
#     arcpy.ExportMosaicDatasetGeometry_management(md_path, vector_1_bound_path, where_clause="#", geometry_type="BOUNDARY")
#     if os.path.exists(gdb_path):
#         arcpy.Delete_management(gdb_path)
#     
#     vector_2_bound_path = os.path.join(stat_out_folder, "B2_{}.shp".format(f_name))
#     deleteFileIfExists(vector_2_bound_path, useArcpy=True)
#     arcpy.Buffer_analysis(in_features=vector_1_bound_path, out_feature_class=vector_2_bound_path, buffer_distance_or_field="{} Meters".format(FOOTPRINT_BUFFER_DIST), line_side="FULL", line_end_type="ROUND", dissolve_option="NONE", dissolve_field="", method="PLANAR")
#     # arcpy.Buffer_analysis(in_features=vector_1_bound_path, out_feature_class=vector_2_bound_path, buffer_distance_or_field="{} Meters".format(FOOTPRINT_BUFFER_DIST), line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
#     arcpy.Delete_management(in_data=vector_1_bound_path, data_type="ShapeFile")
#     
#     vector_3_bound_path = os.path.join(stat_out_folder, "B3_{}.shp".format(f_name))
#     deleteFileIfExists(vector_3_bound_path, useArcpy=True)
#     arcpy.SimplifyPolygon_cartography(in_features=vector_2_bound_path, out_feature_class=vector_3_bound_path, algorithm="BEND_SIMPLIFY", tolerance="{} Meters".format(FOOTPRINT_BUFFER_DIST / 4), minimum_area="0 Unknown", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP", in_barriers="")
#     arcpy.Delete_management(in_data=vector_2_bound_path, data_type="ShapeFile")
#     
#     vector_4_bound_path = os.path.join(stat_out_folder, "B4_{}.shp".format(f_name))
#     deleteFileIfExists(vector_4_bound_path, useArcpy=True)    
#     arcpy.EliminatePolygonPart_management(in_features=vector_3_bound_path, out_feature_class=vector_4_bound_path, condition="AREA", part_area="10000 SquareMiles", part_area_percent="0", part_option="CONTAINED_ONLY")
#     arcpy.Delete_management(in_data=vector_3_bound_path, data_type="ShapeFile")
#     
#     vector_5_bound_path = os.path.join(stat_out_folder, "B5_{}.shp".format(f_name))
#     deleteFileIfExists(vector_5_bound_path, useArcpy=True)    
#     arcpy.Buffer_analysis(in_features=vector_4_bound_path, out_feature_class=vector_5_bound_path, buffer_distance_or_field="-{} Meters".format(FOOTPRINT_BUFFER_DIST), line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
#     arcpy.Delete_management(in_data=vector_4_bound_path, data_type="ShapeFile")
#     
#     footprint_area = 0
#     for row in arcpy.da.SearchCursor(vector_5_bound_path, ["SHAPE@"]):  # @UndefinedVariable
#         shape = row[0]
#         footprint_area = shape.getArea ("PRESERVE_SHAPE", "SQUAREMETERS")
#     
#     arcpy.AddField_management(in_table=vector_5_bound_path, field_name="Path", field_alias="Path", field_type="TEXT", field_length="500", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
#     arcpy.AddField_management(in_table=vector_5_bound_path, field_name="FileName", field_alias="File Name", field_type="TEXT", field_length="50", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
#     arcpy.AddField_management(in_table=vector_5_bound_path, field_name="Area", field_alias="Area (sq meters)", field_type="DOUBLE", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
#     
#     b_f_path = os.path.split(f_path)[0]
#     arcpy.CalculateField_management(in_table=vector_5_bound_path, field="Path", expression='"{}"'.format(b_f_path), expression_type="PYTHON_9.3")
#     arcpy.CalculateField_management(in_table=vector_5_bound_path, field="FileName", expression='"{}"'.format(f_name), expression_type="PYTHON_9.3")
#     arcpy.CalculateField_management(in_table=vector_5_bound_path, field="Area", expression=footprint_area, expression_type="PYTHON_9.3")
#     
#     arcpy.DeleteField_management(in_table=vector_5_bound_path, drop_field="Id;ORIG_FID;InPoly_FID;SimPgnFlag;MaxSimpTol;MinSimpTol")
#     
#     arcpy.Clip_analysis(in_features=vector_5_bound_path, clip_features=bound_path, out_feature_class=vector_bound_path, cluster_tolerance="")
#     arcpy.Delete_management(in_data=vector_5_bound_path, data_type="ShapeFile")
#     
#     doTime(a, "\tCompleted {}".format(vector_bound_path))
    

'''
--------------------------------------------------------------------------------
Calculates a boundary around the dataset using a raster domain.
  
Note this is better than A or B methods above, still it misses water bodies on the edge of a .las file. 
It performs 10x faster than the other 'B' method
--------------------------------------------------------------------------------
'''
def createVectorBoundaryC(f_path, f_name, raster_props, stat_out_folder, vector_bound_path, minZ, maxZ, bound_path, elev_type):
    a = datetime.now()
    
    vector_1_bound_path = os.path.join(stat_out_folder, "B1_{}.shp".format(f_name))
    vector_2_bound_path = os.path.join(stat_out_folder, "B2_{}.shp".format(f_name))
    deleteFileIfExists(vector_bound_path, useArcpy=True)
    deleteFileIfExists(vector_1_bound_path, useArcpy=True)
    deleteFileIfExists(vector_2_bound_path, useArcpy=True)
    
    arcpy.RasterDomain_3d(in_raster=f_path, out_feature_class=vector_2_bound_path, out_geometry_type="POLYGON")
    arcpy.EliminatePolygonPart_management(in_features=vector_2_bound_path, out_feature_class=vector_1_bound_path, condition="AREA", part_area="10000 SquareMiles", part_area_percent="0", part_option="CONTAINED_ONLY")
    deleteFileIfExists(vector_2_bound_path, useArcpy=True)
     
    footprint_area = 0
    for row in arcpy.da.SearchCursor(vector_1_bound_path, ["SHAPE@"]):  # @UndefinedVariable
        shape = row[0]
        footprint_area = shape.getArea ("PRESERVE_SHAPE", "SQUAREMETERS")
     
    arcpy.AddField_management(in_table=vector_1_bound_path, field_name=FIELD_INFO[PATH][0], field_alias=FIELD_INFO[PATH][1], field_type=FIELD_INFO[PATH][2], field_length=FIELD_INFO[PATH][3], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
    arcpy.AddField_management(in_table=vector_1_bound_path, field_name=FIELD_INFO[NAME][0], field_alias=FIELD_INFO[NAME][1], field_type=FIELD_INFO[NAME][2], field_length=FIELD_INFO[NAME][3], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
    arcpy.AddField_management(in_table=vector_1_bound_path, field_name=FIELD_INFO[AREA][0], field_alias=FIELD_INFO[AREA][1], field_type=FIELD_INFO[AREA][2], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
    arcpy.AddField_management(in_table=vector_1_bound_path, field_name=FIELD_INFO[ELEV_TYPE][0], field_alias=FIELD_INFO[ELEV_TYPE][1], field_type=FIELD_INFO[ELEV_TYPE][2], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
    arcpy.AddField_management(in_table=vector_1_bound_path, field_name=FIELD_INFO[RANGE][0], field_alias=FIELD_INFO[RANGE][1], field_type=FIELD_INFO[RANGE][2], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
     
    try:
        arcpy.DeleteField_management(in_table=vector_1_bound_path, drop_field="Id;ORIG_FID;InPoly_FID;SimPgnFlag;MaxSimpTol;MinSimpTol")
    except:
        pass
     
    arcpy.AddMessage(raster_props)
    for field_name in KEY_LIST:
        field_shpname = FIELD_INFO[field_name][0]
        field_alias = FIELD_INFO[field_name][1]
        field_type = FIELD_INFO[field_name][2]
        field_length = FIELD_INFO[field_name][3]
        field_value = raster_props[field_name]
        if field_type == "TEXT":
            if str(field_value).endswith('\\'):
                field_value = str(field_value)[0:-1]
            field_value = r'"{}"'.format(field_value)
            
        # arcpy.AddMessage("Adding field: {} {} {} {} {}".format(field_shpname, field_alias, field_type, field_length, field_value))
        arcpy.AddField_management(in_table=vector_1_bound_path, field_name=field_shpname, field_alias=field_alias, field_type=field_type, field_length=field_length, field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
        if field_value is not None:
            arcpy.CalculateField_management(in_table=vector_1_bound_path, field=field_shpname, expression=field_value, expression_type="PYTHON_9.3")
    
    b_f_path, b_f_name = os.path.split(f_path)
    b_f_name = os.path.splitext(b_f_name)[0]
    arcpy.CalculateField_management(in_table=vector_1_bound_path, field=FIELD_INFO[PATH][0], expression='"{}"'.format(b_f_path), expression_type="PYTHON_9.3")
    arcpy.CalculateField_management(in_table=vector_1_bound_path, field=FIELD_INFO[NAME][0], expression='"{}"'.format(b_f_name), expression_type="PYTHON_9.3")
    arcpy.CalculateField_management(in_table=vector_1_bound_path, field=FIELD_INFO[AREA][0], expression=footprint_area, expression_type="PYTHON_9.3")
    arcpy.CalculateField_management(in_table=vector_1_bound_path, field=FIELD_INFO[ELEV_TYPE][0], expression='"{}"'.format(elev_type), expression_type="PYTHON_9.3")
    try:
        z_expr = "!{}! - !{}!".format(FIELD_INFO[MAX][0], FIELD_INFO[MIN][0])
        arcpy.CalculateField_management(in_table=vector_1_bound_path, field=FIELD_INFO[RANGE][0], expression=z_expr, expression_type="PYTHON_9.3")
    except:
        pass
    
    deleteFileIfExists(vector_bound_path, True)
    arcpy.Clip_analysis(in_features=vector_1_bound_path, clip_features=bound_path, out_feature_class=vector_bound_path, cluster_tolerance="")
    arcpy.Delete_management(in_data=vector_1_bound_path, data_type="ShapeFile")
     
    doTime(a, "\tCreated BOUND {}".format(vector_bound_path))
        
                

def RevalueRaster(f_path, elev_type, raster_props, target_path, publish_path, minZ, maxZ, bound_path, spatial_ref=None):
    a = datetime.now()
    nodata = RasterConfig.NODATA_DEFAULT     
    
    f_name, target_f_path, publish_f_path, stat_out_folder, stat_file_path, bound_out_folder, vector_bound_path = getFilePaths(f_path, elev_type, target_path, publish_path)  # @UnusedVariable
    
#     target_f_left, target_f_right = os.path.splitext(target_f_path)
#     target1_f_path = "{}1{}".format(target_f_left, target_f_right)
    
    publish_f_left, publish_f_right = os.path.splitext(publish_f_path)
    publish1_f_path = "{}1{}".format(publish_f_left, publish_f_right)
    
    # Don't maintain fGDB raster format, update to TIFF
#     if raster_props[FORMAT] == "FGDBR":
#         target_f_path = "{}.TIF".format(target_f_path)
        
        
    if raster_props[BAND_COUNT] <> 1:
        arcpy.AddMessage("Skipping Raster {}, not 1 band image.".format(f_path))
    else:
        if not (raster_props[PIXEL_TYPE] == PIXEL_TYPE_F32 or raster_props[PIXEL_TYPE] == PIXEL_TYPE_D64):
            arcpy.AddMessage("Skipping Raster '{}', '{}' not Float32 type image.".format(f_path, raster_props[PIXEL_TYPE]))
        else:
            if not (raster_props[FORMAT] == "TIFF" or raster_props[FORMAT] == "GRID" or raster_props[FORMAT] == "IMAGINE Image" or raster_props[FORMAT] == "FGDBR"):
                arcpy.AddMessage("Skipping Raster '{}', '{}' not supported image format.".format(f_path, raster_props[FORMAT]))
            else:
                
                if arcpy.Exists(target_f_path):
                    arcpy.AddMessage("\tDerived Raster exists: {}".format(target_f_path))
                else:
                    deleteFileIfExists(target_f_path, True)
                    arcpy.AddMessage("\tSaving derived raster to {}".format(target_f_path))
                    
                    # Compression isn't being applied properly so results are uncompressed
                    rasterObject = arcpy.Raster(f_path) 
                    outSetNull = arcpy.sa.Con(((rasterObject >= (float(minZ))) & (rasterObject <= (float(maxZ)))), f_path)  # @UndefinedVariable
                    outSetNull.save(target_f_path)
                    del outSetNull, rasterObject
                
                    # Set the no data default value on the input raster
                    arcpy.SetRasterProperties_management(target_f_path, data_type="#", statistics="#", stats_file="#", nodata="1 {}".format(nodata))
                    if spatial_ref is not None:
                        arcpy.AddMessage("Applying projection to raster '{}' {}".format(target_f_path, spatial_ref))
                        if str(spatial_ref).lower().endswith(".prj"):
                            spatial_ref = arcpy.SpatialReference(spatial_ref)
                        arcpy.AddMessage("Applying projection to raster '{}' {}".format(target_f_path, spatial_ref.exportToString()))
                        arcpy.DefineProjection_management(in_dataset=target_f_path, coor_system=spatial_ref)
                    
                    # make sure we make a new published copy of this
                    if arcpy.Exists(publish_f_path):
                        arcpy.Delete_management(publish_f_path)
                    
                    a = doTime(a, "\tCopied '{}' to '{}' with valid values between {} and {}".format(f_path, target_f_path, minZ, maxZ))
                    
                
                if arcpy.Exists(publish_f_path):
                    arcpy.AddMessage("\tPublish Raster exists: {}".format(publish_f_path))
                else:
                    arcpy.AddMessage("\tCopy and clip published raster from {} to {}".format(target_f_path, publish1_f_path))
                    a = datetime.now()
                    
                    deleteFileIfExists(publish1_f_path, True)
                    deleteFileIfExists(publish_f_path, True)
                    # arcpy.RasterToOtherFormat_conversion(target_f_path, publish_f_path, Raster_Format="TIFF")
                    arcpy.CopyRaster_management(in_raster=target_f_path, out_rasterdataset=publish1_f_path, config_keyword="", background_value="", nodata_value=nodata, onebit_to_eightbit="NONE", colormap_to_RGB="NONE", pixel_type="32_BIT_FLOAT", scale_pixel_value="NONE", RGB_to_Colormap="NONE", format="TIFF", transform="NONE")
                
                    arcpy.AddMessage("\tCliping temp raster {} to {}".format(publish1_f_path, publish_f_path))
                    arcpy.Clip_management(in_raster=publish1_f_path, out_raster=publish_f_path, in_template_dataset=bound_path, nodata_value=nodata, clipping_geometry="ClippingGeometry", maintain_clipping_extent="NO_MAINTAIN_EXTENT")
                    
                    deleteFileIfExists(publish1_f_path, True)
                    
                    a = doTime(a, "\tCopied '{}' to '{}'".format(target_f_path, publish_f_path))
                


def CheckRasterSpatialReference(v_name, v_unit, h_name, h_unit, h_wkid, raster_props, spatial_ref=None):
    sr = None
    try:
        if raster_props is not None:
            f_name = raster_props[NAME]
            # Check the raster spatial
            isVName = isMatchingValue(v_name, raster_props[V_NAME])
            if isVName:
                arcpy.AddMessage("RASTER CHECK '{}': Vertical Name from LAS '{}' {} raster file '{}'".format(f_name, v_name, ("Matches" if isVName else "Does NOT Match"), raster_props[V_NAME]))
            else:
                arcpy.AddWarning("WARNING: RASTER CHECK '{}': Vertical Name from LAS '{}' {} raster file '{}'".format(f_name, v_name, ("Matches" if isVName else "Does NOT Match"), raster_props[V_NAME]))
            
            isVUnit = isMatchingValue(v_unit, raster_props[V_UNIT])
            if isVUnit:
                arcpy.AddMessage("RASTER CHECK '{}': Vertical Unit from LAS '{}' {} raster file '{}'".format(f_name, v_unit, ("Matches" if isVUnit else "Does NOT Match"), raster_props[V_UNIT]))
            else:
                arcpy.AddWarning("WARNING: RASTER CHECK '{}': Vertical Unit from LAS '{}' {} raster file '{}'".format(f_name, v_unit, ("Matches" if isVUnit else "Does NOT Match"), raster_props[V_UNIT]))
            
            isHName = isMatchingValue(h_name, raster_props[H_NAME])    
            if isHName:
                arcpy.AddMessage("RASTER CHECK '{}': H_NAME from LAS '{}' {} raster file '{}'".format(f_name, h_name, ("Matches" if isHName else "Does NOT Match"), raster_props[H_NAME]))
            else:
                arcpy.AddWarning("WARNING: RASTER CHECK '{}': H_NAME from LAS '{}' {} raster file '{}'".format(f_name, h_name, ("Matches" if isHName else "Does NOT Match"), raster_props[H_NAME]))
            
            isHUnit = isMatchingValue(h_unit, raster_props[H_UNIT])
            if isHUnit:
                arcpy.AddMessage("RASTER CHECK '{}': Horizontal Unit from LAS '{}' {} raster file '{}'".format(f_name, h_unit, ("Matches" if isHUnit else "Does NOT Match"), raster_props[H_UNIT]))
            else:
                arcpy.AddWarning("WARNING: RASTER CHECK '{}': Horizontal Unit from LAS '{}' {} raster file '{}'".format(f_name, h_unit, ("Matches" if isHUnit else "Does NOT Match"), raster_props[H_UNIT]))
                
                
            isHwkid = isMatchingValue(h_wkid, raster_props[H_WKID])
            if isHUnit:
                arcpy.AddMessage("RASTER CHECK '{}': Horizontal WKID from LAS '{}' {} raster file '{}'".format(f_name, h_wkid, ("Matches" if isHwkid else "Does NOT Match"), raster_props[H_WKID]))
            else:
                arcpy.AddWarning("WARNING: RASTER CHECK '{}': Horizontal WKID from LAS '{}' {} raster file '{}'".format(f_name, h_wkid, ("Matches" if isHwkid else "Does NOT Match"), raster_props[H_WKID]))
    
            if spatial_ref is not None:
                sr = arcpy.SpatialReference(spatial_ref)
                arcpy.AddMessage(Utility.getSpatialReferenceInfo(sr))
                arcpy.AddMessage(sr.exportToString())
            # @TODO: exit if a coordinate system is not present or not complete (make them project or define projection)
    except:
        pass
       
    return sr

    
    
       

def processFile(bound_path, f_path, elev_type, target_path, publish_path, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, spatial_ref=None):
    
    f_name, target_f_path, publish_f_path, stat_out_folder, stat_file_path, bound_out_folder, vector_bound_path = getFilePaths(f_path, elev_type, target_path, publish_path)  # @UnusedVariable
    
    # Export Stats file  
    raster_props = None
    if os.path.exists(stat_file_path):
        arcpy.AddMessage("\tStat file exists: {}".format(stat_file_path))
    else:
        raster_props = createRasterDatasetStats(f_path, stat_file_path)
        spatial_ref = CheckRasterSpatialReference(v_name, v_unit, h_name, h_unit, h_wkid, raster_props, spatial_ref)
    
    if os.path.exists(target_f_path) and os.path.exists(publish_f_path):
        arcpy.AddMessage("\tRasters exist: '{}' and '{}'".format(target_f_path, publish_f_path))
    else:
        if raster_props is None:
            # Stat.txt and boundary file already exists, so just read them in here
            raster_props = createRasterDatasetStats(f_path)
        RevalueRaster(f_path, elev_type, raster_props, target_path, publish_path, z_min, z_max, bound_path, spatial_ref)
        CheckRasterSpatialReference(v_name, v_unit, h_name, h_unit, h_wkid, raster_props)
    
    f_name, target_f_path, publish_f_path, stat_out_folder, stat_file_path, bound_out_folder, vector_bound_path = getFilePaths(f_path, elev_type, target_path, publish_path, STAT_FOLDER_DER)  # @UnusedVariable
    if os.path.exists(stat_file_path):
        arcpy.AddMessage("\tStat file exists: {}".format(stat_file_path))
    else:
        raster_props = createRasterDatasetStats(target_f_path, stat_file_path)
        CheckRasterSpatialReference(v_name, v_unit, h_name, h_unit, h_wkid, raster_props)
    
    f_name, target_f_path, publish_f_path, stat_out_folder, stat_file_path, bound_out_folder, vector_bound_path = getFilePaths(f_path, elev_type, target_path, publish_path, STAT_FOLDER_PUB)  # @UnusedVariable
    if os.path.exists(stat_file_path):
        arcpy.AddMessage("\tStat file exists: {}".format(stat_file_path))
    else:
        raster_props = createRasterDatasetStats(publish_f_path, stat_file_path)    
    
    if os.path.exists(vector_bound_path):
        arcpy.AddMessage("\tBound file exists: {}".format(vector_bound_path))
    else:
            raster_props = createRasterDatasetStats(publish_f_path)
        createVectorBoundaryC(f_path, f_name, raster_props, stat_out_folder, vector_bound_path, z_min, z_max, bound_path, elev_type)
    
    CheckRasterSpatialReference(v_name, v_unit, h_name, h_unit, h_wkid, raster_props)
    

def isMatchingValue(val1, val2):
    if val1 is not None:
        val1 = str(val1).upper().strip()
    if val2 is not None:
        val2 = str(val2).upper().strip()
    
    return (val1 == val2)
    
    

'''
--------------------------------------------------------------------------------
Operates on a single .las file to calcluate the following:

1. Verify if the .lasx file exists, if not it creates it by creating a .lasd
2. Export the statistics .txt file
3. [removed] Export the point file information shape file
4. Calculates the boundary of the .las file using a mosaic dataset
5. [optional] Exports a number of statistical QA rasters (point count, predominate class, etc.)

Inputs:
    f_paths = comma separated list of rastar files to process
    target_path = the full path to the DERIVED folder for the project
    publish_path = the full path to the DERIVED folder for the project
    elev_type = DTM, DSM, etc.
    bound_path = the path to the .las file boundary feature class (from A04 tool)
    
    f_paths, elev_type, target_path, publish_path, bound_path

Outputs:
    
--------------------------------------------------------------------------------
'''
if __name__ == '__main__':
    # give time for things to wake up
    time.sleep(6)
    
    # time parameters to gauge how much time things are taking
    aaa = datetime.now()
    
    f_paths = None
    elev_type = None
    target_path = None
    publish_path = None
    bound_path = None
    checkedOut = False
    z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, spatial_ref = None, None, None, None, None, None, None, None
    
    if len(sys.argv) >= 2:
        f_paths = sys.argv[1]
    
    if len(sys.argv) >= 3:
        elev_type = sys.argv[2]
        
    if len(sys.argv) >= 4:
        target_path = sys.argv[3]
    
    if len(sys.argv) >= 5:
        publish_path = sys.argv[4]
    
    if len(sys.argv) >= 6:
        bound_path = sys.argv[5]
    
    if len(sys.argv) >= 7:
        z_min = sys.argv[6]
    
    if len(sys.argv) >= 8:
        z_max = sys.argv[7]
    
    if len(sys.argv) >= 9:
        v_name = sys.argv[8]
    
    if len(sys.argv) >= 10:
        v_unit = sys.argv[9]
    
    if len(sys.argv) >= 11:
        h_name = sys.argv[10]
    
    if len(sys.argv) >= 12:
        h_unit = sys.argv[11]
    
    if len(sys.argv) >= 13:
        h_wkid = sys.argv[12]
    
    if len(sys.argv) >= 14:
        spatial_ref = sys.argv[13]
    
    arcpy.AddMessage("\tf_paths='{}',elev_type='{}',target_path='{}',publish_path='{}',bound_path='{}',z_min='{}', z_max='{}', v_name='{}', v_unit='{}', h_name='{}', h_unit='{}', h_wkid='{}', sr='{}'".format(f_paths, elev_type, target_path, publish_path, bound_path, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, spatial_ref))
    
    f_paths = str(f_paths).split(",")
    
    for f_path in f_paths:
        if not isProcessFile(f_path, elev_type, target_path, publish_path):
            arcpy.AddMessage("\tAll raster file artifacts exist. Ignoring: {}".format(f_path))
        else:
            if not checkedOut:
                checkedOut = True
                arcpy.AddMessage("\tChecking out licenses")
                arcpy.CheckOutExtension("3D")
                arcpy.CheckOutExtension("Spatial")
            
            processFile(bound_path, f_path, elev_type, target_path, publish_path, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, spatial_ref)
            
    if checkedOut:
        arcpy.CheckInExtension("3D")
        arcpy.CheckInExtension("Spatial")
        
    doTime(aaa, "Completed {}".format(f_path))
        
        
        
        

