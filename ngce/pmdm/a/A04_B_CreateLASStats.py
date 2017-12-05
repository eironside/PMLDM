'''
Created on Jun 15, 2017

@author: eric5946
'''
import arcpy
import copy
import csv
from datetime import datetime
import os
from shutil import copyfile
import sys
import time

from ngce import Utility
from ngce.Utility import addToolMessages, doTime, deleteFileIfExists, setArcpyEnv, getVertCSInfo
from ngce.folders.FoldersConfig import ELEVATION, FIRST, LAST, lasClassified_dir, \
    lasUnclassified_dir, lasd_dir, ALAST, INT, ALL, STATS_METHODS, DATASET_NAMES, \
    pulse_count_dir, point_count_dir
from ngce.las import LAS
from ngce.raster import RasterConfig, Raster
from ngce.raster.RasterConfig import MEAN, MAX, MIN, STAND_DEV, XMIN, XMAX, YMIN, \
    YMAX, V_NAME, V_UNIT, H_NAME, H_UNIT, H_WKID, FIELD_INFO, \
    AREA, NAME, PATH, IS_CLASSIFIED, POINT_COUNT, POINT_PERCENT, POINT_SPACING, \
    RANGE, FIRST_RETURNS, SECOND_RETURNS, THIRD_RETURNS, FOURTH_RETURNS, \
    SINGLE_RETURNS, FIRST_OF_MANY_RETURNS, LAST_OF_MANY_RETURNS, ALL_RETURNS, \
    STAT_LAS_FOLDER, SAMPLE_TYPE


RasterConfig.NODATA_DEFAULT
SMALL_POINT_COUNT = 100
arcpy.env.parallelProcessingFactor = "8"

setArcpyEnv(True)

CELL_SIZE = 10  # Meters
ELE_CELL_SIZE = 10  # Meters
FOOTPRINT_BUFFER_DIST = 25  # Meters
B_SIMPLE_DIST = 1 # Meters
C_SIMPLE_DIST = 0.1 # Meters

MAX_TRIES = 10

KEY_LIST = [MAX, MEAN, MIN, RANGE, STAND_DEV, XMIN, YMIN, XMAX, YMAX, V_NAME, V_UNIT, H_NAME, H_UNIT, H_WKID]

'''
--------------------------------------------------------------------------------
Determines if a file needs to be processed or not by looking at all the derivatives
--------------------------------------------------------------------------------
'''
def isProcessFile(f_path, target_path, createQARasters=False, isClassified=True, createMissingRasters=False):
    process_file = False
    
    if f_path is not None and os.path.exists(f_path) and os.path.exists(target_path):
        
        f_name = os.path.split(os.path.splitext(f_path)[0])[1]
        stat_out_folder = os.path.join(target_path, STAT_LAS_FOLDER)
        
        las_type_folder = lasClassified_dir
        if not isClassified:
            las_type_folder = lasUnclassified_dir
        
        target_las_path = os.path.join(target_path, las_type_folder)
        target_lasd_path = os.path.join(target_las_path, lasd_dir)
        out_lasd_path = os.path.join(target_lasd_path, "{}.lasd".format(f_name))
        
        out_las_path = os.path.join(target_las_path, "{}.las".format(f_name))
        out_lasx_path = os.path.join(target_las_path, "{}.lasx".format(f_name))
        
        # LASX Exists    
        if not os.path.exists(out_lasx_path):
            process_file = True
        
        # LAS Exists    
        if not os.path.exists(out_las_path):
            process_file = True
        
        # LASD Exists    
        if not os.path.exists(out_lasd_path):
            process_file = True
        
        # stat file exists
        stat_file_path = os.path.join(stat_out_folder, "S_{}.txt".format(f_name))
        if not os.path.exists(stat_file_path):
            process_file = True
        
        # point file info exists
        point_file_path = os.path.join(stat_out_folder, "I_{}.shp".format(f_name))    
        if not os.path.exists(point_file_path):
            process_file = True
            
        # boundary shape file exists
        vector_bound_path = os.path.join(stat_out_folder, "B_{}.shp".format(f_name))
        if not os.path.exists(vector_bound_path):
            process_file = True
        
        # footprint shape file exists
        vector_bound_path = os.path.join(stat_out_folder, "C_{}.shp".format(f_name))
        if not os.path.exists(vector_bound_path):
            process_file = True
        
        
        value_field = ELEVATION
        for dataset_name in [ "_" + FIRST, "_" + LAST]:
            name = dataset_name
            
                            
            if not isClassified:
                # Using a generic name for non-classified data
                name = ""
            
            out_folder = os.path.join(target_path, value_field)
            if len(name) > 0:
                out_folder = os.path.join(target_path, value_field, name[1:])
                
            if not os.path.exists(out_folder):
                process_file = True
            
            out_raster = os.path.join(out_folder, "{}{}".format(f_name, name))
            out_raster_path = "{}.tif".format(out_raster)
            clip_raster_path = os.path.join(out_folder, "C_{}{}.tif".format(f_name, name))
            if not os.path.exists(clip_raster_path) and not os.path.exists(out_raster_path):
                process_file = True
        
        if createMissingRasters:
            value_field = ELEVATION
            for dataset_name in ["_" + ALAST]:
                name = dataset_name
                                                
                if not isClassified:
                    # Using a generic name for non-classified data
                    name = ""
                
                out_folder = os.path.join(target_path, value_field)
                if len(name) > 0:
                    out_folder = os.path.join(target_path, value_field, name[1:])
                    
                if not os.path.exists(out_folder):
                    process_file = True
                
                out_raster = os.path.join(out_folder, "{}{}".format(f_name, name))
                out_raster_path = "{}.tif".format(out_raster)
                clip_raster_path = os.path.join(out_folder, "C_{}{}.tif".format(f_name, name))
                if not os.path.exists(clip_raster_path) and not os.path.exists(out_raster_path):
                    process_file = True
            
            value_field = INT
            for dataset_name in ["_" + FIRST]:
                name = dataset_name
                                                
                if not isClassified:
                    # Using a generic name for non-classified data
                    name = ""
                
                out_folder = os.path.join(target_path, value_field)
                if len(name) > 0:
                    out_folder = os.path.join(target_path, value_field, name[1:])
                    
                if not os.path.exists(out_folder):
                    process_file = True
                
                out_raster = os.path.join(out_folder, "{}{}".format(f_name, name))
                out_raster_path = "{}.tif".format(out_raster)
                clip_raster_path = os.path.join(out_folder, "C_{}{}.tif".format(f_name, name))
                if not os.path.exists(clip_raster_path) and not os.path.exists(out_raster_path):
                    process_file = True
        
        # Create the QA statistics files
        if createQARasters:
            # Create the statistics rasters
            stats_methods = STATS_METHODS
            for dataset_name in DATASET_NAMES:
                name = dataset_name
                                
                if not isClassified:
                    # Using a generic name for non-classified data
                    name = ""
                
                
                for method in stats_methods:
                    out_folder = os.path.join(target_path, method)
                    if len(name) > 0:
                        out_folder = os.path.join(target_path, method, name[1:])
                    
                    out_raster = os.path.join(out_folder, "{}{}".format(f_name, name))
                    out_raster_path = "{}.tif".format(out_raster)
                    
                    if not os.path.exists(out_raster_path):
                        process_file = True
                        
    return process_file




    
    
# '''
# --------------------------------------------------------------------------------
# Creates the las Dataset. Used in multiple places.
# target_path = "<projectdir>\DERIVED"
# --------------------------------------------------------------------------------
# '''
# 
# def extractLas(f_name, isClassified, in_lasd_path, out_lasd_path, out_las_path, out_lasx_path=None):
#     a = datetime.now()
#     if not os.path.exists(out_lasd_path):
#         lasd_layer = in_lasd_path
#         if isClassified:
#             lasd_layer = "{}_lasd_layer".format(f_name)
#             arcpy.MakeLasDatasetLayer_management(in_lasd_path, lasd_layer, "0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", "'Last Return';'First of Many';'Last of Many';'Single Return';1;2;3;4;5", "INCLUDE_UNFLAGGED", "INCLUDE_SYNTHETIC", "INCLUDE_KEYPOINT", "EXCLUDE_WITHHELD", "")
#         a = doTime(a, "\tCreated LASD layer '{}'".format(lasd_layer))
# #         compute_stats = "NO_COMPUTE_STATS"
#         rearrange = "MAINTAIN_POINTS"
#         if out_lasx_path is not None and not os.path.exists(out_lasx_path):
#             rearrange = "REARRANGE_POINTS"
#         compute_stats = "COMPUTE_STATS"
#         arcpy.ExtractLas_3d(lasd_layer, out_las_path, "DEFAULT", "", "PROCESS_EXTENT", "", "MAINTAIN_VLR", rearrange, compute_stats, out_lasd_path)
#         a = doTime(a, "\tExtracted LAS to '{}'".format(out_las_path))
    

def createLasDataset(f_name, f_path, spatial_reference, target_path, isClassified):
    a = datetime.now()
    aa = a
    las_type_folder = lasClassified_dir
    if not isClassified:
        las_type_folder = lasUnclassified_dir
    target_las_path = os.path.join(target_path, las_type_folder)
    target_lasd_path = os.path.join(target_las_path, lasd_dir)
    if not os.path.exists(target_lasd_path):
        os.makedirs(target_lasd_path)
    temp_lasd_path = os.path.join(target_las_path, "temp")
    if not os.path.exists(temp_lasd_path):
        os.makedirs(temp_lasd_path)
        
    out_lasd_path = os.path.join(target_lasd_path, "{}.lasd".format(f_name))
    temp1_lasd_path = os.path.join(temp_lasd_path, "temp_{}.lasd".format(f_name))
    
    out_las_path = os.path.join(target_las_path, "{}.las".format(f_name))
    out_las_rear_path = os.path.join(target_las_path, "Rearrange_{}.las".format(f_name))
    out_lasx_path = os.path.join(target_las_path, "{}.lasx".format(f_name))
    
    if not os.path.exists(out_las_path) or not os.path.exists(out_lasx_path) or not os.path.exists(out_lasd_path):
        deleteFileIfExists(temp1_lasd_path)
        
        arcpy.AddMessage("\t  Creating LASD: '{}' '{}' {}'".format(f_path, spatial_reference, out_lasd_path))
        arcpy.CreateLasDataset_management(input=f_path,
                                          spatial_reference=spatial_reference,
                                          out_las_dataset=temp1_lasd_path,
                                          folder_recursion="NO_RECURSION",
                                          in_surface_constraints="",
                                          compute_stats="NO_COMPUTE_STATS",
                                          relative_paths="ABSOLUTE_PATHS",
                                          create_las_prj="FILES_MISSING_PROJECTION")
        
        a = doTime(a, "\tCreated LASD {}".format(temp1_lasd_path))

        deleteFileIfExists(out_lasd_path)
        deleteFileIfExists(out_las_path)
        deleteFileIfExists(out_las_rear_path)
        deleteFileIfExists(out_lasx_path)

        lasd_layer = temp1_lasd_path
        if isClassified:
            lasd_layer = "{}_lasd_layer".format(f_name)
            lasd_layer = arcpy.MakeLasDatasetLayer_management(temp1_lasd_path, lasd_layer, "0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", "'Last Return';'First of Many';'Last of Many';'Single Return';1;2;3;4;5", "INCLUDE_UNFLAGGED", "INCLUDE_SYNTHETIC", "INCLUDE_KEYPOINT", "EXCLUDE_WITHHELD", "")
                
        arcpy.ExtractLas_3d(in_las_dataset=lasd_layer,
                            target_folder=target_las_path,
                            process_entire_files="PROCESS_EXTENT",
                            remove_vlr="MAINTAIN_VLR",
                            rearrange_points="REARRANGE_POINTS",
                            compute_stats="COMPUTE_STATS",
                            out_las_dataset=out_lasd_path)
        
        a = doTime(a, "\tExtracted LAS to '{}'".format(out_las_path))
        
    in_prj_path = "{}.prj".format(os.path.split(f_path)[0])
    out_prj_path = os.path.join(target_las_path, "{}.prj".format(f_name))
    if os.path.exists(in_prj_path):
        copyfile(in_prj_path, out_prj_path)
        a = doTime(a, "\tCopied LAS projection file '{}'".format(out_prj_path))
    
    deleteFileIfExists(temp1_lasd_path)
    
    a = doTime(aa, "\tCompleted LASD '{}'".format(out_lasd_path))

# 
# def classifyGround(lasd_path, las_v_unit):
#     methods = ["CONSERVATIVE", 'STANDARD', "AGGRESSIVE"]
#     veg_classes = {3:5, 4:25, 5:50}
#     
#     for method in methods:
#         ground_lasd_layer = "gnd_{}_lasd_layer".format(method)    
#         arcpy.MakeLasDatasetLayer_management(lasd_path, ground_lasd_layer, "0;1;2", "'Last Return';'Last of Many';'Single Return'", "INCLUDE_UNFLAGGED", "INCLUDE_SYNTHETIC", "INCLUDE_KEYPOINT", "EXCLUDE_WITHHELD", "")
#         arcpy.ClassifyLasGround_3d (in_las_dataset=ground_lasd_layer, method="CONSERVATIVE", reuse_ground="REUSE_GROUND", compute_stats="COMPUTE_STATS", extent="DEFAULT", process_entire_files="PROCESS_ENTIRE_FILES")
#         
#         classify_lasd_layer = "cbh_{}_lasd_layer".format(method)    
#         arcpy.MakeLasDatasetLayer_management(lasd_path, classify_lasd_layer, "0;1;2", "'Last Return';'First of Many';'Last of Many';'Single Return';1;2;3;4;5", "INCLUDE_UNFLAGGED", "INCLUDE_SYNTHETIC", "INCLUDE_KEYPOINT", "EXCLUDE_WITHHELD", "")
#         arcpy.ClassifyLasByHeight_3d(in_las_dataset=classify_lasd_layer,
#                                      ground_source="GROUND",
#                                      height_classification="3 5;4 25;6 50",
#                                      noise="ALL_NOISE",
#                                      compute_stats="NO_COMPUTE_STATS",
#                                      extent="DEFAULT",
#                                      process_entire_files="PROCESS_ENTIRE_FILES", boundary="")
#         
#     
# def classifyNoise(lasd_path, las_v_unit):
#     max_z = 6200
#     
#     a = datetime.now()
#     
#     if str(las_v_unit).upper() == "Survey Feet".upper():
#         max_z = max_z / (1200 / 3937)
#     elif str(las_v_unit).upper() == "International Feet".upper():
#         max_z = max_z / 0.3048
#         
#     for clazz in range(0, 19):
#         if clazz <> 7 and clazz < 18:
#             lasd_layer = "{}_lasd_layer".format(clazz)    
#             arcpy.MakeLasDatasetLayer_management(lasd_path, lasd_layer, "{}".format(clazz), "'Last Return';'First of Many';'Last of Many';'Single Return';1;2;3;4;5", "INCLUDE_UNFLAGGED", "INCLUDE_SYNTHETIC", "INCLUDE_KEYPOINT", "EXCLUDE_WITHHELD", "")
#             desc = arcpy.Describe(lasd_layer)
#             point_count = desc.pointCount
#             if point_count > 0:
#                 
#                 arcpy.AddMessage("Removing noise from {} class {} points".format(point_count, clazz))
#                 arcpy.ClassifyLasByHeight_3d(in_las_dataset=lasd_layer,
#                                              ground_source="GROUND",
#                                              height_classification="{} {}".format(clazz, max_z),
#                                              noise="ALL_NOISE",
#                                              compute_stats="NO_COMPUTE_STATS",
#                                              extent="DEFAULT",
#                                              process_entire_files="PROCESS_ENTIRE_FILES", boundary="")
#                 a = doTime(a, "Removed noise from {} class {} points".format(point_count, clazz))                 
#     
#     
'''
--------------------------------------------------------------------------------
Exports the .las file statistics into a .txt file 
--------------------------------------------------------------------------------
'''
def createLasDatasetStats(lasd_path, f_path, spatial_reference, stat_file_path):
    if not os.path.exists(lasd_path):
        createLasDataset(f_path, spatial_reference, lasd_path)
    a = datetime.now()
    
    deleteFileIfExists(stat_file_path)

    # Determine state of statistics
    calculation_type="SKIP_EXISTING_STATS"
    desc = arcpy.Describe(lasd_path)
    if desc.needsUpdateStatistics:
        calculation_type="OVERWRITE_EXISTING_STATS"
    
    arcpy.LasDatasetStatistics_management(in_las_dataset=lasd_path,
                                          calculation_type=calculation_type,
                                          out_file=stat_file_path,
                                          summary_level="LAS_FILES",
                                          delimiter="COMMA",
                                          decimal_separator="DECIMAL_POINT")
    
    doTime(a, "\tCreated STATS {}".format(stat_file_path))


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
#     arcpy.LasPointStatsAsRaster_management(in_las_dataset=lasd_path, out_raster=raster_bound_path, method="PREDOMINANT_LAST_RETURN", sampling_type="CELLSIZE", sampling_value="1")
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


'''
--------------------------------------------------------------------------------
Calculates a boundary around the LAS dataset using a mosaic dataset.
  
Note this is not perfect, as small slivers can be formed along the edges of the dataset. 
But it performs 10x faster than the A method and is more accurate than C
--------------------------------------------------------------------------------
'''
def createVectorBoundaryB(spatial_reference, stat_out_folder, f_name, f_path, vector_bound_path):
    a = datetime.now()
    
    deleteFileIfExists(vector_bound_path, useArcpy=True)
    horz_cs_name, horz_cs_unit_name, horz_cs_factory_code, vert_cs_name, vert_unit_name = Utility.getSRValues(spatial_reference)
    raster_type = LAS.LAS_raster_type_1_all_bin_mean_idw
    if str(horz_cs_unit_name.upper()).find("METER") < 0:
        raster_type = LAS.LAS_raster_type_3_all_bin_mean_idw
    arcpy.AddMessage("Horizontal units are {}, using raster type {}".format(horz_cs_unit_name, raster_type))
    
        
    gdb_name = f_name
     
    gdb_path = os.path.join(stat_out_folder, "{}.gdb".format(gdb_name))
    if os.path.exists(gdb_path):
        arcpy.Delete_management(gdb_path)
    arcpy.CreateFileGDB_management(out_folder_path=stat_out_folder, out_name=gdb_name, out_version="CURRENT")
         
    md_name = "B{}".format(f_name)
    md_path = os.path.join(gdb_path, md_name)
     
    deleteFileIfExists(md_path, useArcpy=True)
    # Create a MD in same SR as LAS Dataset
    arcpy.CreateMosaicDataset_management(in_workspace=gdb_path,
                                         in_mosaicdataset_name=md_name,
                                         coordinate_system=spatial_reference,
                                         num_bands="1",
                                         pixel_type="32_BIT_FLOAT",
                                         product_definition="NONE",
                                         product_band_definitions="#")
     
    doTime(a, "\tCreated MD {}".format(md_name))
    a = datetime.now()
    
    
    # Add the LAS files to the Mosaic Dataset and don't update the boundary yet.
    # The cell size of the Mosaic Dataset is determined by the art.xml file chosen by the user.
     
    arcpy.AddRastersToMosaicDataset_management(in_mosaic_dataset=md_path,
                                               raster_type=raster_type,
                                               input_path=f_path,
                                               spatial_reference=spatial_reference,
                                               update_cellsize_ranges="NO_CELL_SIZES",
                                               update_boundary="NO_BOUNDARY",
                                               update_overviews="NO_OVERVIEWS",
                                               maximum_pyramid_levels="0",
                                               maximum_cell_size="0",
                                               minimum_dimension="100",
                                               filter="*.las",
                                               sub_folder="SUBFOLDERS",
                                               duplicate_items_action="ALLOW_DUPLICATES",
                                               build_pyramids="NO_PYRAMIDS",
                                               calculate_statistics="NO_STATISTICS",
                                               build_thumbnails="NO_THUMBNAILS",
                                               operation_description="#",
                                               force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE",
                                               estimate_statistics="NO_STATISTICS",
                                               aux_inputs="")
     
    doTime(a, "\t\tCompleted Add rasters to {}".format(md_name))
    a = datetime.now()
     
    # Build Footprints using min_region_size="20" and approx_num_vertices="200". Update the Boundary using the new footprints.
    arcpy.BuildFootprints_management(in_mosaic_dataset=md_path,
                                     where_clause="",
                                     reset_footprint="RADIOMETRY",
                                     min_data_value="0",
                                     max_data_value="15000",
                                     approx_num_vertices="-1",
                                     shrink_distance="0",
                                     maintain_edges="NO_MAINTAIN_EDGES",
                                     skip_derived_images="SKIP_DERIVED_IMAGES",
                                     update_boundary="UPDATE_BOUNDARY",
                                     request_size="2750",
                                     min_region_size="250",
                                     simplification_method="NONE",
                                     edge_tolerance="-1",
                                     max_sliver_size="5",
                                     min_thinness_ratio="0.05")
#     arcpy.BuildFootprints_management(las_qainfo.LASMD_path, where_clause="#", reset_footprint="RADIOMETRY", min_data_value=0,
#                                      max_data_value=22000, approx_num_vertices=200, shrink_distance="0", maintain_edges="MAINTAIN_EDGES",
#                                      skip_derived_images="SKIP_DERIVED_IMAGES", update_boundary="UPDATE_BOUNDARY", request_size="2000",
#                                      min_region_size=las_qainfo.LAS_Footprint_MIN_REGION_SIZE, simplification_method="NONE", edge_tolerance="#", max_sliver_size="20",
#                                      min_thinness_ratio="0.05")
     
    doTime(a, "\t\tCompleted build footprints on {}".format(md_path))
    a = datetime.now()
    
    
     
     
    vector_R_bound_path = os.path.join(stat_out_folder, "B_{}_R.shp".format(f_name))
    deleteFileIfExists(vector_R_bound_path, useArcpy=True)
    arcpy.ExportMosaicDatasetGeometry_management(md_path, vector_R_bound_path, where_clause="#", geometry_type="BOUNDARY")
    addToolMessages()
    if os.path.exists(gdb_path):
        arcpy.Delete_management(gdb_path)
     
    vector_REB_bound_path = os.path.join(stat_out_folder, "B_{}_REB.shp".format(f_name))
    deleteFileIfExists(vector_REB_bound_path, useArcpy=True)
    arcpy.Buffer_analysis(in_features=vector_R_bound_path, out_feature_class=vector_REB_bound_path, buffer_distance_or_field="{} Meters".format(FOOTPRINT_BUFFER_DIST), line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
    addToolMessages()
    arcpy.Delete_management(in_data=vector_R_bound_path, data_type="ShapeFile")
     
    vector_REBS_bound_path = os.path.join(stat_out_folder, "B_{}_REBS.shp".format(f_name))
    deleteFileIfExists(vector_REBS_bound_path, useArcpy=True)
    arcpy.SimplifyPolygon_cartography(in_features=vector_REB_bound_path, out_feature_class=vector_REBS_bound_path, algorithm="BEND_SIMPLIFY", tolerance="{} Meters".format(B_SIMPLE_DIST), minimum_area="0 Unknown", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP", in_barriers="")
    #addToolMessages()
    arcpy.Delete_management(in_data=vector_REB_bound_path, data_type="ShapeFile")
     
    vector_RE_bound_path = os.path.join(stat_out_folder, "B_{}_RE.shp".format(f_name))
    deleteFileIfExists(vector_RE_bound_path, useArcpy=True)    
    arcpy.EliminatePolygonPart_management(in_features=vector_REBS_bound_path, out_feature_class=vector_RE_bound_path, condition="AREA", part_area="10000 SquareMiles", part_area_percent="0", part_option="CONTAINED_ONLY")
    #addToolMessages()
    arcpy.Delete_management(in_data=vector_REBS_bound_path, data_type="ShapeFile")
     
    arcpy.Buffer_analysis(in_features=vector_RE_bound_path, out_feature_class=vector_bound_path, buffer_distance_or_field="-{} Meters".format(FOOTPRINT_BUFFER_DIST), line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
    arcpy.RepairGeometry_management(in_features=vector_bound_path, delete_null="DELETE_NULL")
    #addToolMessages()
    arcpy.Delete_management(in_data=vector_RE_bound_path, data_type="ShapeFile")
     
    footprint_area = 0
    for row in arcpy.da.SearchCursor(vector_bound_path, ["SHAPE@"]):  # @UndefinedVariable
        shape = row[0]
        footprint_area = shape.getArea ("PRESERVE_SHAPE", "SQUAREMETERS")

    b_f_path = os.path.split(f_path)[0]
    b_f_name = os.path.splitext(f_name)[0]

    
    tries = 0
    success = False

    while not success and tries < MAX_TRIES:
        tries = tries + 1
        try:

            if not fieldExists(vector_bound_path, FIELD_INFO[PATH][0]):
                arcpy.AddField_management(in_table=vector_bound_path, field_name=FIELD_INFO[PATH][0], field_alias=FIELD_INFO[PATH][1], field_type=FIELD_INFO[PATH][2], field_length=FIELD_INFO[PATH][3], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
            arcpy.CalculateField_management(in_table=vector_bound_path, field=FIELD_INFO[PATH][0], expression='"{}"'.format(b_f_path), expression_type="PYTHON_9.3")

            if not fieldExists(vector_bound_path, FIELD_INFO[NAME][0]):
                arcpy.AddField_management(in_table=vector_bound_path, field_name=FIELD_INFO[NAME][0], field_alias=FIELD_INFO[NAME][1], field_type=FIELD_INFO[NAME][2], field_length=FIELD_INFO[NAME][3], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
            arcpy.CalculateField_management(in_table=vector_bound_path, field=FIELD_INFO[NAME][0], expression='"{}"'.format(b_f_name), expression_type="PYTHON_9.3")

            if not fieldExists(vector_bound_path, FIELD_INFO[AREA][0]):
                arcpy.AddField_management(in_table=vector_bound_path, field_name=FIELD_INFO[AREA][0], field_alias=FIELD_INFO[AREA][1], field_type=FIELD_INFO[AREA][2], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
            arcpy.CalculateField_management(in_table=vector_bound_path, field=FIELD_INFO[AREA][0], expression=footprint_area, expression_type="PYTHON_9.3")
            success = True
        except:
            if tries >= MAX_TRIES:
                error_vector_bound_path = os.path.join(stat_out_folder, "ERROR_B_{}.shp".format(f_name))
                deleteFileIfExists(error_vector_bound_path, useArcpy=True) 
                arcpy.AddError("\tERROR: Failed to modify fields, giving up and renaming to {}".format(error_vector_bound_path))
                arcpy.Rename_management(vector_bound_path, error_vector_bound_path)
                deleteFileIfExists(vector_bound_path, useArcpy=True) 
            else:
                arcpy.AddWarning("\tWARNING: Failed to modify fields, trying again")
                pass
    
    try:
        arcpy.DeleteField_management(in_table=vector_bound_path, drop_field="Id;ORIG_FID;InPoly_FID;SimPgnFlag;MaxSimpTol;MinSimpTol")
        #addToolMessages()
    except:
        pass
     
    doTime(a, "\tCreated BOUND {}".format(vector_bound_path))


def fieldExists(fc, target_name):
    field_list = arcpy.ListFields(fc)
    found = False
    for field in field_list:
        if field.name == target_name:
            found = True
            break
    return found
'''
--------------------------------------------------------------------------------
Adds the exported point file info I_<name>.shp point spacing value to the bound feature class
--------------------------------------------------------------------------------
'''
def addInfoFileFieldsToBound(vector_bound_path, info_file_path):
    try:
        indx = 0
        field_post = FIELD_INFO[POINT_SPACING]
        field_name = ALL_RETURNS
                
        for row in arcpy.da.SearchCursor(info_file_path, ["Class", "Pt_Spacing"]):  # @UndefinedVariable
            indx = indx + 1
            clazz = None if row[0] is None else int(row[0])
            pt_spc = None if row[1] is None else float(row[1])
            if pt_spc <= 0:
                pt_spc = None
                
            if clazz is not None:
                if clazz == -1:
                    field_shpname = "{}_{}".format(FIELD_INFO[field_name][0], field_post[0])
                    field_alias = "{} {}".format(FIELD_INFO[field_name][1], field_post[1])
                    field_type = field_post[2]
                    field_length = field_post[3]
                    field_value = pt_spc
                elif clazz >= 0 and clazz <> 7:
                    field_shpname = "c{}_{}".format(("0{}".format(clazz))[-2:], field_post[0])
                    field_alias = "Class {} {}".format(clazz, field_post[1])
                    field_type = field_post[2]
                    field_length = field_post[3]
                    field_value = pt_spc
                # arcpy.AddMessage("\t\tAdding field: name'{}' alias'{}' type'{}' len'{}' value'{}'".format(field_shpname, field_alias, field_type, field_length, field_value))
                arcpy.AddField_management(in_table=vector_bound_path, field_name=field_shpname, field_alias=field_alias, field_type=field_type, field_length=field_length, field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
                if field_value is not None:
                    arcpy.CalculateField_management(in_table=vector_bound_path, field=field_shpname, expression=field_value, expression_type="PYTHON_9.3")
    except:
        arcpy.AddWarning("WARNING: Failed to add Point Spacing info {} to boundary {}".format(info_file_path, vector_bound_path))
    

'''
--------------------------------------------------------------------------------
Adds the exported statistics file S_<name>.txt values to the bound feature class
--------------------------------------------------------------------------------
'''
def addStatFileFieldsToBound(vector_bound_path, stat_file_path):
    clazz_data = {}
    for clazz in range(0, 18):
        if clazz <> 7:
            clazz_data[clazz] = {FIELD_INFO[POINT_COUNT][0]:0,
                            FIELD_INFO[POINT_PERCENT][0]:0,
                            FIELD_INFO[MIN][0]:None,
                            FIELD_INFO[MAX][0]:None,
                            FIELD_INFO[RANGE][0]:None}
        
    try:
        reader = csv.reader(open(stat_file_path, 'rb'))
        indx = 0
        for row in reader:
            clazz = None
            field_name = None
            indx = indx + 1
            if indx > 2:
                item = str(row[1])
                category = str(row[2])
                if category == "Returns":
                    if item == "First":
                        field_name = FIRST_RETURNS
                    elif item == "Second":
                        field_name = SECOND_RETURNS
                    elif item == "Third":
                        field_name = THIRD_RETURNS
                    elif item == "Fourth":
                        field_name = FOURTH_RETURNS
                    elif item == "Single":
                        field_name = SINGLE_RETURNS
                    elif item == "First_of_Many":
                        field_name = FIRST_OF_MANY_RETURNS
                    elif item == "Last_of_Many":
                        field_name = LAST_OF_MANY_RETURNS
                    elif item == "All":
                        field_name = ALL_RETURNS
                elif category == "ClassCodes":
                    clazz = None
                    try:
                        clazz = int(item.split("_")[0])
                        if clazz is not None and (clazz == 18 or clazz == 7 or clazz > 18):
                            clazz = None
                    except:
                        pass
                
                
                if not (clazz is None and field_name is None):
                    pt_cnt = None if row[3] is None else float(row[3])
                    percent = None if row[4] is None else float(row[4])
                    z_min = None if row[5] is None else float(row[5])
                    z_max = None if row[6] is None else float(row[6])
                    z_range = 0
                    if z_min > -430 and z_max < 15000:
                        z_range = z_max - z_min
                        
                    field_props = {FIELD_INFO[POINT_COUNT][0]:pt_cnt,
                        FIELD_INFO[POINT_PERCENT][0]:percent,
                        FIELD_INFO[MIN][0]:z_min,
                        FIELD_INFO[MAX][0]:z_max,
                        FIELD_INFO[RANGE][0]:z_range}
                    
                    if clazz is not None and clazz >= 0 and clazz <> 7 and clazz < 18:
                        clazz_data[clazz] = field_props
                    elif field_name is not None:
                        for field_post in [FIELD_INFO[POINT_COUNT], FIELD_INFO[POINT_PERCENT], FIELD_INFO[MIN], FIELD_INFO[MAX], FIELD_INFO[RANGE]]:
                            field_shpname = None
                            field_alias = None
                            field_type = None
                            field_length = None
                            field_value = None
                        
                            field_shpname = "{}_{}".format(FIELD_INFO[field_name][0], field_post[0])
                            field_alias = "{} {}".format(FIELD_INFO[field_name][1], field_post[1])
                            field_type = field_post[2]
                            field_length = field_post[3]
                            field_value = field_props[field_post[0]] 
                        
                            # arcpy.AddMessage("\t\tAdding field: name'{}' alias'{}' type'{}' len'{}' value'{}'".format(field_shpname, field_alias, field_type, field_length, field_value))
                            arcpy.AddField_management(in_table=vector_bound_path, field_name=field_shpname, field_alias=field_alias, field_type=field_type, field_length=field_length, field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
                            if field_value is not None:
                                arcpy.CalculateField_management(in_table=vector_bound_path, field=field_shpname, expression=field_value, expression_type="PYTHON_9.3")
                    
        for field_post in [FIELD_INFO[POINT_COUNT], FIELD_INFO[POINT_PERCENT], FIELD_INFO[MIN], FIELD_INFO[MAX], FIELD_INFO[RANGE]]:
            for clazz in clazz_data.keys():
                field_shpname = None
                field_alias = None
                field_type = None
                field_length = None
                field_value = None

                field_props = clazz_data[clazz]
                field_shpname = "c{}_{}".format(("0{}".format(clazz))[-2:], field_post[0])
                field_alias = "Class {} {}".format(clazz, field_post[1])
                field_type = field_post[2]
                field_length = field_post[3]
                field_value = field_props[field_post[0]]
                
                # arcpy.AddMessage("\t\tAdding field: name'{}' alias'{}' type'{}' len'{}' value'{}'".format(field_shpname, field_alias, field_type, field_length, field_value))
                arcpy.AddField_management(in_table=vector_bound_path, field_name=field_shpname, field_alias=field_alias, field_type=field_type, field_length=field_length, field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
                if field_value is not None:
                    arcpy.CalculateField_management(in_table=vector_bound_path, field=field_shpname, expression=field_value, expression_type="PYTHON_9.3")
    except:
        arcpy.AddWarning("\tWARNING: Failed to add Stat file info {} to boundary {}".format(stat_file_path, vector_bound_path))
    
    
'''
--------------------------------------------------------------------------------
Adds the exported elevation raster key values to the bound feature class
--------------------------------------------------------------------------------
'''
def addKeyFieldValues(vector_bound_path, stat_props):
    try :
        for field_name in KEY_LIST:
            field_shpname = FIELD_INFO[field_name][0]
            field_alias = FIELD_INFO[field_name][1]
            field_type = FIELD_INFO[field_name][2]
            field_length = FIELD_INFO[field_name][3]
            field_value = stat_props[field_name]
            if field_type == "TEXT":
                if str(field_value).endswith('\\'):
                    field_value = str(field_value)[0:-1]
                field_value = r'"{}"'.format(field_value)
            # arcpy.AddMessage("\t\tAdding field: name'{}' alias'{}' type'{}' len'{}' value'{}'".format(field_shpname, field_alias, field_type, field_length, field_value))
            arcpy.AddField_management(in_table=vector_bound_path, field_name=field_shpname, field_alias=field_alias, field_type=field_type, field_length=field_length, field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
            if field_value is not None:
                arcpy.CalculateField_management(in_table=vector_bound_path, field=field_shpname, expression=field_value, expression_type="PYTHON_9.3")
    except:
        arcpy.AddWarning("\tWARNING: Failed to add raster statistics key value info to boundary {}: values = {}".format(vector_bound_path, stat_props))

'''
--------------------------------------------------------------------------------
Calculates a boundary around the dataset using a raster domain.
   
Note this is faster than A or B methods above, still it misses water bodies on the edge of a .las file.
It also misses concave edges, not as accurate as B in this case. 
It performs 10x faster than the other 'B' method
--------------------------------------------------------------------------------
''' 
def createVectorBoundaryC(f_path, vector_bound_path, isClassified, stat_props=None):
    a = datetime.now()
    # arcpy.AddMessage("\tBoundary '{}'".format(vector_bound_path))
     
    vector_bound_left, vector_bound_right = os.path.splitext(vector_bound_path)
    vector_bound_1_path = "{}1{}".format(vector_bound_left, vector_bound_right) 
    vector_bound_2_path = "{}2{}".format(vector_bound_left, vector_bound_right) 
    deleteFileIfExists(vector_bound_path, useArcpy=True)
    deleteFileIfExists(vector_bound_1_path, useArcpy=True)
    deleteFileIfExists(vector_bound_2_path, useArcpy=True)
     
     
    arcpy.RasterDomain_3d(in_raster=f_path, out_feature_class=vector_bound_1_path, out_geometry_type="POLYGON")
    arcpy.EliminatePolygonPart_management(in_features=vector_bound_1_path, out_feature_class=vector_bound_2_path, condition="AREA", part_area="10000 SquareMiles", part_area_percent="0", part_option="CONTAINED_ONLY")
    arcpy.SimplifyPolygon_cartography(in_features=vector_bound_2_path, out_feature_class=vector_bound_path, algorithm="BEND_SIMPLIFY", tolerance="{} Meters".format(C_SIMPLE_DIST), minimum_area="0 Unknown", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP", in_barriers="")
    arcpy.RepairGeometry_management(in_features=vector_bound_path, delete_null="DELETE_NULL")
    deleteFileIfExists(vector_bound_1_path, useArcpy=True)
    deleteFileIfExists(vector_bound_2_path, useArcpy=True)
      
    footprint_area = 0
    for row in arcpy.da.SearchCursor(vector_bound_path, ["SHAPE@"]):  # @UndefinedVariable
        shape = row[0]
        footprint_area = shape.getArea ("PRESERVE_SHAPE", "SQUAREMETERS")
    
    arcpy.AddField_management(in_table=vector_bound_path, field_name=FIELD_INFO[PATH][0], field_alias=FIELD_INFO[PATH][1], field_type=FIELD_INFO[PATH][2], field_length=FIELD_INFO[PATH][3], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
    arcpy.AddField_management(in_table=vector_bound_path, field_name=FIELD_INFO[NAME][0], field_alias=FIELD_INFO[NAME][1], field_type=FIELD_INFO[NAME][2], field_length=FIELD_INFO[NAME][3], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
    arcpy.AddField_management(in_table=vector_bound_path, field_name=FIELD_INFO[IS_CLASSIFIED][0], field_alias=FIELD_INFO[IS_CLASSIFIED][1], field_type=FIELD_INFO[IS_CLASSIFIED][2], field_length=FIELD_INFO[IS_CLASSIFIED][3], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
    arcpy.AddField_management(in_table=vector_bound_path, field_name=FIELD_INFO[AREA][0], field_alias=FIELD_INFO[AREA][1], field_type=FIELD_INFO[AREA][2], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
#     arcpy.AddField_management(in_table=vector_bound_path, field_name=FIELD_INFO[RANGE][0], field_alias=FIELD_INFO[RANGE][1], field_type=FIELD_INFO[RANGE][2], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
    
      
    b_f_path, vector_f_name = os.path.split(f_path)
    f_name = os.path.splitext(vector_f_name)[0]
    if str(f_name).startswith("C_"):
        f_name = f_name[2:]
    if str(f_name).endswith("_LAST"):
        f_name = f_name[:-5]
    elif str(f_name).endswith("_FIRST"):
        f_name = f_name[:-6]
    arcpy.CalculateField_management(in_table=vector_bound_path, field=FIELD_INFO[PATH][0], expression='"{}"'.format(b_f_path), expression_type="PYTHON_9.3")
    arcpy.CalculateField_management(in_table=vector_bound_path, field=FIELD_INFO[NAME][0], expression='"{}"'.format(f_name), expression_type="PYTHON_9.3")
    arcpy.CalculateField_management(in_table=vector_bound_path, field=FIELD_INFO[IS_CLASSIFIED][0], expression='"{}"'.format(isClassified), expression_type="PYTHON_9.3")
    arcpy.CalculateField_management(in_table=vector_bound_path, field=FIELD_INFO[AREA][0], expression=footprint_area, expression_type="PYTHON_9.3")
#     try:
#         # try to calculate a z range
#         arcpy.CalculateField_management(in_table=vector_bound_path, field=FIELD_INFO[RANGE][0], expression="!{}! - !{}!".format(FIELD_INFO[MAX][0], FIELD_INFO[MIN][0]), expression_type="PYTHON_9.3")
#     except:
#         pass

    if stat_props is not None: 
        addKeyFieldValues(vector_bound_path, stat_props)
    else:
        arcpy.AddWarning("  WARNING: Failed to find raster props {}".format(stat_props))
        
    parent_path = os.path.split(vector_bound_path)[0]
    stat_file_path = os.path.join(parent_path, "S_{}.txt".format(f_name))
    if os.path.exists(stat_file_path): 
        addStatFileFieldsToBound(vector_bound_path, stat_file_path)
    else:
        arcpy.AddWarning("  WARNING: Failed to find stat file {}".format(stat_file_path))
            
    info_file_path = os.path.join(parent_path, "I_{}.shp".format(f_name))
    if os.path.exists(info_file_path): 
        addInfoFileFieldsToBound(vector_bound_path, info_file_path)
    else:
        arcpy.AddWarning("  WARNING: Failed to find info file {}".format(info_file_path))
      
    doTime(a, "\tCreated BOUND {}".format(vector_bound_path))
        
'''
--------------------------------------------------------------------------------
Exports the LAS Dataset Info shape file. Do this to get to Point Spacing values.
--------------------------------------------------------------------------------
'''
def createLasDatasetInfo(point_file_path, stat_out_folder, f_name, f_path, spatial_reference):
    a = datetime.now()
     
    point_file_path1 = os.path.join(stat_out_folder, "I_{}_1.shp".format(f_name))
         
    deleteFileIfExists(point_file_path, useArcpy=True)
    deleteFileIfExists(point_file_path1, useArcpy=True)
    
    arcpy.PointFileInformation_3d(input=f_path, out_feature_class=point_file_path, in_file_type="LAS", input_coordinate_system=spatial_reference, folder_recursion="NO_RECURSION", extrude_geometry="NO_EXTRUSION", decimal_separator="DECIMAL_POINT", summarize_by_class_code="NO_SUMMARIZE", improve_las_point_spacing="LAS_SPACING")
    addToolMessages()

    tries = 0
    success = False
    while not success and tries < MAX_TRIES:
        tries = tries + 1
        try:
            if not fieldExists(point_file_path, "Class"): 
                arcpy.AddField_management(in_table=point_file_path, field_name="Class", field_type="LONG")
            arcpy.CalculateField_management(in_table=point_file_path, field="Class", expression=-1, expression_type="PYTHON_9.3")
            success = True
            
        except Exception e:
            if tries >= MAX_TRIES:
                raise e
            else:
                pass
            
    # Create the rows for the other classes with -1 (null) values
    blank_rows = {}
    info_fields = ["SHAPE@", "FileName", "Class", "Pt_Count", "Pt_Spacing", "Z_Min", "Z_Max"]
    for row in arcpy.da.SearchCursor(point_file_path, info_fields):  # @UndefinedVariable
        blank_row = list(row)
        for idx in range(3, 7):
            blank_row[idx] = -1
        for clazz in range(0, 18):
            if clazz <> 7:
                clazz_row = copy.deepcopy(blank_row)
                clazz_row[2] = clazz
                blank_rows[clazz] = clazz_row
        
    
    arcpy.PointFileInformation_3d(input=f_path, out_feature_class=point_file_path1, in_file_type="LAS", input_coordinate_system=spatial_reference, folder_recursion="NO_RECURSION", extrude_geometry="NO_EXTRUSION", decimal_separator="DECIMAL_POINT", summarize_by_class_code="SUMMARIZE", improve_las_point_spacing="NO_LAS_SPACING")
    arcpy.Append_management(inputs=point_file_path1, target=point_file_path, schema_type="TEST")
    arcpy.Delete_management(in_data=point_file_path1, data_type="ShapeFile")
    
    # Delete the rows we have values for from the empty list
    for row in arcpy.da.SearchCursor(point_file_path, info_fields):  # @UndefinedVariable
        clazz = None if row[2] is None else int(row[2])
        if clazz is not None and clazz >= 0  and clazz <> 7 and  clazz <> 18:
            del blank_rows[clazz]
    
    # Add the rows for the other classes with -1 (null) values
    insert_curser = arcpy.da.InsertCursor(point_file_path, info_fields)  # @UndefinedVariable
    for row in blank_rows.values():
        insert_curser.insertRow(row)
    del insert_curser
    
    doTime(a, "\tCreated PINFO {}".format(point_file_path))


'''
--------------------------------------------------------------------------------
Evaluates two z values for their valid-ness, and returns the list of valid values
returns None or a list of one or two values
--------------------------------------------------------------------------------
'''
def chooseZValue(za, zb):
    result = None
    
    if za is None or za < -430 or za > 15000:
        za = None
    
    if zb is None or zb < -430 or zb > 15000:
        zb = None
    
    if za is not None and zb is None:
        result = [za]
    elif za is None and zb is not None:
        result = [zb]
    elif za is not None and zb is not None:
        result = [za, zb]
    
    return result


def clipDerivedRaster(out_raster_path, vector_bound_path):
    a = datetime.now()
    if os.path.exists(out_raster_path):
        raster_path, raster_name = os.path.split(out_raster_path) 
        temp_raster_path = os.path.join(raster_path, "T{}".format(raster_name))
        arcpy.Rename_management(out_raster_path, temp_raster_path)
        
        try:
            arcpy.Clip_management(in_raster=temp_raster_path, out_raster=out_raster_path, in_template_dataset=vector_bound_path, nodata_value=RasterConfig.NODATA_DEFAULT, clipping_geometry="ClippingGeometry", maintain_clipping_extent="NO_MAINTAIN_EXTENT")
        except:
            try:
                # NO Data value may not be applicable to this raster type
                arcpy.Clip_management(in_raster=temp_raster_path, out_raster=out_raster_path, in_template_dataset=vector_bound_path, nodata_value="", clipping_geometry="ClippingGeometry", maintain_clipping_extent="NO_MAINTAIN_EXTENT")
            except:
                pass
        deleteFileIfExists(temp_raster_path, True)
    
        doTime(a, "\tClip raster {}".format(out_raster_path))    
    

'''

'''



def exportElevation(target_path, isClassified, f_name, lasd_path, createMissingRasters=False):
    lasd_last = None
    lasd_first = None
    lasd_alast = None
    value_field = "ELEVATION"
    dataset_names = ["_FIRST", "_LAST"]
    if createMissingRasters:
        dataset_names = ["_FIRST", "_LAST", "_ALAST"]
    
    
    for dataset_name in dataset_names:
        name = dataset_name
        lasd = lasd_path
        if not isClassified:
            # Using a generic name for non-classified data
            name = ""
        out_folder = os.path.join(target_path, value_field)
        if len(name) > 0:
            out_folder = os.path.join(target_path, value_field, name[1:])
        if not os.path.exists(out_folder):
            os.makedirs(out_folder)
        out_raster = os.path.join(out_folder, "{}{}".format(f_name, name))
        out_raster_path = "{}.tif".format(out_raster)
        clip_raster_path = os.path.join(out_folder, "C_{}{}.tif".format(f_name, name))
        
        if os.path.exists(clip_raster_path):
            arcpy.AddMessage("\tRast file exists: {}".format(clip_raster_path))
        elif os.path.exists(out_raster_path):
            arcpy.AddMessage("\tRast file exists: {}".format(out_raster_path))
        else:
            a = datetime.now()
            # do this here to avoid arcpy penalty if they all exist
            if isClassified:
                if name == "_" + LAST:
                    if lasd_last is None:
                        lasd_last = arcpy.MakeLasDatasetLayer_management(in_las_dataset=lasd_path, out_layer="LasDataset_last", class_code="0;2;8;9;10;11;12", return_values="'Last Return'", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                    lasd = lasd_last
                elif name == "_" + ALAST:
                    if lasd_alast is None:
                        lasd_alast = arcpy.MakeLasDatasetLayer_management(in_las_dataset=lasd_path, out_layer="LasDataset_alast", class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="'Last Return';'Last of Many';'Single Return'", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                    lasd = lasd_alast
                elif name == "_" + FIRST:
                    if lasd_first is None:
                        lasd_first = arcpy.MakeLasDatasetLayer_management(in_las_dataset=lasd_path, out_layer="LasDataset_first", class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="1", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                    lasd = lasd_first
            
            cell_size = getCellSize(spatial_reference, ELE_CELL_SIZE, createMissingRasters)
            
            arcpy.LasDatasetToRaster_conversion(in_las_dataset=lasd, out_raster=out_raster_path, value_field=value_field, interpolation_type="BINNING AVERAGE LINEAR", data_type="FLOAT", sampling_type="CELLSIZE", sampling_value=cell_size, z_factor="1")
            arcpy.BuildPyramidsandStatistics_management(in_workspace=out_raster_path, build_pyramids="NONE", calculate_statistics="CALCULATE_STATISTICS", BUILD_ON_SOURCE="BUILD_ON_SOURCE", pyramid_level="-1", SKIP_FIRST="NONE", resample_technique="BILINEAR", compression_type="NONE", compression_quality="75", skip_existing="SKIP_EXISTING")
            doTime(a, "\tCreated ELE {}".format(out_raster))
    
    return lasd_last, lasd_first


def exportIntensity(target_path, isClassified, f_name, lasd_path, createMissingRasters=False):
    lasd_first = None
    value_field = INT
    
    for dataset_name in ["_" + FIRST]:
        name = dataset_name
        lasd = lasd_path
        if not isClassified:
            # Using a generic name for non-classified data
            name = ""
        out_folder = os.path.join(target_path, value_field)
        if len(name) > 0:
            out_folder = os.path.join(target_path, value_field, name[1:])
        if not os.path.exists(out_folder):
            os.makedirs(out_folder)
        out_raster = os.path.join(out_folder, "{}{}".format(f_name, name))
        out_raster_path = "{}.tif".format(out_raster)
        clip_raster_path = os.path.join(out_folder, "C_{}{}.tif".format(f_name, name))
        
        if os.path.exists(clip_raster_path):
            arcpy.AddMessage("\tRast file exists: {}".format(clip_raster_path))
        elif os.path.exists(out_raster_path):
            arcpy.AddMessage("\tRast file exists: {}".format(out_raster_path))
        else:
            a = datetime.now()
            # do this here to avoid arcpy penalty if they all exist
            if isClassified:
#                 if name == "_LAST":
#                     if lasd_last is None:
#                         lasd_last = arcpy.MakeLasDatasetLayer_management(in_las_dataset=lasd_path, out_layer="LasDataset_last", class_code="0;2;8;9;10;11;12", return_values="'Last Return'", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
#                     lasd = lasd_last
#                 elif name == "_ALAST":
#                     if lasd_alast is None:
#                         lasd_alast = arcpy.MakeLasDatasetLayer_management(in_las_dataset=lasd_path, out_layer="LasDataset_alast", class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="'Last Return';'Last of Many';'Single Return'", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
#                     lasd = lasd_alast
                if name == "_" + FIRST:
                    if lasd_first is None:
                        lasd_first = arcpy.MakeLasDatasetLayer_management(in_las_dataset=lasd_path, out_layer="LasDataset_first", class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="1", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                    lasd = lasd_first
            
            cell_size = getCellSize(spatial_reference, ELE_CELL_SIZE, createMissingRasters)
            arcpy.LasDatasetToRaster_conversion(in_las_dataset=lasd, out_raster=out_raster_path, value_field=value_field, interpolation_type="BINNING AVERAGE LINEAR", data_type="FLOAT", sampling_type="CELLSIZE", sampling_value=cell_size, z_factor="1")
            arcpy.BuildPyramidsandStatistics_management(in_workspace=out_raster_path, build_pyramids="NONE", calculate_statistics="CALCULATE_STATISTICS", BUILD_ON_SOURCE="BUILD_ON_SOURCE", pyramid_level="-1", SKIP_FIRST="NONE", resample_technique="BILINEAR", compression_type="NONE", compression_quality="75", skip_existing="SKIP_EXISTING")
            doTime(a, "\tCreated INT {}".format(out_raster))
    
    return lasd_first

def getCellSize(spatial_reference, cell_size, createMissingRasters=False):
    result = cell_size
    
    if createMissingRasters:
        result = 1
        
    if spatial_reference is not None:
        try:
            horz_cs_name, horz_cs_unit_name, horz_cs_factory_code, vert_cs_name, vert_unit_name = Utility.getSRValues(spatial_reference)  # @UnusedVariable
            
            horz_cs_unit_name = str(horz_cs_unit_name).upper()
            #arcpy.AddMessage("\t\tGet Cell Size for hunit {}".format(horz_cs_unit_name))
            
            if horz_cs_unit_name.find("FT") >= 0 or horz_cs_unit_name.find("FOOT") >= 0 or horz_cs_unit_name.find("FEET") >= 0:
                result = result / 0.3048
                #if horz_cs_unit_name.find("INT") > 0:
                #    result = result / 0.3048
                    
                if horz_cs_unit_name.find("US") > 0:
                    result = result / (1200 / 3937)

            arcpy.AddMessage("\t\tCell Size for hunit {}={}".format(horz_cs_unit_name,result))
            
                    
        except:
            pass
    arcpy.AddMessage("\t\tCell Size = {}".format(result))
    return result
#   processFile(f_path, target_path, spatial_reference, isClassified, createQARasters,       createMissingRasters,       overrideBorderPath)
def processFile(f_path, target_path, spatial_reference, isClassified, createQARasters=False, createMissingRasters=False, overrideBorderPath=None):
    if not isinstance(createQARasters, bool):
        createQARasters = (str(createQARasters) in ['True', 'true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh'])
        arcpy.AddMessage("Converted createQARaters to bool {}".format(createQARasters))
    arcpy.AddMessage("A04_B Process File: f_path={}\n\ttarget_path{}, spatial_reference={}\n\tisClassified={}\n\tcreateQARasters={}\n\tcreateMissingRasters={}\n\toverrideBorderPath={}".format(f_path, target_path, spatial_reference, isClassified, createQARasters, createMissingRasters, overrideBorderPath))
    aa = datetime.now()

    f_name = os.path.split(os.path.splitext(f_path)[0])[1]
    
    cell_size = getCellSize(spatial_reference, CELL_SIZE)
    
    las_folder = lasClassified_dir
    if not isClassified:
        las_folder = lasUnclassified_dir
        
    out_lasd_path = os.path.join(target_path, las_folder, lasd_dir, "{}.lasd".format(f_name))
    out_las_path = os.path.join(target_path, las_folder, "{}.las".format(f_name))
    out_lasx_path = "{}x".format(out_las_path)
        
    if os.path.exists(out_lasx_path) and os.path.exists(out_las_path) and os.path.exists(out_lasd_path):
        arcpy.AddMessage("\tlasx, las, and lasd file exists: \n\t{}\n\t{}\n\t{}".format(out_lasx_path, out_las_path, out_lasd_path))
    else:
        deleteFileIfExists(out_lasd_path)
        deleteFileIfExists(out_las_path)
        deleteFileIfExists(out_lasx_path)
        createLasDataset(f_name, f_path, spatial_reference, target_path, isClassified)

    desc = arcpy.Describe(out_lasd_path)
    point_count = desc.pointCount
    arcpy.AddMessage("\t\tLAS file {} has {} points".format(f_name, point_count))
    
    # Make the STAT folder if it doesn't already exist
    stat_out_folder = os.path.join(target_path, STAT_LAS_FOLDER)
    if not os.path.exists(stat_out_folder):
        os.makedirs(stat_out_folder)

    # Export LAS Stats file from lasx 
    stat_file_path = os.path.join(stat_out_folder, "S_{}.txt".format(f_name))
    if os.path.exists(stat_file_path):
        arcpy.AddMessage("\tStat file exists: {}".format(stat_file_path))
    else:
        createLasDatasetStats(out_lasd_path, f_path, spatial_reference, stat_file_path)
    
    # Export Point File Information
    point_file_path = os.path.join(stat_out_folder, "I_{}.shp".format(f_name))    
    if os.path.exists(point_file_path):
        arcpy.AddMessage("\tInfo file exists: {}".format(point_file_path))
    else:
        createLasDatasetInfo(point_file_path, stat_out_folder, f_name, f_path, spatial_reference)
    
    
    # Create the derived files
    
    lasd_all = None
    
    lasd_last, lasd_first = exportElevation(target_path, isClassified, f_name, out_lasd_path, createMissingRasters)
    if createMissingRasters:
        lasd_first = exportIntensity(target_path, isClassified, f_name, out_lasd_path, createMissingRasters)
    
    # Export the boundary shape file
    vector_bound_path = os.path.join(stat_out_folder, "B_{}.shp".format(f_name))
    vector_bound_B_path = os.path.join(stat_out_folder, "B_{}.shp".format(f_name))
    vector_bound_C_path = os.path.join(stat_out_folder, "C_{}.shp".format(f_name))
    # if os.path.exists(vector_bound_path):
    if os.path.exists(vector_bound_B_path) and os.path.exists(vector_bound_C_path) :
        arcpy.AddMessage("\tBound files exists: {}".format(vector_bound_path))
    else:
        
        value_field = ELEVATION
        name = "_" + FIRST
        if not isClassified:
            # Using a generic name for non-classified data
            name = ""
        
        out_folder = os.path.join(target_path, value_field)
        if len(name) > 0:
            out_folder = os.path.join(target_path, value_field, name[1:])
        
        out_raster = os.path.join(out_folder, "{}{}".format(f_name, name))
        out_raster_path = "{}.tif".format(out_raster)
        if not os.path.exists(out_raster_path):
            out_raster = os.path.join(out_folder, "C_{}{}".format(f_name, name))
            out_raster_path = "{}.tif".format(out_raster)
        
        
        stat_props = Raster.createRasterDatasetStats(out_raster_path)
        
        
        minz = stat_props[MIN]
        maxz = stat_props[MAX]
        meanz = stat_props[MEAN]
        stdevz = stat_props[STAND_DEV]
        
        value_field = ELEVATION
        name = "_" + LAST
        if not isClassified:
            # Using a generic name for non-classified data
            name = ""
        
        out_folder = os.path.join(target_path, value_field)
        if len(name) > 0:
            out_folder = os.path.join(target_path, value_field, name[1:])
        
        out_raster = os.path.join(out_folder, "{}{}".format(f_name, name))
        out_raster_path = "{}.tif".format(out_raster)
        if not os.path.exists(out_raster_path):
            out_raster = os.path.join(out_folder, "C_{}{}".format(f_name, name))
            out_raster_path = "{}.tif".format(out_raster)
        
        stat_props = Raster.createRasterDatasetStats(out_raster_path)
        # first argument is the preferred value (from the 'all' data set)
        maxz = chooseZValue(maxz, stat_props[MAX])
        minz = chooseZValue(minz, stat_props[MIN])
        meanz = chooseZValue(meanz, stat_props[MEAN])
        stdevz = chooseZValue(stdevz, stat_props[STAND_DEV])
        
        stat_props[MAX] = (None if maxz is None else max(maxz))
        stat_props[MIN] = (None if minz is None else min(minz))
        stat_props[MEAN] = (None if meanz is None else meanz[0])
        stat_props[STAND_DEV] = (None if stdevz is None else stdevz[0])
        try:
            if stat_props[MAX] is not None and stat_props[MIN] is not None: 
                stat_props[RANGE] = (stat_props[MAX] - stat_props[MIN])
        except:
            pass
        
        # NOT USED (slow & same result as B): createVectorBoundaryA(stat_out_folder, f_name, lasd_path, vector_bound_path)
        
        if not os.path.exists(vector_bound_B_path):
            if point_count > SMALL_POINT_COUNT:
                createVectorBoundaryB(spatial_reference, stat_out_folder, f_name, f_path, vector_bound_B_path)
            else:
                try:
                    createVectorBoundaryB(spatial_reference, stat_out_folder, f_name, f_path, vector_bound_B_path)
                except:
                    arcpy.AddWarning("Failed to build boundary B, but point count is small. Ignoring error for {}.".format(f_name))
        
        if not os.path.exists(vector_bound_C_path):
            if point_count > SMALL_POINT_COUNT:
                createVectorBoundaryC(out_raster_path, vector_bound_C_path, isClassified, stat_props)
            else:
                try:
                    createVectorBoundaryC(out_raster_path, vector_bound_C_path, isClassified, stat_props)
                except:
                    arcpy.AddWarning("Failed to build boundary C, but point count is small. Ignoring error for {}.".format(f_name))
        
            
    
    # Do later with the final boundary file
#     if len(out_rasters) > 0:
#         vector_bound_B_path = os.path.join(stat_out_folder, "BB{}.shp".format(f_name))
#         arcpy.Buffer_analysis(in_features=vector_bound_path, out_feature_class=vector_bound_B_path, buffer_distance_or_field="1 Meters", line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
#     
#         for out_raster_path in out_rasters:
#             clipDerivedRaster(out_raster_path, vector_bound_B_path)
#         
#         deleteFileIfExists(vector_bound_B_path, True)
        
    # Create the QA statistics files
    if createQARasters and (point_count > SMALL_POINT_COUNT):
        try:
            arcpy.AddMessage("Creating QA Raters = {}".format(createQARasters))
            cell_size = getCellSize(spatial_reference, CELL_SIZE)
            # Create the statistics rasters
            stats_methods = STATS_METHODS
            for dataset_name in DATASET_NAMES:
                name = dataset_name
                lasd = out_lasd_path
                                
                if not isClassified:
                    # Using a generic name for non-classified data
                    name = ""
                
                for method in stats_methods:
                    if createQARasters or method == pulse_count_dir or method == point_count_dir:
                        out_folder = os.path.join(target_path, method)
                        if len(name) > 0:
                            out_folder = os.path.join(target_path, method, name[1:])
                            
                        if not os.path.exists(out_folder):
                            os.makedirs(out_folder)
                        
                        out_raster = os.path.join(out_folder, "{}{}".format(f_name, name))
                        out_raster_path = "{}.tif".format(out_raster)
                        out_raster1_path = "{}1.tif".format(out_raster)
                        
                        if os.path.exists(out_raster_path):
                            # arcpy.AddMessage("\tRaster already exists, skipping creation: {}".format(out_raster_path))
                            continue
                        else:
                            a = datetime.now()
                            
                                
                            
                            # do this here to avoid arcpy penalty if they all exist
                            if isClassified:
                                if name == "_" + LAST:
                                    if lasd_last is None:
                                        lasd_last = arcpy.MakeLasDatasetLayer_management(in_las_dataset=out_lasd_path, out_layer="LasDataset_last", class_code="0;2;8;9;10;11;12", return_values="'Last Return'", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                                    lasd = lasd_last
                                elif name == "_" + FIRST:
                                    if lasd_first is None:
                                        lasd_first = arcpy.MakeLasDatasetLayer_management(in_las_dataset=out_lasd_path, out_layer="LasDataset_first", class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="1", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                                    lasd = lasd_first
                                elif name == '_' + ALL:
                                    if lasd_all is None:
                                        lasd_all = arcpy.MakeLasDatasetLayer_management(in_las_dataset=out_lasd_path, out_layer="LasDataset_All", class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="'Last Return';'First of Many';'Last of Many';'Single Return';1;2;3;4;5", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                                    lasd = lasd_all

                            arcpy.LasPointStatsAsRaster_management(lasd, out_raster_path, method, SAMPLE_TYPE, cell_size)

        ##                    # NOTE: This section removed so long as CELL_SIZE==10Raster is integer type, units chaged to count/100 sq meters)                        
        ##                    if ((method is pulse_count_dir) or (method is point_count_dir)):
        ##                        # divide the cells by cell size (meters) squared, overwrite
        ##                        
        ##                        os.rename(out_raster_path, out_raster1_path)
        ##                        ras1 = arcpy.Raster(out_raster1_path)
        ##                        ras = ras1 / ((CELL_SIZE / 10.0) ** 2) # divide by 10 meters to make units count/100 sq meters
        ##                        del ras1
        ##                        ras.save(out_raster_path)
        ##                        arcpy.Delete_management(out_raster1_path)
        ##                        del ras
                            
                            arcpy.BuildPyramidsandStatistics_management(in_workspace=out_raster_path,
                                                                        build_pyramids="BUILD_PYRAMIDS",
                                                                        calculate_statistics="CALCULATE_STATISTICS",
                                                                        BUILD_ON_SOURCE="BUILD_ON_SOURCE",
                                                                        pyramid_level="-1",
                                                                        SKIP_FIRST="NONE",
                                                                        resample_technique="NEAREST",
                                                                        compression_type="LZ77",
                                                                        compression_quality="75",
                                                                        skip_existing="SKIP_EXISTING")
                            
                            doTime(a, "\tCreated {}x{} RASTER {}".format(cell_size, cell_size, out_raster))
        except:
            arcpy.AddWarning("Failed to create RASTER")
    
    # Don't remove the .lasd file since we may need it again if something went wrong above
    # arcpy.Delete_management(lasd_path)
    
    try:
        if arcpy.Exists("LasDataset_last"):
            arcpy.Delete_management("LasDataset_last")
        if arcpy.Exists("LasDataset_alast"):
            arcpy.Delete_management("LasDataset_alast")
        if arcpy.Exists("LasDataset_first"):
            arcpy.Delete_management("LasDataset_first")
        if arcpy.Exists("LasDataset_all"):
            arcpy.Delete_management("LasDataset_all")
    except:
        pass
    
    doTime(aa, "   Completed {}".format(f_path))
    try:
        del f_name, out_lasd_path, out_lasx_path, stat_out_folder, stat_file_path, point_file_path, lasd_all, lasd_last, lasd_first, value_field, name, lasd, out_folder, out_raster, out_raster_path, dataset_name, vector_bound_path, stat_props, minz, maxz, meanz, stdevz, out_raster1_path
    except:
        pass

'''
--------------------------------------------------------------------------------
Operates on a single .las file to calcluate the following:

1. Verify if the .lasx file exists, if not it creates it by creating a .lasd
2. Export the statistics .txt file
3. [removed] Export the point file information shape file
4. Calculates the boundary of the .las file using a mosaic dataset
5. [optional] Exports a number of statistical QA rasters (point count, predominate class, etc.)

Inputs:
    f_path = The full file path to the .las file
    target_path = the full path to the DERIVED folder for the project
    spatial_reference = The spatial reference or a full path to a .prj file
    isClassified = True or False. Default is True
    createQARasters = True or False: True creates the QA statistical rasters. Default is False

Outputs:
    DERIVED/STATS/S_<f_name>.txt = The statistics text file for the .las file
    [removed] DERIVED/STATS/I_<f_name>.shp = The point file information shape file for the .las file
    DERIVED/STATS/B_<f_name>.shp = The boundary shape file for the .las file
    n DERIVED/<Statistic>/[ALL|FIRST|LAST]/<f_name>.tif = The QA statistic file for the given Statistic. Classified data is further separated into folders for All, First, and Last returns.
--------------------------------------------------------------------------------
'''
if __name__ == '__main__':
    
    # give time for things to wake up
    time.sleep(1)
    
    # time parameters to gauge how much time things are taking
    aaa = datetime.now()
    
    f_paths = None
    target_path = None
    spatial_reference = None
    isClassified = True
    createQARasters = True
    createMissingRasters = False
    checkedOut = False
    overrideBorderPath = None
    
    if len(sys.argv) > 1:
        f_paths = sys.argv[1]
    
    if len(sys.argv) > 2:
        target_path = sys.argv[2]
        
    if len(sys.argv) > 3:
        spatial_reference = sys.argv[3]
    
    if len(sys.argv) > 4:
        arcpy.AddMessage("isClassified argv = '{}'".format(sys.argv[4]))
        isClassified = (str(sys.argv[4]).upper() == "TRUE")
    
    if len(sys.argv) > 5:
        arcpy.AddMessage("createQARasters argv = '{}'".format(sys.argv[5]))
        createQARasters = (str(sys.argv[5]).upper() == "TRUE")
        if not isinstance(createQARasters, bool):
            createQARasters = (str(createQARasters) in ['True', 'true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh'])
            arcpy.AddMessage("Converted createQARaters to bool {}".format(createQARasters))
    
    if len(sys.argv) > 6:
        arcpy.AddMessage("createMissingRasters argv = '{}'".format(sys.argv[6]))
        createMissingRasters =  (str(sys.argv[6]).upper() == "TRUE")

    if len(sys.argv) > 7:
        overrideBorderPath = sys.argv[7]    
    
    arcpy.AddMessage("\n\tf_paths='{}',\n\ttarget_path='{}',\n\tspatial_reference='{}',\n\tisClassified='{}',\n\tcreateQARasters='{}',\n\tcreateMissingRasters='{}',\n\toverrideBorderPath='{}'".format(f_paths, target_path, spatial_reference, isClassified, createQARasters, createMissingRasters, overrideBorderPath))
    
    f_paths = str(f_paths).split(",")
     
    for f_path in f_paths:    
        if not isProcessFile(f_path, target_path, createQARasters, isClassified, createMissingRasters):
            arcpy.AddMessage("\tAll las file artifacts exist. Ignoring: {}".format(f_path))
        else:
            if not checkedOut:
                arcpy.AddMessage("\tChecking out licenses")
                arcpy.CheckOutExtension("3D")
                arcpy.CheckOutExtension("Spatial")
                checkedOut = True
            
            processFile(f_path, target_path, spatial_reference, isClassified, createQARasters, createMissingRasters, overrideBorderPath)

    if checkedOut:
        arcpy.CheckInExtension("3D")
        arcpy.CheckInExtension("Spatial")
    
    doTime(aaa, "  Completed A04_B_CreateLASStats")
            
            
            
    
