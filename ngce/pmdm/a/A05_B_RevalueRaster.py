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
from ngce.Utility import isMatchingStringValue, deleteFileIfExists, doTime
from ngce.folders.FoldersConfig import INT
from ngce.raster import RasterConfig
from ngce.raster.Raster import createRasterDatasetStats
from ngce.raster.RasterConfig import STAT_FOLDER_ORG, STAT_RASTER_FOLDER, FIELD_INFO, \
    PATH, NAME, AREA, ELEV_TYPE, RANGE, KEY_LIST, MAX, MIN, BAND_COUNT, \
    PIXEL_TYPE, PIXEL_TYPE_F32, PIXEL_TYPE_D64, FORMAT, V_NAME, V_UNIT, H_NAME, \
    H_UNIT, H_WKID, STAT_FOLDER_DER, STAT_FOLDER_PUB


arcpy.env.parallelProcessingFactor = "80%"

Utility.setArcpyEnv(True)


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
    
    bound_out_folder = os.path.join(target_path, STAT_RASTER_FOLDER, elev_type)
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
    Utility.setArcpyEnv(is_overwrite_output=True)
    a = datetime.now()
    nodata = RasterConfig.NODATA_DEFAULT     
    isInt = (elev_type == INT)
    if isInt:
        minZ, maxZ = 0, 255
    
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
        # Intensity may be another type
        if not isInt and not (raster_props[PIXEL_TYPE] == PIXEL_TYPE_F32 or raster_props[PIXEL_TYPE] == PIXEL_TYPE_D64):
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
                
                    if spatial_ref is not None:
                        arcpy.AddMessage("Applying projection to raster '{}' {}".format(target_f_path, spatial_ref))
                        if str(spatial_ref).lower().endswith(".prj"):
                            spatial_ref = arcpy.SpatialReference(spatial_ref)
                        arcpy.AddMessage("Applying projection to raster '{}' {}".format(target_f_path, spatial_ref.exportToString()))
                        arcpy.DefineProjection_management(in_dataset=target_f_path, coor_system=spatial_ref)
                    
                    # Set the no data default value on the input raster
                    arcpy.SetRasterProperties_management(in_raster=target_f_path, data_type="ELEVATION", nodata="1 {}".format(nodata))    
                    arcpy.CalculateStatistics_management(in_raster_dataset=target_f_path, x_skip_factor="1", y_skip_factor="1", ignore_values="", skip_existing="OVERWRITE", area_of_interest="Feature Set")
#                     arcpy.BuildPyramidsandStatistics_management(in_workspace=target_f_path,
#                                                                 build_pyramids="BUILD_PYRAMIDS",
#                                                                 calculate_statistics="CALCULATE_STATISTICS",
#                                                                 BUILD_ON_SOURCE="BUILD_ON_SOURCE",
#                                                                 pyramid_level="-1",
#                                                                 SKIP_FIRST="NONE",
#                                                                 resample_technique="BILINEAR",
#                                                                 compression_type="LZ77",
#                                                                 compression_quality="75",
#                                                                 skip_existing="SKIP_EXISTING")
                    
                    
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
                    
                    arcpy.SetRasterProperties_management(in_raster=publish_f_path, data_type="ELEVATION", nodata="1 {}".format(nodata))
                    arcpy.CalculateStatistics_management(in_raster_dataset=publish_f_path, x_skip_factor="1", y_skip_factor="1", ignore_values="", skip_existing="OVERWRITE", area_of_interest="Feature Set")
#                     arcpy.BuildPyramidsandStatistics_management(in_workspace=publish_f_path,
#                                                                 build_pyramids="BUILD_PYRAMIDS",
#                                                                 calculate_statistics="CALCULATE_STATISTICS",
#                                                                 BUILD_ON_SOURCE="BUILD_ON_SOURCE",
#                                                                 pyramid_level="-1",
#                                                                 SKIP_FIRST="NONE",
#                                                                 resample_technique="BILINEAR",
#                                                                 compression_type="LZ77",
#                                                                 compression_quality="75",
#                                                                 skip_existing="SKIP_EXISTING")
                    
                    a = doTime(a, "\tCopied '{}' to '{}'".format(target_f_path, publish_f_path))
                


def CheckRasterSpatialReference(v_name, v_unit, h_name, h_unit, h_wkid, raster_props, spatial_ref=None):
    sr = None
    try:
        if raster_props is not None:
            f_name = raster_props[NAME]
            # Check the raster spatial
            isVName = isMatchingStringValue(v_name, raster_props[V_NAME])
            if isVName:
                arcpy.AddMessage("RASTER CHECK '{}': Vertical Name from LAS '{}' {} raster file '{}'".format(f_name, v_name, ("Matches" if isVName else "Does NOT Match"), raster_props[V_NAME]))
            else:
                arcpy.AddWarning("WARNING: RASTER CHECK '{}': Vertical Name from LAS '{}' {} raster file '{}'".format(f_name, v_name, ("Matches" if isVName else "Does NOT Match"), raster_props[V_NAME]))
            
            isVUnit = isMatchingStringValue(v_unit, raster_props[V_UNIT])
            if isVUnit:
                arcpy.AddMessage("RASTER CHECK '{}': Vertical Unit from LAS '{}' {} raster file '{}'".format(f_name, v_unit, ("Matches" if isVUnit else "Does NOT Match"), raster_props[V_UNIT]))
            else:
                arcpy.AddWarning("WARNING: RASTER CHECK '{}': Vertical Unit from LAS '{}' {} raster file '{}'".format(f_name, v_unit, ("Matches" if isVUnit else "Does NOT Match"), raster_props[V_UNIT]))
            
            isHName = isMatchingStringValue(h_name, raster_props[H_NAME])    
            if isHName:
                arcpy.AddMessage("RASTER CHECK '{}': H_NAME from LAS '{}' {} raster file '{}'".format(f_name, h_name, ("Matches" if isHName else "Does NOT Match"), raster_props[H_NAME]))
            else:
                arcpy.AddWarning("WARNING: RASTER CHECK '{}': H_NAME from LAS '{}' {} raster file '{}'".format(f_name, h_name, ("Matches" if isHName else "Does NOT Match"), raster_props[H_NAME]))
            
            isHUnit = isMatchingStringValue(h_unit, raster_props[H_UNIT])
            if isHUnit:
                arcpy.AddMessage("RASTER CHECK '{}': Horizontal Unit from LAS '{}' {} raster file '{}'".format(f_name, h_unit, ("Matches" if isHUnit else "Does NOT Match"), raster_props[H_UNIT]))
            else:
                arcpy.AddWarning("WARNING: RASTER CHECK '{}': Horizontal Unit from LAS '{}' {} raster file '{}'".format(f_name, h_unit, ("Matches" if isHUnit else "Does NOT Match"), raster_props[H_UNIT]))
                
                
            isHwkid = isMatchingStringValue(h_wkid, raster_props[H_WKID])
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
        
        
        
        

