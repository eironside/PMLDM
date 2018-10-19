'''
Created on Dec 11, 2015

@author: eric5946


#-------------------------------------------------------------------------------
# Name:                NRCS_LoadVendorProject
#
# Purpose:         To create a mosaic dataset and ingest with:
#
#                            1) Rasters (Vendor-supplied or created using LAS Dataset to Tiled raster)
#                            2) LAS tiles
#
#                            This GP tool creates a Mosaic Dataset, adds Project-related fields
#                            and indices to it, then populates the MD with rasters, and (optionally)
#                            LAS tiles.
#
#                            The applicable MD properties are set, overviews are defined and built,
#                            and finally Project-related metadata fields are populated.
#
# Author:             Roslyn Dunn
# Organization: Esri Inc.
#
# Created:         012/17/2014
#
# Last Edited: 05/04/15     Force the spatial reference of the project to Web Mercator.
#                                                 Append the appropriate function to function chain if z_units
#                                                     (Raster or LAS) are in FOOT_US or FOOT_INTL.
#                                                 Ingest *.TIF or *.IMG data.
#                                                 Put JPEG back into list of possible compression types.
#                                                 Implement Editor Tracking for UserName and DateLoaded.
#                                                 Densify, then generalize footprints & boundary to remove slivers
#            Edited: 09/24/15     Lower minimum_elevation_value to -5 from 1 for footprint generation.
#                                                 Remove generation of raster pyramid files (ovr) in Add Rasters
#                                                     because of NoData bug in GDAL.
#                                                 Create MD with NoData value of -3.40282346639e+038
#                                                     instead of -3.40282306074E+38 (for consistency)
# *
#-------------------------------------------------------------------------------
'''

import arcpy
from datetime import datetime
import os
import sys

from ngce import Utility
from ngce.cmdr import CMDRConfig, CMDR
from ngce.cmdr.CMDRConfig import DSM, DTM, DHM, INT, OCS
from ngce.cmdr.JobUtil import getProjectFromWMXJobID
from ngce.folders import ProjectFolders
from ngce.folders.FoldersConfig import DLM, DCM
from ngce.las import LAS
from ngce.pmdm.a import A04_C_ConsolidateLASInfo, A05_A_RemoveDEMErrantValues, A05_C_ConsolidateRasterInfo, \
    A04_A_GenerateQALasDataset
from ngce.pmdm.a.A04_B_CreateLASStats import doTime
from ngce.raster import Raster, RasterConfig
from ngce.raster.RasterConfig import PROJECT_SOURCE_LAS, MOSAIC_Z_TOLERANCE

PARTITION_COUNT = 500

Utility.setArcpyEnv(True)

LAS_RASTER_TYPE = LAS.LAS_raster_type_1_all_bin_mean_idw


arcpy.env.parallelProcessingFactor = "100%"

SKIP_FACTOR_LRG = 100
SKIP_FACTOR_MED = SKIP_FACTOR_LRG/4
SKIP_FACTOR_SML = SKIP_FACTOR_LRG/10

def addNameSource(footprints, img_type):
    a = datetime.now()
    success = False
    maxTries = 10
    numTries = 0
    while not success and numTries <= maxTries:
        numTries = numTries + 1
        try:
            a = doTime(a, "\tAdding NameSource to {} footprints".format(img_type))
            Utility.addAndCalcFieldText(footprints, "NameSource", 100, field_value="!name! + '_{}'".format(img_type), field_alias="Name Source", code_block="", add_index=True, debug=True)
            success = True
        except:
            pass
    if not success:
        a = doTime(a, "\tFailed to add NameSource field to {} footprints".format(img_type))

def mergeFootprints(las_footprints, el_type, fgdb_path):
    a = datetime.now()
    aa = a
    
    raster_footprints_original = A05_C_ConsolidateRasterInfo.getRasterFootprintPath(fgdb_path, el_type)

    arcpy.AddMessage('# Raster Foot Original: {}'.format(raster_footprints_original))
    
    # Simplify these footprints even more than original (requirement of the Mosaic Dataset)
    raster_footprints_sim = "{}_SIM".format(raster_footprints_original)
    if not arcpy.Exists(raster_footprints_sim):
        arcpy.SimplifyPolygon_cartography(
            in_features=raster_footprints_original,
            out_feature_class=raster_footprints_sim,
            algorithm="POINT_REMOVE",
            tolerance=Raster.boundary_interval,
            minimum_area="0 SquareMeters",
            error_option="RESOLVE_ERRORS",
            collapsed_point_option="NO_KEEP"
            )
        Utility.addToolMessages()
        Utility.deleteFields(raster_footprints_sim)
        
        arcpy.RepairGeometry_management(in_features=raster_footprints_sim, delete_null="DELETE_NULL")
        Utility.addToolMessages()
        Utility.deleteFields(raster_footprints_sim)
        
        addNameSource(raster_footprints_sim, "RAS")
        Utility.deleteFields(raster_footprints_sim)
        
        # If LAS footprints are None, use this as the source of the footprints
        merged_raster_footprints = raster_footprints_sim
        
        a = doTime(a, "\tSimplified raster footprint geometry to {}".format(raster_footprints_sim))
    else:
        merged_raster_footprints = raster_footprints_sim
        
    # Combine the raster footprints with the LAS file footprints. 
    # NameSource differentiates the footprints in the combined feature class.
    # NameSource is used to join the footprint/fields to the mosaic dataset later on.
    if las_footprints is not None:
        # If las_footprints is supplied, merge it with the rasters
        raster_footprints_all = "{}_ALL".format(raster_footprints_original)
        if not arcpy.Exists(raster_footprints_all):
            
            arcpy.Merge_management(inputs=";".join([las_footprints, raster_footprints_sim]), output=raster_footprints_all,
                               field_mappings='path "path" true true false 254 Text 0 0 ,First,#,{1},path,-1,-1,{0},path,-1,-1;name "name" true true false 100 Text 0 0 ,First,#,{1},name,-1,-1,{0},name,-1,-1;area "area" true true false 8 Double 0 0 ,First,#,{1},area,-1,-1,{0},area,-1,-1;el_type "el_type" true true false 254 Text 0 0 ,First,#,{1},el_type,-1,-1,{1},el_type,-1,-1;zran "zran" true true false 8 Double 0 0 ,First,#,{1},zran,-1,-1,{0},zran,-1,-1;zmax "zmax" true true false 8 Double 0 0 ,First,#,{1},zmax,-1,-1,{0},zmax,-1,-1;zmean "zmean" true true false 8 Double 0 0 ,First,#,{1},zmean,-1,-1,{0},zmean,-1,-1;zmin "zmin" true true false 8 Double 0 0 ,First,#,{1},zmin,-1,-1,{0},zmin,-1,-1;zdev "zdev" true true false 8 Double 0 0 ,First,#,{1},zdev,-1,-1,{0},zdev,-1,-1;width "width" true true false 8 Double 0 0 ,First,#,{1},width,-1,-1;height "height" true true false 8 Double 0 0 ,First,#,{1},height,-1,-1;cell_h "cell_h" true true false 8 Double 0 0 ,First,#,{1},cell_h,-1,-1;cell_w "cell_w" true true false 8 Double 0 0 ,First,#,{1},cell_w,-1,-1;comp_type "comp_type" true true false 50 Text 0 0 ,First,#,{1},comp_type,-1,-1;format "format" true true false 50 Text 0 0 ,First,#,{1},format,-1,-1;pixel "pixel" true true false 100 Text 0 0 ,First,#,{1},pixel,-1,-1;unc_size "unc_size" true true false 8 Double 0 0 ,First,#,{1},unc_size,-1,-1;xmin "xmin" true true false 8 Double 0 0 ,First,#,{1},xmin,-1,-1,{0},xmin,-1,-1;ymin "ymin" true true false 8 Double 0 0 ,First,#,{1},ymin,-1,-1,{0},ymin,-1,-1;xmax "xmax" true true false 8 Double 0 0 ,First,#,{1},xmax,-1,-1,{0},xmax,-1,-1;ymax "ymax" true true false 8 Double 0 0 ,First,#,{1},ymax,-1,-1,{0},ymax,-1,-1;v_name "v_name" true true false 100 Text 0 0 ,First,#,{1},v_name,-1,-1,{0},v_name,-1,-1;v_unit "v_unit" true true false 100 Text 0 0 ,First,#,{1},v_unit,-1,-1,{0},v_unit,-1,-1;h_name "h_name" true true false 100 Text 0 0 ,First,#,{1},h_name,-1,-1,{0},h_name,-1,-1;h_unit "h_unit" true true false 100 Text 0 0 ,First,#,{1},h_unit,-1,-1,{0},h_unit,-1,-1;h_wkid "h_wkid" true true false 100 Text 0 0 ,First,#,{1},h_wkid,-1,-1,{0},h_wkid,-1,-1;nodata "nodata" true true false 8 Double 0 0 ,First,#,{1},nodata,-1,-1;Project_ID "Project_ID" true true false 50 Text 0 0 ,First,#,{1},Project_ID,-1,-1,{0},Project_ID,-1,-1;Project_Dir "Project_Dir" true true false 1000 Text 0 0 ,First,#,{1},Project_Dir,-1,-1,{0},Project_Dir,-1,-1;Project_GUID "Project_GUID" true true false 38 Guid 0 0 ,First,#,{1},Project_GUID,-1,-1,{0},Project_GUID,-1,-1;is_class "is_class" true true false 10 Text 0 0 ,First,#,{0},is_class,-1,-1;ra_pt_ct "ra_pt_ct" true true false 8 Double 0 0 ,First,#,{0},ra_pt_ct,-1,-1;ra_pt_sp "ra_pt_sp" true true false 8 Double 0 0 ,First,#,{0},ra_pt_sp,-1,-1;ra_zmin "ra_zmin" true true false 8 Double 0 0 ,First,#,{0},ra_zmin,-1,-1;ra_zmax "ra_zmax" true true false 8 Double 0 0 ,First,#,{0},ra_zmax,-1,-1;ra_zran "ra_zran" true true false 8 Double 0 0 ,First,#,{0},ra_zran,-1,-1;NameSource "Name Source" true true false 100 Text 0 0 ,First,#,{1},NameSource,-1,-1,{0},NameSource,-1,-1'.format(las_footprints, raster_footprints_sim))
            Utility.addToolMessages()
            Utility.deleteFields(raster_footprints_all)
            a = doTime(a, "\tMerged las and raster footprints for {}".format(el_type))
    
            arcpy.RepairGeometry_management(in_features=raster_footprints_all, delete_null="DELETE_NULL")
            Utility.addToolMessages()
            Utility.deleteFields(raster_footprints_all)
            
            merged_raster_footprints = raster_footprints_all

            Utility.deleteFileIfExists(raster_footprints_sim, True)
            
            a = doTime(a, "\tRepaired footprint geometry in {}".format(raster_footprints_all))
        else:
            merged_raster_footprints = raster_footprints_all
            
    
    Utility.deleteFields(merged_raster_footprints)
    doTime(aa, "Merged footprints for LAS and Raster into {}".format(merged_raster_footprints))
    
    return merged_raster_footprints


def isSpatialRefSameForAll(InputFolder):
    a = datetime.now()
    aa = a
    arcpy.env.workspace = InputFolder
    rasters = arcpy.ListRasters("*", "TIF")
    count = len(rasters)
    a = doTime(a, "Listed {} rasters from folder {}".format(count, InputFolder))
    

    SpatRefFirstRaster = None
    SRMatchFlag = True
    firstRaster = None

    arcpy.AddMessage("Checking raster spatial references for {} rasters in folder {}".format(count, InputFolder))
    for raster in rasters:
        describe = arcpy.Describe(raster)
        SRef = describe.SpatialReference.exportToString()
        SRef = str(SRef).split(';')[0] # Split off extra parameters after ';' character
        if SpatRefFirstRaster is None:
            SpatRefFirstRaster = SRef
            firstRaster = raster

        if SRef != SpatRefFirstRaster:
            SRMatchFlag = False
            arcpy.AddError("Raster has a PCSCode (EPSG code) that is different than first raster: \n\tFirst raster '{}' SR: {}\n\tThis raster '{}' SR: {}".format(firstRaster, SpatRefFirstRaster, raster, SRef))

    doTime(aa, "Checked raster spatial references for {}".format(count))
    return SRMatchFlag, count


def setMosaicDatasetProperties(md_path):

    arcpy.SetMosaicDatasetProperties_management(md_path,
                                                rows_maximum_imagesize="25000",
                                                columns_maximum_imagesize="25000",
                                                allowed_compressions="None;LZ77;JPEG;LERC",
                                                default_compression_type="LERC",
                                                JPEG_quality="75",
                                                LERC_Tolerance="0.001",
                                                resampling_type="BILINEAR",
                                                clip_to_footprints="NOT_CLIP",
                                                footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA",
                                                clip_to_boundary="NOT_CLIP",
                                                color_correction="NOT_APPLY",
                                                allowed_mensuration_capabilities="#",
                                                        default_mensuration_capabilities="NONE",
                                                        allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None",
                                                default_mosaic_method="NorthWest",
                                                order_field=CMDRConfig.PROJECT_DATE,
                                                order_base="#",
                                                sorting_order="ASCENDING",
                                                mosaic_operator="FIRST",
                                                blend_width="0",
                                                view_point_x="600",
                                                view_point_y="300",
                                                max_num_per_mosaic="20",
                                                cell_size_tolerance="0.8",
                                                cell_size="#",
                                                        metadata_level="BASIC",
                                                transmission_fields=CMDRConfig.TRANSMISSION_FIELDS,
                                                use_time="DISABLED",
                                                start_time_field=CMDRConfig.PROJECT_DATE,
                                                end_time_field="#",
                                                time_format="#",
                                                geographic_transform="#",
                                                max_num_of_download_items="20",
                                                max_num_of_records_returned="1000",
                                                data_source_type="ELEVATION",
                                                minimum_pixel_contribution="1",
                                                processing_templates="None",
                                                        default_processing_template="None")


def generateOverviews(target_path, md_name, md_path, count_rasters, spatial_ref, cellsize, boundary_fc_path = None):
    Utility.setArcpyEnv(is_overwrite_output=True)
    a = datetime.now()
    linearUnitName = spatial_ref.linearUnitName
    # Set the cell size of the first level overview according to the cell size of the Mosaic Dataset
    # Do this by doubling cell size and finding the next ArcGIS Online cache scale
    cellsizeOVR = Raster.getOverviewCellSize(cellsize)
    arcpy.AddMessage("Cell size of First level Overview:    {0} {1}".format(cellsizeOVR, linearUnitName))

    # Location of Mosaic Dataset overview TIFF files (Note: this folder needs to be in the ArcGIS Server Data Store)
    mosaic_dataset_overview_path = os.path.join(target_path, "{}.Overviews".format(md_name))
    A05_C_ConsolidateRasterInfo.deleteFileIfExists(mosaic_dataset_overview_path, True)
    arcpy.AddMessage("Mosaic Dataset Overview Folder: {0}".format(mosaic_dataset_overview_path))

    # Define how Overviews will be created and sets
    # the location of Mosaic Dataset overview TIFF files
    #     pixel size of the first level overview is cellsizeOVR
    #     overview_factor="2"
    #     force_overview_tiles="FORCE_OVERVIEW_TILES"
    #     compression_method="LZW"
    #     20180507 EI: Added the template dataset using the las dataset boundary to constrain overview generation to fix issues with .las file invalid projections
    arcpy.DefineOverviews_management(md_path, mosaic_dataset_overview_path, in_template_dataset=boundary_fc_path, extent="#", pixel_size=cellsizeOVR,
                                     number_of_levels="#", tile_rows="5120", tile_cols="5120", overview_factor="2", force_overview_tiles="FORCE_OVERVIEW_TILES",
                                     resampling_method="BILINEAR", compression_method="LZ77", compression_quality="100")
    Utility.addToolMessages()



    # Build Overviews as defined in the previous step
    #    define_missing_tiles="NO_DEFINE_MISSING_TILES"
    arcpy.BuildOverviews_management(md_path, where_clause="#", define_missing_tiles="NO_DEFINE_MISSING_TILES", generate_overviews="GENERATE_OVERVIEWS", generate_missing_images="GENERATE_MISSING_IMAGES",
                                    regenerate_stale_images="REGENERATE_STALE_IMAGES")
    Utility.addToolMessages()


#     workspace = arcpy.env.workspace  # @UndefinedVariable
#     try:
#         arcpy.env.workspace = mosaic_dataset_overview_path
#         rasters = arcpy.ListRasters("*", "TIF")
#         for raster in rasters:
#             arcpy.SetRasterProperties_management(in_raster=raster, data_type="ELEVATION", nodata="1 {}".format(RasterConfig.NODATA_DEFAULT))
#             arcpy.CalculateStatistics_management(in_raster_dataset=raster, x_skip_factor="1", y_skip_factor="1", ignore_values="", skip_existing="OVERWRITE", area_of_interest="Feature Set")
#     except:
#         arcpy.AddWarning("Failed to set props and calculate stats on overviews.")
#     finally:
#         arcpy.env.workspace = workspace
#
#     arcpy.SynchronizeMosaicDataset_management(in_mosaic_dataset=md_path, where_clause="", new_items="NO_NEW_ITEMS", sync_only_stale="SYNC_STALE", update_cellsize_ranges="NO_CELL_SIZES", update_boundary="NO_BOUNDARY", update_overviews="NO_OVERVIEWS", build_pyramids="NO_PYRAMIDS", calculate_statistics="NO_STATISTICS", build_thumbnails="NO_THUMBNAILS", build_item_cache="NO_ITEM_CACHE", rebuild_raster="NO_RASTER", update_fields="NO_FIELDS", fields_to_update="area;cell_h;cell_w;CenterX;CenterY;comp_type;created_date;created_user;el_type;format;GroupName;h_name;h_unit;h_wkid;height;is_class;last_edited_date;last_edited_user;nodata;pixel;PointCount;PointSpacing;ProductName;Project_Date;Project_Dir;Project_GUID;Project_ID;Project_Source;ra_pt_ct;ra_pt_sp;ra_zmax;ra_zmin;ra_zran;Raster;Shape;Tag;unc_size;v_name;v_unit;Version;width;xmax;xmin;ymax;ymin;zdev;ZMax;zmax_1;zmean;ZMin;zmin_1;ZOrder;zran", existing_items="UPDATE_EXISTING_ITEMS", broken_items="REMOVE_BROKEN_ITEMS", skip_existing_items="SKIP_EXISTING_ITEMS", refresh_aggregate_info="NO_REFRESH_INFO", estimate_statistics="NO_STATISTICS")
#     Utility.addToolMessages()

    arcpy.AddMessage("Building statistics on overviews: {0}".format(md_path))
    overview_layer = arcpy.MakeMosaicLayer_management(in_mosaic_dataset=md_path, out_mosaic_layer="{}_MosaicLayer".format(md_name), where_clause="TypeID = 2")
    arcpy.BuildPyramidsandStatistics_management(overview_layer, include_subdirectories="INCLUDE_SUBDIRECTORIES", build_pyramids="NONE", calculate_statistics="CALCULATE_STATISTICS", BUILD_ON_SOURCE="NONE",
                                                block_field="#", estimate_statistics="NONE",
                                                x_skip_factor=SKIP_FACTOR_LRG, y_skip_factor=SKIP_FACTOR_LRG, ignore_values="#", pyramid_level="-1", SKIP_FIRST="NONE", resample_technique="BILINEAR",
                                                compression_type="NONE", compression_quality="75", skip_existing="SKIP_EXISTING")
    Utility.addToolMessages()
    
    arcpy.CalculateField_management(in_table=overview_layer, field="Project_Source", expression='"OVR"', expression_type="PYTHON_9.3", code_block="")
    Utility.addToolMessages()
    del overview_layer

    # This tool is re-run because sometimes the clip_to_footprints="NOT_CLIP" gets re-set to "CLIP" for some reason
    setMosaicDatasetProperties(md_path)

    # Get a count to determine how many service overviews were generated
    total_rows = int(arcpy.GetCount_management(md_path).getOutput(0))
    count_overviews = total_rows - count_rasters
    doTime(a, "Built {} service overview(s) on {}".format(count_overviews, md_path))

    return count_overviews, total_rows



def addLasFilesToMosaicDataset(out_las_dataset, las_folder, las_v_name, las_v_unit, isClassified, md_path, total_rows_without_las):
    a = datetime.now()

    LASSpatialRef = arcpy.Describe(out_las_dataset).SpatialReference
    las_h_name = LASSpatialRef.name
    las_h_unit = LASSpatialRef.linearUnitName
    las_h_code = LASSpatialRef.PCSCode
    try:
        arcpy.AddMessage(
            "Adding LAS files with spatial reference:\n\tH Name '{}'\n\tH Unit '{}'\n\tH Code '{}'\n\tV Name '{}'\n\tV Unit '{}'".format(
                las_h_name, las_h_unit, las_h_code, las_v_name, las_v_unit)
            )
    except UnicodeEncodeError as uer:
        arcpy.AddMessage('Adding Las Files - Encoding Error Has Truncated Text')

    # Get the maximum value of ItemTS From the Project Mosaic Dataset
    #    The value of ItemTS is based on the last time the row was modified. Knowing
    #    the current maximum value of ItemTS in the Project MD will help us determine which rows were
    #    added as a result of the subsequent call to "Add Raster" (i.e. which rows represent LAS)
    fc = r"in_memory/MaxItemTS"
    arcpy.Statistics_analysis(md_path, fc, statistics_fields="ItemTS MAX", case_field="#")
    fields = ['MAX_ITEMTS']
    with arcpy.da.SearchCursor(fc, fields) as cursor:  # @UndefinedVariable
            for row in cursor:
                    MaxItemTSValue = float(row[0])
    # arcpy.AddMessage("Maximum value for ItemTS before adding LAS to Project Mosaic Dataset:             {0}".format(MaxItemTSValue))
    del fc

    # Add the LAS files to the Mosaic Dataset, but don't recalculate cell size ranges, since MaxPS will be
    # set in a subsequent step. Don't update the boundary.
    arcpy.AddRastersToMosaicDataset_management(md_path, LAS_RASTER_TYPE, las_folder, update_cellsize_ranges="NO_CELL_SIZES", update_boundary="NO_BOUNDARY",
                                               update_overviews="NO_OVERVIEWS", maximum_pyramid_levels="#", maximum_cell_size="0", minimum_dimension="1500",
                                               spatial_reference=LASSpatialRef, filter="#", sub_folder="NO_SUBFOLDERS", duplicate_items_action="ALLOW_DUPLICATES",
                                               build_pyramids="NO_PYRAMIDS", calculate_statistics="NO_STATISTICS", build_thumbnails="NO_THUMBNAILS",
                                               operation_description="#", force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE")
    Utility.addToolMessages()

    # If the LAS have Z-units of FOOT_INTL or FOOT_US then append the appropriate function to their
    # function chain to convert the elevation values from feet to meters
    las_v_unit = las_v_unit.upper()
    arcpy.AddMessage("Checking las_v_unit {}".format(las_v_unit))
    if ("FEET" in las_v_unit) or ("FOOT" in las_v_unit) or ("FT" in las_v_unit):
        where_clause = "ItemTS > " + str(MaxItemTSValue) + " AND CATEGORY = 1"
        arcpy.AddMessage("Inserting a function to convert LAS Feet to Meters, since LAS has Z_Unit of:    {}".format(las_v_unit))
        arcpy.AddMessage("\nMosaic Layer where clause: {0}".format(where_clause))
        arcpy.MakeMosaicLayer_management(md_path, "ProjectMDLayer1", where_clause, template="#", band_index="#", mosaic_method="NORTH_WEST", order_field=CMDRConfig.PROJECT_DATE, order_base_value="", lock_rasterid="#", sort_order="ASCENDING", mosaic_operator="LAST", cell_size="1")
        Utility.addToolMessages()

        # Use the applicable function chain for either Foot_US or Foot_Intl
        if ("US" in las_v_unit) or ("SURVEY" in las_v_unit):
            functionChainDef = Raster.Us_ft2mtrs_function_chain_path
        else:
            functionChainDef = Raster.Intl_ft2mtrs_function_chain_path

        arcpy.EditRasterFunction_management("ProjectMDLayer1", edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET_ITEM", edit_options="INSERT", function_chain_definition=functionChainDef, location_function_name="#")
        Utility.addToolMessages()

    # This tool is re-run because sometimes the clip_to_footprints="NOT_CLIP" gets re-set to "CLIP" for some reason
    setMosaicDatasetProperties(md_path)

    Utility.alterField(md_path, "ZMin", "LAS_ZMin", "LAS Z Min")
    Utility.alterField(md_path, "ZMax", "LAS_ZMax", "LAS Z Max")
    
    las_layer = arcpy.MakeMosaicLayer_management(in_mosaic_dataset=md_path, out_mosaic_layer="LAS_MosaicLayer", where_clause="TypeID = 3 and Project_Source IS NULL")
    arcpy.CalculateField_management(in_table=las_layer, field="Project_Source", expression='"LAS"', expression_type="PYTHON_9.3", code_block="")
    Utility.addToolMessages()
    del las_layer
    
    # Get a count to determine how many LAS tiles were added to the MD
    total_rows_with_las = int(arcpy.GetCount_management(md_path).getOutput(0))
    count_las = total_rows_with_las - total_rows_without_las
    doTime(a, "{} las files were added to {}".format(count_las, md_path))

    return count_las, total_rows_with_las


def getDates(dateDeliver, dateStart, dateEnd, project_year):
    if dateDeliver is None:
        if dateEnd is not None:
            dateDeliver = dateEnd
        elif dateStart is not None:
            dateDeliver = dateStart
        elif project_year is not None:
            dateDeliver = "1/1/{}".format(project_year)

    if dateStart is None:
        if dateEnd is not None:
            dateStart = dateEnd
        else:
            dateStart = dateDeliver

    if dateEnd is None:
        if dateStart is not None:
            dateEnd = dateStart
        else:
            dateEnd = "12/31/{}".format(project_year)
    arcpy.AddMessage("Using dates: \n\tdateDeliver: {}\n\tdateStart: {} \n\tdateEnd: {}".format(dateDeliver, dateStart, dateEnd))
    return dateDeliver, dateStart, dateEnd


def createReferenceddMosaicDataset(in_md_path, out_md_path, spatial_ref, raster_v_unit, area_of_interest=None):
    a = datetime.now()
    arcpy.CreateReferencedMosaicDataset_management(in_dataset=in_md_path, out_mosaic_dataset=out_md_path, coordinate_system=spatial_ref, number_of_bands="1", pixel_type="32_BIT_FLOAT", where_clause="TypeID <> 3",
                                                   in_template_dataset=area_of_interest, extent="", select_using_features="SELECT_USING_FEATURES", lod_field="", minPS_field="", maxPS_field="", pixelSize="",
                                                   build_boundary="NO_BOUNDARY")

    raster_function_path = Raster.Height_function_chain_path

    arcpy.EditRasterFunction_management(out_md_path, edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET", edit_options="INSERT", function_chain_definition=raster_function_path, location_function_name="#")
    Utility.addToolMessages()
## 20180508 EI No reason to calc stats here, it has to be done manually
    #arcpy.CalculateStatistics_management(in_raster_dataset=out_md_path, x_skip_factor=SKIP_FACTOR_LRG, y_skip_factor=SKIP_FACTOR_LRG, ignore_values="", skip_existing="OVERWRITE", area_of_interest=area_of_interest)
    # setMosaicDatasetProperties(out_md_path)
    arcpy.AddMessage("\tNOTE: !!! Please edit the raster function !! Replace the DTM with this project's DTM mosaic dataset.\n\n\t{}\n".format(out_md_path))

    try:
        arcpy.SetRasterProperties_management(in_raster=out_md_path, statistics="1 3.0 50.0 8.0 4.25")
    except:
        pass
    doTime(a, "Created Referenced MD '{}'".format(out_md_path))

def createMosaicDatasetAndAddRasters(raster_v_unit, publish_path, filegdb_name, imagePath, md_name, md_path, SpatRefMD, fix, area_of_interest= None):
    a = datetime.now()
    filegdb_path = os.path.join(publish_path, filegdb_name)

    # If the file gdb doesn't exist, then create it
    if not os.path.exists(filegdb_path):
        # A05_C_ConsolidateRasterInfo.deleteFileIfExists(filegdb_path, True)
        arcpy.CreateFileGDB_management(publish_path, filegdb_name)
        Utility.addToolMessages()

    # Create the Mosaic Dataset
    arcpy.CreateMosaicDataset_management(filegdb_path, md_name, coordinate_system=SpatRefMD, num_bands="1", pixel_type="32_BIT_FLOAT", product_definition="NONE", product_band_definitions="#")
    Utility.addToolMessages()
    # set the NoData value to -3.40282346639e+038
    arcpy.SetRasterProperties_management(md_path, data_type="ELEVATION", statistics="", stats_file="#", nodata="1 {}".format(RasterConfig.NODATA_DEFAULT))
    Utility.addToolMessages()
    Raster.addStandardMosaicDatasetFields(md_path)

    arcpy.AddMessage("Adding rasters to mosaic dataset {}".format(md_path))
    arcpy.AddRastersToMosaicDataset_management(md_path, raster_type="Raster Dataset", input_path=imagePath, update_cellsize_ranges="NO_CELL_SIZES",
                                               update_boundary="UPDATE_BOUNDARY", update_overviews="NO_OVERVIEWS", maximum_pyramid_levels="#", maximum_cell_size="0",
                                               minimum_dimension="1500", spatial_reference="#", filter="*.TIFF,*.TIF", sub_folder="NO_SUBFOLDERS",
                                               duplicate_items_action="ALLOW_DUPLICATES", build_pyramids="NO_PYRAMIDS", calculate_statistics="CALCULATE_STATISTICS",
                                               build_thumbnails="NO_THUMBNAILS", operation_description="#", force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE")
    Utility.addToolMessages()
    # Get a record count just to be sure we found raster products to ingest
    count_rasters = int(arcpy.GetCount_management(md_path).getOutput(0))
    arcpy.AddMessage("{} has {} raster dataset(s).".format(md_path, count_rasters))
    # Calculate the values of MinPS and MaxPS with max_range_factor = 3 (for performance)
    arcpy.CalculateCellSizeRanges_management(md_path, where_clause="#", do_compute_min="MIN_CELL_SIZES", do_compute_max="MAX_CELL_SIZES", max_range_factor="3", cell_size_tolerance_factor="0.8", update_missing_only="UPDATE_ALL")
    Utility.addToolMessages()
    setMosaicDatasetProperties(md_path)
    # Determine the cell size of the Mosaic Dataset
    md_cellsize = float(arcpy.GetRasterProperties_management(md_path, property_type="CELLSIZEX", band_index="").getOutput(0))
    arcpy.AddMessage("Cell size of Mosaic Dataset:    {} {}".format(md_cellsize, SpatRefMD.linearUnitName))
    # If the Rasters have Z-units of FOOT_INTL or FOOT_US then append the appropriate function to their
    # function chain to convert the elevation values from feet to meters
    raster_v_unit = raster_v_unit.upper()
    arcpy.AddMessage("Raster vertical unit is {}.".format(raster_v_unit))
    if fix is not None:
        arcpy.AddMessage("Mosaic dataset is in original coordinate system. Leaving vertical units as {}".format(raster_v_unit))
    else:
        if ("FEET" in raster_v_unit) or ("FOOT" in raster_v_unit) or ("FT" in raster_v_unit):
            if ("US" in raster_v_unit) or ("SURVEY" in raster_v_unit):
                arcpy.AddMessage("Raster vertical unit is {}, adding conversion function for US Feet.".format(raster_v_unit))
                arcpy.EditRasterFunction_management(md_path, edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET_ITEM", edit_options="INSERT", function_chain_definition=Raster.Us_ft2mtrs_function_chain_path, location_function_name="#")
                Utility.addToolMessages()
            else:
                arcpy.AddMessage("Raster vertical unit is {}, adding conversion function for International Feet.".format(raster_v_unit))
                arcpy.EditRasterFunction_management(md_path, edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET_ITEM", edit_options="INSERT", function_chain_definition=Raster.Intl_ft2mtrs_function_chain_path, location_function_name="#")
                Utility.addToolMessages()
        
        else:
            arcpy.AddMessage("Raster vertical unit is meters, no need for conversion. {}".format(raster_v_unit))

    doTime(a, "Updated function. Calculating statistics on  {} ".format(md_path))
    arcpy.CalculateStatistics_management(md_path, x_skip_factor=SKIP_FACTOR_LRG, y_skip_factor=SKIP_FACTOR_LRG, ignore_values="#", skip_existing="OVERWRITE", area_of_interest=None)
    Utility.addToolMessages()
    doTime(a, "Calculated statistics on  {} ".format(md_path))

    arcpy.AddMessage("Calculating Project_Source field on {}".format(md_path))
    arcpy.CalculateField_management(in_table=md_path, field="Project_Source", expression='"RAS"', expression_type="PYTHON_9.3", code_block="")
    doTime(a, "Calculated field Project_Source on  {} ".format(md_path))    
    
    # This tool is re-run because sometimes the clip_to_footprints="NOT_CLIP" gets re-set to "CLIP" for some reason
    setMosaicDatasetProperties(md_path)
    
    doTime(a, "Finished adding rasters to {} ".format(md_path))
    return count_rasters, md_cellsize


def updateMosaicDatasetFields(dateDeliver, md_path, footprint_path, md_spatial_ref):
    arcpy.JoinField_management(in_data=md_path, in_field="NameSource", join_table=footprint_path, join_field="NameSource", fields="area;el_type;zran;zmax;zmean;zmin;zdev;width;height;cell_h;cell_w;comp_type;format;pixel;unc_size;xmin;ymin;xmax;ymax;v_name;v_unit;h_name;h_unit;h_wkid;nodata;Project_ID;Project_Dir;Project_GUID;is_class;ra_pt_ct;ra_pt_sp;ra_zmin;ra_zmax;ra_zran")
    # @TODO: Add a start and an end date?
    
    # Calculate the value of certain metadata fields using an update cursor:
    fields = [
              'OBJECTID',  # 0
              'MINPS',  # 1
              'MAXPS',  # 2
              'CATEGORY',  # 3
              'ZORDER',  # 4
        CMDRConfig.PROJECT_DATE,  # 5
        CMDRConfig.PROJECT_SOURCE,  # 6
        CMDRConfig.RASTER_PATH,  # 7
        "v_name",  # 8
        "v_unit",  # 9
        "h_name",  # 10
        "h_unit",  # 11
        "h_wkid"]  # 12
    with arcpy.da.UpdateCursor(md_path, fields) as rows:  # @UndefinedVariable
        for row in rows:
            # all new rows (primary raster, LAS, and overviews) will have ProjectID and ProjectDate
            row[5] = dateDeliver
            # row[6] = PROJECT_SOURCE_LAS
            category = row[3]
            # spatialReference = (arcpy.Describe(row[7])).spatialReference
            # If MinPS is set (i.e. > 0), the row is a primary raster or an overview
            minps = row[1]
            if minps >= 0:
                # Set ZORDER = -1 for these raster items. This will ensure that the rasters are given priority over LAS data, if they are added.
                # This will ensure performance is maintained, in case LAS is also ingested (since LAS data takes longer to render).
                row[4] = -1
                # if category == 1:
                    # row[6] = RasterConfig.PROJECT_SOURCE_RASTER
                if category == 2:
                    # row[6] = RasterConfig.PROJECT_SOURCE_OVERVIEW
                    try:
                        if md_spatial_ref.VCS is not None:
                            row[8] = md_spatial_ref.VCS.datumName
                            row[9] = md_spatial_ref.VCS.linearUnitName
                    except:
                        pass
                    row[10] = md_spatial_ref.name
                    row[11] = md_spatial_ref.linearUnitName
                    row[12] = md_spatial_ref.PCSCode
            else:
                # If MINPS is Null then the row is LAS

                # set LAS MINPS to 0.0
                row[1] = 0.0000
                # Set LAS MAXPS to 0.25 Meter
                row[2] = 0.25000
            rows.updateRow(row)


    del row
    del rows


def importMosaicDatasetGeometries(md_path, footprint_path, boundary_path):
    if footprint_path is not None:
        arcpy.AddMessage("Importing footprints for {} from {}".format(md_path, footprint_path))
        arcpy.ImportMosaicDatasetGeometry_management(md_path, target_featureclass_type="FOOTPRINT", target_join_field="NameSource", input_featureclass=footprint_path, input_join_field="NameSource")
        Utility.addToolMessages()
    if boundary_path is not None:
        
        arcpy.ImportMosaicDatasetGeometry_management(md_path, target_featureclass_type="BOUNDARY", target_join_field="OBJECTID", input_featureclass=boundary_path, input_join_field="OBJECTID")
        Utility.addToolMessages()


def calculateMosaicDatasetStatistics(raster_z_min, raster_z_max, md_path, raster_v_unit = None, area_of_interest = None):
    if raster_v_unit is not None:
        raster_v_unit = str(raster_v_unit).upper()
        #arcpy.AddMessage("Raster vertical unit is {}. Checking to see if it needs to be converted...".format(raster_v_unit))
        if ("FEET" in raster_v_unit) or ("FOOT" in raster_v_unit) or ("FT" in raster_v_unit):
            if ("US" in raster_v_unit) or ("SURVEY" in raster_v_unit):
                arcpy.AddMessage("Raster vertical unit is {}, adding conversion function for US Feet.".format(raster_v_unit))
                raster_z_min = raster_z_min * 1200 / 3937
                raster_z_max = raster_z_max * 1200 / 3937
            else:
                arcpy.AddMessage("Raster vertical unit is {}, adding conversion function for International Feet.".format(raster_v_unit))
                raster_z_min = raster_z_min * 0.3048
                raster_z_max = raster_z_max * 0.3048
        else:
            arcpy.AddMessage("Raster vertical unit is {}. Leaving it as is.".format(raster_v_unit))
    else:
        arcpy.AddMessage("Raster vertical unit is not provided, no need for conversion.")

    full_calc = False
    minResult = arcpy.GetRasterProperties_management(md_path, property_type="MINIMUM", band_index="Band_1")
    Utility.addToolMessages()
    minMDValue = float(minResult.getOutput(0))

    maxResult = arcpy.GetRasterProperties_management(md_path, property_type="MAXIMUM", band_index="Band_1")
    Utility.addToolMessages()
    maxMDValue = float(maxResult.getOutput(0))

    arcpy.AddMessage("Before statistics calc Min/Max \n\tMosaic values ({},{})\n\tRaster values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))

    # Only Calculate Statistics if they are corrupted (The constants can apply to Meters or Feet)
    if minMDValue < -300.0 or maxMDValue > 30000.0:
        arcpy.AddWarning("Mosaic values for Min/Max are way out of spec. Trying large calc statistics \n\tMosaic values ({},{})\n\tRaster values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))
        full_calc = True
        # Calculate stats on the Mosaic Dataset (note: if this takes too long, enlarge skip factors)
        arcpy.AddMessage("CalculateStatistics_management with skip factor {}".format(SKIP_FACTOR_LRG))
        arcpy.CalculateStatistics_management(md_path, x_skip_factor=SKIP_FACTOR_LRG, y_skip_factor=SKIP_FACTOR_LRG, ignore_values=None, skip_existing="OVERWRITE", area_of_interest=None)
        Utility.addToolMessages()

        minResult = arcpy.GetRasterProperties_management(md_path, property_type="MINIMUM", band_index="Band_1")
        Utility.addToolMessages()
        minMDValue = float(minResult.getOutput(0))
        maxResult = arcpy.GetRasterProperties_management(md_path, property_type="MAXIMUM", band_index="Band_1")
        Utility.addToolMessages()
        maxMDValue = float(maxResult.getOutput(0))

    zmin_deviation = abs((minMDValue - raster_z_min) / MOSAIC_Z_TOLERANCE)
    zmax_deviation = abs((maxMDValue - raster_z_max) / MOSAIC_Z_TOLERANCE)
    if raster_z_min <> 0:
        zmin_deviation = abs((minMDValue - raster_z_min) / raster_z_min)
    if raster_z_max <> 0:
        zmax_deviation = abs((maxMDValue - raster_z_max) / raster_z_max)
    if zmin_deviation > 0.1 or zmax_deviation > 0.1:
        if not full_calc:
            full_calc = True
            arcpy.AddWarning("Mosaic values for Min/Max are >10% of rasters. Trying medium calc statistics \n\tMosaic values ({},{})\n\tRaster values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))
            # Calculate statistics and histogram for all rows in the Mosaic Dataset
            #arcpy.AddMessage("BuildPyramidsandStatistics_management")
            #arcpy.BuildPyramidsandStatistics_management(md_path, include_subdirectories="INCLUDE_SUBDIRECTORIES", build_pyramids="NONE", calculate_statistics="CALCULATE_STATISTICS", BUILD_ON_SOURCE="NONE",
            #                                            block_field="#", estimate_statistics="NONE", x_skip_factor=SKIP_FACTOR_MED, y_skip_factor=SKIP_FACTOR_MED, ignore_values="#", pyramid_level="-1",
            #                                            SKIP_FIRST="NONE", resample_technique="BILINEAR", compression_type="DEFAULT", compression_quality="75", skip_existing="SKIP_EXISTING")
            #Utility.addToolMessages()
            arcpy.AddMessage("CalculateStatistics_management with skip factor {}".format(SKIP_FACTOR_MED))
            arcpy.CalculateStatistics_management(md_path, x_skip_factor=SKIP_FACTOR_MED, y_skip_factor=SKIP_FACTOR_MED, ignore_values="#", skip_existing="OVERWRITE", area_of_interest=None)
            Utility.addToolMessages()

            # This tool is re-run because sometimes the clip_to_footprints="NOT_CLIP" gets re-set to "CLIP" for some reason
            setMosaicDatasetProperties(md_path)

            minResult = arcpy.GetRasterProperties_management(md_path, property_type="MINIMUM", band_index="Band_1")
            Utility.addToolMessages()
            minMDValue = float(minResult.getOutput(0))

            maxResult = arcpy.GetRasterProperties_management(md_path, property_type="MAXIMUM", band_index="Band_1")
            Utility.addToolMessages()
            maxMDValue = float(maxResult.getOutput(0))

            arcpy.AddMessage("After statistics calc Min/Max \n\tMosaic values ({},{})\n\tRaster values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))

        else:
            arcpy.AddWarning("Mosaic values for Min/Max are >10% of rasters after full calc. Please manually fix statistics. \n\tMosaic values ({},{})\n\tRaster values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))

    zmin_deviation = abs((minMDValue - raster_z_min) / MOSAIC_Z_TOLERANCE)
    zmax_deviation = abs((maxMDValue - raster_z_max) / MOSAIC_Z_TOLERANCE)
    if raster_z_min <> 0:
        zmin_deviation = abs((minMDValue - raster_z_min) / raster_z_min)
    if raster_z_max <> 0:
        zmax_deviation = abs((maxMDValue - raster_z_max) / raster_z_max)
    if zmin_deviation > 0.1 or zmax_deviation > 0.1:
        
        
        arcpy.AddWarning("Mosaic values for Min/Max are still >10% of rasters. Trying full calc statistics \n\tMosaic values ({},{})\n\tRaster values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))
        # Calculate statistics and histogram for all rows in the Mosaic Dataset
        #arcpy.AddMessage("BuildPyramidsandStatistics_management")
        #arcpy.BuildPyramidsandStatistics_management(md_path, include_subdirectories="INCLUDE_SUBDIRECTORIES", build_pyramids="NONE", calculate_statistics="CALCULATE_STATISTICS", BUILD_ON_SOURCE="NONE",
        #                                            block_field="#", estimate_statistics="NONE", x_skip_factor=SKIP_FACTOR_MED, y_skip_factor=SKIP_FACTOR_MED, ignore_values="#", pyramid_level="-1",
        #                                            SKIP_FIRST="NONE", resample_technique="BILINEAR", compression_type="DEFAULT", compression_quality="75", skip_existing="SKIP_EXISTING")
        #Utility.addToolMessages()
        arcpy.AddMessage("CalculateStatistics_management with skip factor {}".format(SKIP_FACTOR_SML))
        arcpy.CalculateStatistics_management(md_path, x_skip_factor=SKIP_FACTOR_SML, y_skip_factor=SKIP_FACTOR_SML, ignore_values="#", skip_existing="OVERWRITE", area_of_interest=None)
        Utility.addToolMessages()

        # This tool is re-run because sometimes the clip_to_footprints="NOT_CLIP" gets re-set to "CLIP" for some reason
        setMosaicDatasetProperties(md_path)

        minResult = arcpy.GetRasterProperties_management(md_path, property_type="MINIMUM", band_index="Band_1")
        Utility.addToolMessages()
        minMDValue = float(minResult.getOutput(0))

        maxResult = arcpy.GetRasterProperties_management(md_path, property_type="MAXIMUM", band_index="Band_1")
        Utility.addToolMessages()
        maxMDValue = float(maxResult.getOutput(0))

        arcpy.AddMessage("After full statistics calc Min/Max \n\tMosaic values ({},{})\n\tRaster values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))



    zmin_deviation = abs((minMDValue - raster_z_min) / MOSAIC_Z_TOLERANCE)
    zmax_deviation = abs((maxMDValue - raster_z_max) / MOSAIC_Z_TOLERANCE)
    if raster_z_min <> 0:
        zmin_deviation = abs((minMDValue - raster_z_min) / raster_z_min)
    if raster_z_max <> 0:
        zmax_deviation = abs((maxMDValue - raster_z_max) / raster_z_max)
    if zmin_deviation > 0.1 or zmax_deviation > 0.1:
        arcpy.AddWarning("Min/Max MD values ({},{}) still don't match expected values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))
    else:
        arcpy.AddMessage("Current Min/Max is within tolerance:\n\tMosaic values ({},{})\n\tRaster values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))

def createHeightRefMD(imageDir, publish_folder, md_paths, SpatRefMD, raster_v_unit, area_of_interest=None):
    # ## DCM is the height above last return (e.g. canopy height)
    # ## DHM is the height above ground

    filegdb_name, filegdb_ext = os.path.splitext(publish_folder.fgdb_name)  # @UnusedVariable
    filegdb_name = "{}_{}.gdb".format(filegdb_name, imageDir)
#     target_path = os.path.join(publish_folder.path, imageDir)
    filegdb_path = os.path.join(publish_folder.path, filegdb_name)
    md_name = imageDir
    dhm_md_path = os.path.join(filegdb_path, md_name)

    if not os.path.exists(filegdb_path):
        arcpy.CreateFileGDB_management(publish_folder.path, filegdb_name)

    if arcpy.Exists(dhm_md_path):
        arcpy.AddMessage("Height Model already exists. {}".format(dhm_md_path))
    else:
        createReferenceddMosaicDataset(md_paths[DSM], dhm_md_path, SpatRefMD, raster_v_unit, area_of_interest=area_of_interest)

def getOriginalCoordinateSystem(publish_folder):
    ocs_spatial_ref = None
    dtm_folder = publish_folder.demLastTiff_path
    arcpy.AddMessage("getting spatial reference from DTM: {}".format(dtm_folder))
    first_raster = None
    for dtm_file in os.listdir(dtm_folder):
        arcpy.AddMessage("\tgetting spatial reference from DTM: {}".format(dtm_file))
        if str(dtm_file).upper().endswith(".TIF"):
            arcpy.AddMessage("\t\tgetting spatial reference from DTM: {}".format(dtm_file))
            first_raster = dtm_file
            break

    if first_raster is not None:
        first_raster = os.path.join(dtm_folder, first_raster)
        arcpy.AddMessage("getting spatial reference from DTM: {}".format(first_raster))
        ocs_spatial_ref = arcpy.Describe(first_raster).spatialReference

    arcpy.AddMessage("Found spatial reference from DTM: {}".format(ocs_spatial_ref))
    if ocs_spatial_ref is not None:
        try:
            arcpy.AddMessage("Found spatial reference from DTM: {}".format(ocs_spatial_ref.exportToString()))
        except:
            pass
    return ocs_spatial_ref

'''
retrieve the boundary for the specified elevation type (DTM, DSM, etc)
'''
def getBoundary(fgdb_path, el_type):
    a = datetime.now()
    
    raster_boundary = A05_C_ConsolidateRasterInfo.getRasterBoundaryPath(fgdb_path, el_type)
    
    raster_boundary_md = "{}_SIM".format(raster_boundary)
    
    if not arcpy.Exists(raster_boundary_md):
        arcpy.SimplifyPolygon_cartography(in_features=raster_boundary, out_feature_class=raster_boundary_md, algorithm="POINT_REMOVE", tolerance=Raster.boundary_interval, minimum_area="0 SquareMeters", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP")
        Utility.addToolMessages()
        Utility.deleteFields(raster_boundary_md)
        
        arcpy.RepairGeometry_management(in_features=raster_boundary_md, delete_null="DELETE_NULL")
        Utility.addToolMessages()
        Utility.deleteFields(raster_boundary_md)
        
        a = doTime(a, "\tSimplified boundary {}".format(raster_boundary_md))
    else:
        a = doTime(a, "\tFound existing boundary {}".format(raster_boundary_md))
    
    return raster_boundary_md

def processJob(project_job, project, ProjectUID, dateDeliver, dateStart, dateEnd):
    a = datetime.now()
    aa = a

    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(project_job, project)
#     ProjectID = ProjectJob.getProjectID(project)
    
    project_year = project_job.getYear(project)
    project_id = project_job.getProjectID(project)
    las_qainfo = A04_A_GenerateQALasDataset.getLasQAInfo(ProjectFolder)

    publish_folder = ProjectFolder.published
    derived_fgdb_path = ProjectFolder.derived.fgdb_path
    arcpy.Compact_management(derived_fgdb_path)
    Utility.addToolMessages()

    lasd_path = ProjectFolder.derived.lasd_path
    lasd_boundary_path = A04_C_ConsolidateLASInfo.getLasdBoundaryPath(derived_fgdb_path)
    las_footprint_path = A04_C_ConsolidateLASInfo.getLasFootprintPath(derived_fgdb_path)
    
    las_footprint_path_sim = "{}_SIM".format(las_footprint_path)
    if not arcpy.Exists(las_footprint_path_sim):
        arcpy.SimplifyPolygon_cartography(in_features=las_footprint_path, out_feature_class=las_footprint_path_sim,
                                                algorithm="POINT_REMOVE", tolerance=Raster.boundary_interval, minimum_area="0 SquareMeters",
                                                error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP")
        Utility.addToolMessages()
        Utility.deleteFields(las_footprint_path_sim)
        
        arcpy.RepairGeometry_management(in_features=las_footprint_path_sim, delete_null="DELETE_NULL")
        Utility.addToolMessages()
        Utility.deleteFields(las_footprint_path_sim)
        
        Utility.addAndCalcFieldText(dataset_path=las_footprint_path_sim, field_name="el_type", field_length=20, field_value='"LAS"', field_alias="Elevation Type", add_index=True)
        addNameSource(las_footprint_path_sim, "LAS")
        Utility.deleteFields(las_footprint_path_sim)
        
        a = doTime(a, "\tSimplified LAS footprint geometry {}".format(las_footprint_path_sim))
    else:
        a = doTime(a, "\tUsing simplified LAS footprint geometry {}".format(las_footprint_path_sim))
    
    las_z_min, las_z_max, las_v_name, las_v_unit, las_h_name, las_h_unit, las_h_wkid, isClassified = A05_A_RemoveDEMErrantValues.getLasdBoundData(lasd_boundary_path)  # @UnusedVariable
    
    SpatRefMD = arcpy.SpatialReference()
    SpatRefMD.loadFromString(RasterConfig.SpatRef_WebMercator)

    SpatRefOCS = getOriginalCoordinateSystem(publish_folder)

    dateDeliver, dateStart, dateEnd = getDates(dateDeliver, dateStart, dateEnd, project_year)


    ImageDirectories = [[DTM, SpatRefOCS, OCS], [DTM, SpatRefMD, None], [DSM, SpatRefMD, None], [DLM, SpatRefMD, None], [INT, SpatRefMD, None]]
    md_paths = {}
    for el_type, SpatRef, fix  in ImageDirectories:
        if SpatRef is not None:
            raster_boundary = getBoundary(derived_fgdb_path, el_type)
            
            raster_z_min, raster_z_max, raster_v_name, raster_v_unit, raster_h_name, raster_h_unit, raster_h_wkid = A05_A_RemoveDEMErrantValues.getRasterBoundData(raster_boundary, el_type, False)  # @UnusedVariable
            
            pub_filegdb_name, filegdb_ext = os.path.splitext(publish_folder.fgdb_name)  # @UnusedVariable
            pub_filegdb_name = "{}_{}.gdb".format(pub_filegdb_name, el_type)
            pub_filegdb_path = os.path.join(publish_folder.path, pub_filegdb_name)
            
            raster_target_path = os.path.join(publish_folder.path, el_type)
            
            md_name = el_type
            if fix is not None:
                # Add the OCS (original coordinate system) postfix if it is requested
                md_name = "{}{}".format(el_type, fix)
            md_path = os.path.join(pub_filegdb_path, md_name)

            
            if arcpy.Exists(md_path):
                md_paths[md_name] = md_path
                arcpy.AddMessage("Mosaic exists: {}".format(md_path))
            else:
                SRMatchFlag, ras_count = isSpatialRefSameForAll(raster_target_path)
                if not SRMatchFlag:
                    arcpy.AddError("SR doesn't match for all {} images, aborting.".format(el_type))
                    raise Exception("SR doesn't match for all {} images, aborting.".format(el_type))
                    
                elif ras_count <= 0:
                    arcpy.AddError("No rasters for selected elevation type {}.".format(el_type))
                else:
                    arcpy.AddMessage("Working on {} rasters for elevation type {} vertical unit {}.".format(ras_count, el_type, raster_v_unit))

                    count_rasters, md_cellsize = createMosaicDatasetAndAddRasters(raster_v_unit, publish_folder.path, pub_filegdb_name, raster_target_path, md_name, md_path, SpatRef, fix, area_of_interest=lasd_boundary_path)
                    count_total = count_rasters 
                    if count_total > 0:
                        md_paths[md_name] = md_path
                        arcpy.Compact_management(in_workspace=os.path.dirname(md_path))
                        Utility.addToolMessages()

                    if fix is None:
                        calculateMosaicDatasetStatistics(raster_z_min, raster_z_max, md_path, raster_v_unit, area_of_interest=lasd_boundary_path)
                    else:
                        calculateMosaicDatasetStatistics(raster_z_min, raster_z_max, md_path, area_of_interest=lasd_boundary_path)
                    
                    # Import the boundary here for stats calcs
                    importMosaicDatasetGeometries(md_path, None, raster_boundary)

                    count_overviews = 0
                    count_las = 0
                    if fix is None:
                        count_overviews, count_total = generateOverviews(raster_target_path, md_name, md_path, count_rasters, SpatRef, md_cellsize, lasd_boundary_path)
                        count_las, count_total = addLasFilesToMosaicDataset(lasd_path, las_qainfo.las_directory, las_v_name, las_v_unit, isClassified, md_path, count_total)
                    
                    raster_footprint_path = None
                    if fix is None:
                        raster_footprint_path = mergeFootprints(las_footprint_path_sim, el_type, derived_fgdb_path)
                    else:
                        raster_footprint_path = mergeFootprints(None, el_type, derived_fgdb_path)
                    arcpy.AddMessage("Using footprints from {}".format(raster_footprint_path))
                    
                    all_layer = arcpy.MakeMosaicLayer_management(in_mosaic_dataset=md_path, out_mosaic_layer="AllMosaicLayer", where_clause="1=1")
                    Utility.addAndCalcFieldText(md_path, "NameSource", 100, field_value='!Name! + "_" + !Project_Source!', field_alias="NameSource", code_block="", add_index=True, debug=True)
                    del all_layer
                    
                    importMosaicDatasetGeometries(md_path, raster_footprint_path, raster_boundary)
                    arcpy.Compact_management(in_workspace=os.path.dirname(md_path))
                    Utility.addToolMessages()
                    updateMosaicDatasetFields(dateDeliver, md_path, raster_footprint_path, SpatRef)
                    
                    arcpy.CalculateField_management(in_table=md_path, field="Project_ID", expression='"{}"'.format(project_id), expression_type="PYTHON_9.3", code_block="")

                    # Analyze the Mosaic Dataset in preparation for publishing it
                    arcpy.AddMessage("Analyzing mosaic dataset {}".format(md_path))
                    arcpy.AnalyzeMosaicDataset_management(md_path, where_clause="#", checker_keywords="FOOTPRINT;FUNCTION;RASTER;PATHS;SOURCE_VALIDITY;STALE;PYRAMIDS;STATISTICS;PERFORMANCE;INFORMATION")
                    Utility.addToolMessages()
                    arcpy.AddMessage("To view detailed results, Add the MD to the map, rt-click --> Data --> View Analysis Results")

                    Utility.deleteFileIfExists(raster_footprint_path, True)
                    Utility.deleteFileIfExists(raster_boundary, True)
                    
                    arcpy.AddMessage("Mosaic dataset has {} rasters {} overviews and {} las files.".format(count_rasters, count_overviews, count_las))
                    doTime(a, "completed building mosaic dataset {}".format(md_path))

    if md_paths[DSM] is None:
        arcpy.AddWarning("DSM doesnt exist, height models cant be created.")
    else:
        # ## DHM is the height above ground
        createHeightRefMD(DHM, publish_folder, md_paths, SpatRefMD, raster_v_unit, area_of_interest=lasd_boundary_path)
        # ## DCM is the height above last return (e.g. canopy height)
        createHeightRefMD(DCM, publish_folder, md_paths, SpatRefMD, raster_v_unit, area_of_interest=lasd_boundary_path)


    doTime(aa, "A06 A Operation complete")


def CreateProjectMDs(strJobId, dateDeliver=None, dateStart=None, dateEnd=None):
    aa = datetime.now()
    Utility.printArguments(["WMX Job ID", "dateDeliver", "dateStart", "dateEnd"], [strJobId, dateDeliver, dateStart, dateEnd], "A06 CreateProjectMDs")
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")

    project_job, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable

    processJob(project_job, project, strUID, dateDeliver, dateStart, dateEnd)

    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")

    doTime(aa, "Operation Complete: A06 Create Project MDs")

if __name__ == '__main__':

    dateStart, dateEnd = None, None
    if len(sys.argv) > 1:
        strJobId = sys.argv[1]
        dateDeliver = sys.argv[2]

        if len(sys.argv) >= 4:
            dateStart = sys.argv[3]
        if len(sys.argv) >= 5:
            dateEnd = sys.argv[4]

        CreateProjectMDs(strJobId, dateDeliver, dateStart, dateEnd)
    else:
        # DEBUG

        dateDeliver = "04/09/1971"
        strUID = None  # field_ProjectJob_UID
        wmx_job_id = 1
        project_Id = "OK_SugarCreek_2008"
        alias = "Sugar Creek"
        alias_clean = "SugarCreek"
        state = "OK"
        year = 2008
        parent_dir = r"E:\NGCE\RasterDatasets"
        archive_dir = r"E:\NGCE\RasterDatasets"
        project_dir = r"E:\NGCE\RasterDatasets\OK_SugarCreek_2008"
        project_AOI = None
        project_job = CMDR.ProjectJob()
        project = [
            strUID,  # field_ProjectJob_UID
            wmx_job_id,  # field_ProjectJob_WMXJobID,
            project_Id,  # field_ProjectJob_ProjID,
            alias,  # field_ProjectJob_Alias
            alias_clean,  # field_ProjectJob_AliasClean
            state ,  # field_ProjectJob_State
            year ,  # field_ProjectJob_Year
            parent_dir,  # field_ProjectJob_ParentDir
            archive_dir,  # field_ProjectJob_ArchDir
            project_dir,  # field_ProjectJob_ProjDir
            project_AOI  # field_ProjectJob_SHAPE
        ]

        processJob(project_job, project, strUID, dateDeliver, dateStart, dateEnd)

