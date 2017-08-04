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
import os

from ngce import Utility
from ngce.cmdr import CMDR, CMDRConfig
from ngce.cmdr.CMDRConfig import DSM, DTM
from ngce.folders import ProjectFolders
from ngce.las import LAS, LASConfig
from ngce.raster import Raster, RasterConfig


Utility.setArcpyEnv(True)


# Set the processing factor if Parallel processing is overwhelming Service
#    Overview Creation (i.e. Category is > 2 for any MD rows)
# arcpy.env.parallelProcessingFactor = "0"

LAS_RASTER_TYPE_name = LASConfig.LAS_RASTER_TYPE_1_All_Bin_Mean_IDW

ProjectID = None

vendor_raster_path = None
raster_source = None
ProjectDate = None
raster_Z_Unit = None



def isSpatialRefSameForAll(InputFolder):
    arcpy.env.workspace = InputFolder
    rasters = arcpy.ListRasters("*", "TIF")
    count = len(rasters)
    
    SpatRefFirstRaster = None
    SRMatchFlag = True
    
    
    for raster in rasters:
        describe = arcpy.Describe(raster)
        SRef = describe.SpatialReference.exportToString()
        if SpatRefFirstRaster is None:
            SpatRefFirstRaster = SRef
        if SRef != SpatRefFirstRaster:
            SRMatchFlag = False
            arcpy.AddError("Raster has a PCSCode (EPSG code) that is different than first raster: {}".format(raster))
    
    return SRMatchFlag, count


def CreateProjectMosaicDataset(jobID=16402, publish=True):
    Utility.printArguments(["WMX Job ID"], [jobID], "A06 Create Project Mosaic Dataset")
    
    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
    
    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)  # @UnusedVariable
    ProjectYear = ProjectJob.getYear(project)
    if project is not None:
        ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
        TargetFolder = ProjectFolder.derived
        if publish:
            TargetFolder = ProjectFolder.published
                     
        ProjectID = ProjectJob.getProjectID(project)
    
        Deliver = CMDR.Deliver()
        delivery = list(Deliver.getDeliver(ProjectID))
        
        # Note: the default minimum of 1 was excluding some shoreline, since elevation is < 1 at the shoreline
        minZ = Deliver.getValidZMin(delivery)
        maxZ = Deliver.getValidZMax(delivery)
        minZ, maxZ = LAS.validateZRange(minZ, maxZ)
        rangeZ = (maxZ - minZ)
        rangeZ10p = 0.1 * rangeZ
        isClassified = (Deliver.getIsClassified(delivery) == "Yes")
        PCSCodeZeroFlag = 0
        
        ProjectDate = Deliver.getValidDate(delivery)
        if ProjectDate is None:
            ProjectDate = "6/1/{}".format(ProjectYear)
        if ProjectDate is not None:
            raster_Z_Unit = Deliver.getVertUnit(delivery)
            if raster_Z_Unit is not None:
                
                ImageDirectories = [DSM, DTM]
                for imageDir in ImageDirectories:
                    filegdb_name = "{}_{}.gdb".format(TargetFolder.fgdb_name, imageDir)
                    if TargetFolder.fgdb_name.endswith(".gdb"):
                        filegdb_name = "{}_{}.gdb".format(TargetFolder.fgdb_name[:-4], imageDir)
                    filegdb_path = os.path.join(TargetFolder.path, filegdb_name)               
                    
                    # If the file gdb doesn't exist, then create it
                    if not os.path.exists(filegdb_path):
                        arcpy.CreateFileGDB_management(TargetFolder.path, filegdb_name)
                        Utility.addToolMessages()
                    
                    imagePath = os.path.join(TargetFolder.path, imageDir)
                    SRMatchFlag, count = isSpatialRefSameForAll(imagePath)
                    if count > 0:
                        if SRMatchFlag:
                            
                            md_name = imageDir
                            md_path = os.path.join(filegdb_path, md_name)
                             
                            if arcpy.Exists(md_path):
                                arcpy.Delete_management(md_path)
                                Utility.addToolMessages()
                            if not arcpy.Exists(md_path):
                   
                                # Create the Mosaic Dataset
                                arcpy.CreateMosaicDataset_management(filegdb_path,
                                                         md_name,
                                                         coordinate_system=RasterConfig.SpatRef_WebMercator,
                                                         num_bands="1",
                                                         pixel_type="32_BIT_FLOAT",
                                                         product_definition="NONE",
                                                         product_band_definitions="#")
                                Utility.addToolMessages()
                    
                
                                # set the NoData value to -3.40282346639e+038
                                arcpy.SetRasterProperties_management(md_path, data_type="ELEVATION", statistics="", stats_file="#", nodata="1 {}".format(RasterConfig.NODATA_DEFAULT))
                                Utility.addToolMessages()
                    
                                Raster.addStandardMosaicDatasetFields(md_path)
#                                 # Add the required metadata fields to the Mosaic Dataset
#                                 arcpy.AddField_management(md_path, field_name=CMDRConfig.PROJECT_ID, field_type="TEXT", field_precision="#", field_scale="#", field_length="100", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                                 Utility.addToolMessages()
#                                 arcpy.AddField_management(md_path, field_name="ProjectDate", field_type="DATE", field_precision="#", field_scale="#", field_length="#", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                                 Utility.addToolMessages()
#                                 arcpy.AddField_management(md_path, field_name="RasterPath", field_type="TEXT", field_precision="#", field_scale="#", field_length="512", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                                 Utility.addToolMessages()
#                                 arcpy.AddField_management(md_path, field_name="ProjectSrs", field_type="TEXT", field_precision="#", field_scale="#", field_length="100", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                                 Utility.addToolMessages()
#                                 arcpy.AddField_management(md_path, field_name="ProjectSrsUnits", field_type="TEXT", field_precision="#", field_scale="#", field_length="20", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                                 Utility.addToolMessages()
#                                 arcpy.AddField_management(md_path, field_name="ProjectSrsUnitsZ", field_type="TEXT", field_precision="#", field_scale="#", field_length="20", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                                 Utility.addToolMessages()
#                                 arcpy.AddField_management(md_path, field_name="ProjectSource", field_type="TEXT", field_precision="#", field_scale="#", field_length="20", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                                 Utility.addToolMessages()
#                                 arcpy.AddField_management(md_path, field_name="PCSCode", field_type="TEXT", field_precision="#", field_scale="#", field_length="20", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                                 Utility.addToolMessages()
#                             
#                                 # Create indexes on all metadata fields to facilitate query
#                                 arcpy.AddIndex_management(md_path, fields=CMDRConfig.PROJECT_ID, index_name=CMDRConfig.PROJECT_ID, unique="NON_UNIQUE", ascending="ASCENDING")
#                                 Utility.addToolMessages()
#                                 arcpy.AddIndex_management(md_path, fields="ProjectDate", index_name="ProjectDate", unique="NON_UNIQUE", ascending="ASCENDING")
#                                 Utility.addToolMessages()
#                                 arcpy.AddIndex_management(md_path, fields="ProjectSrs", index_name="ProjectSrs", unique="NON_UNIQUE", ascending="ASCENDING")
#                                 Utility.addToolMessages()
#                                 arcpy.AddIndex_management(md_path, fields="ProjectSrsUnits", index_name="ProjectSrsUnits", unique="NON_UNIQUE", ascending="ASCENDING")
#                                 Utility.addToolMessages()
#                                 arcpy.AddIndex_management(md_path, fields="ProjectSrsUnitsZ", index_name="ProjectSrsUnitsZ", unique="NON_UNIQUE", ascending="ASCENDING")
#                                 Utility.addToolMessages()
#                                 arcpy.AddIndex_management(md_path, fields="ProjectSource", index_name="ProjectSource", unique="NON_UNIQUE", ascending="ASCENDING")
#                                 Utility.addToolMessages()
#                                 arcpy.AddIndex_management(md_path, fields="PCSCode", index_name="PCSCode", unique="NON_UNIQUE", ascending="ASCENDING")
#                                 Utility.addToolMessages()
#                                 arcpy.EnableEditorTracking_management(in_dataset=md_path, creator_field="created_user", creation_date_field="created_date", last_editor_field="last_edited_user", last_edit_date_field="last_edited_date", add_fields="ADD_FIELDS", record_dates_in="UTC")
#                                 Utility.addToolMessages()
                    
                                # Add the rasters in vendor_raster_path to the Mosaic Dataset.
                                # Subfolders are not checked. 
                                #     sub_folder="NO_SUBFOLDERS"
                                # Cell size ranges are not calculated, as this will be done in a subsequent step
                                #     update_cellsize_ranges="NO_CELL_SIZES"
                                # Pyramids are NOT created for each raster dataset (hopefully they don't already exist)
                                #     build_pyramids="NO_PYRAMIDS"
                                # Statistics are created for each raster dataset (if they don't already exist)
                                #     calculate_statistics="CALCULATE_STATISTICS"
                                # Only TIFF files are ingested 
                                #     filter="*.TIF"
                                # TODO: Can we include sub folders?
                                arcpy.AddRastersToMosaicDataset_management(md_path, raster_type="Raster Dataset", input_path=imagePath,
                                                                         update_cellsize_ranges="NO_CELL_SIZES", update_boundary="UPDATE_BOUNDARY",
                                                                         update_overviews="NO_OVERVIEWS", maximum_pyramid_levels="#", maximum_cell_size="0",
                                                                         minimum_dimension="1500", spatial_reference="#", filter="*.TIFF,*.TIF", sub_folder="NO_SUBFOLDERS",
                                                                         duplicate_items_action="ALLOW_DUPLICATES", build_pyramids="NO_PYRAMIDS",
                                                                         calculate_statistics="CALCULATE_STATISTICS", build_thumbnails="NO_THUMBNAILS",
                                                                         operation_description="#", force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE")
                                Utility.addToolMessages()
                                
                    
                                # Get a record count just to be sure we found raster products to ingest
                                result = arcpy.GetCount_management(md_path)
                                Utility.addToolMessages()
                                countRasters = int(result.getOutput(0))
                                if countRasters > 0:
                                    arcpy.AddMessage("{} has {} raster dataset(s).".format(md_path, countRasters))
                
                                    # Calculate the values of MinPS and MaxPS with max_range_factor = 3 (for performance)
                                    arcpy.CalculateCellSizeRanges_management(md_path, where_clause="#", do_compute_min="MIN_CELL_SIZES",
                                                                                                                     do_compute_max="MAX_CELL_SIZES", max_range_factor="3", cell_size_tolerance_factor="0.8",
                                                                                                                     update_missing_only="UPDATE_ALL")
                                    Utility.addToolMessages()
                
                                    # Build footprints with up to 3000 vertices (to eliminate possible slivers)
                                    # These footprints will be simplified
                                    # Note: minimum elevation value is set lower than the default value (1) to get at accurate value at the coast
                                    # EI added 10% of range (10% below min and 10% above max)
                                    arcpy.BuildFootprints_management(md_path, where_clause="#", reset_footprint="RADIOMETRY", min_data_value=(minZ - rangeZ10p), max_data_value=(maxZ + rangeZ10p),
                                                                                                     approx_num_vertices="3000", shrink_distance="0", maintain_edges="NO_MAINTAIN_EDGES",
                                                                                                     skip_derived_images="SKIP_DERIVED_IMAGES", update_boundary="NO_BOUNDARY", request_size="2000",
                                                                                                     min_region_size="100", simplification_method="NONE", edge_tolerance="#", max_sliver_size="20", min_thinness_ratio="0.05")
                                    Utility.addToolMessages()
                
                                    # Set the desired MD properties (non-default parameters are listed below):
                                    #     default_compression_type="LERC"
                                    #     limited the transmission_fields
                                    #     start_time_field="ProjectDate" (in case we decide to enable time later)
                                    #     max_num_of_records_returned="2000" (default is 1000)
                                    #     max_num_of_download_items="40" (default is 20)
                                    #     data_source_type="ELEVATION"
                                    #     rows_maximum_imagesize="25000"
                                    #     columns_maximum_imagesize="25000"
                                    #     Metadata_level = "BASIC"
                                    transmissionFields = CMDRConfig.TRANSMISSION_FIELDS  # "Name;LowPS;CenterX;CenterY;ProjectID;ProjectDate;ProjectSrs;ProjectSrsUnits;ProjectSrsUnitsZ;ProjectSource;PCSCode"
                                    arcpy.AddMessage("Setting transmissionFields: {}".format(transmissionFields))
                                    
                                    arcpy.SetMosaicDatasetProperties_management(md_path, rows_maximum_imagesize="25000", columns_maximum_imagesize="25000",
                                                                                allowed_compressions="None;LZ77;JPEG;LERC", default_compression_type="LERC", JPEG_quality="75",
                                                                                LERC_Tolerance="0.001", resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP",
                                                                                footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA", clip_to_boundary="NOT_CLIP",
                                                                                color_correction="NOT_APPLY", allowed_mensuration_capabilities="#",
                                                                                default_mensuration_capabilities="NONE",
                                                                                allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None",
                                                                                default_mosaic_method="NorthWest", order_field=CMDRConfig.PROJECT_DATE, order_base="#",
                                                                                sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width="0", view_point_x="600",
                                                                                view_point_y="300", max_num_per_mosaic="20", cell_size_tolerance="0.8", cell_size="#",
                                                                                metadata_level="BASIC",
                                                                                transmission_fields=transmissionFields,
                                                                                use_time="DISABLED", start_time_field=CMDRConfig.PROJECT_DATE, end_time_field="#", time_format="#",
                                                                                geographic_transform="#", max_num_of_download_items="40", max_num_of_records_returned="2000",
                                                                                data_source_type="ELEVATION", minimum_pixel_contribution="1", processing_templates="None",
                                                                                default_processing_template="None")
                                    Utility.addToolMessages()
                                
                                    # Export, simplify, and Import Footprints
                                    arcpy.MakeMosaicLayer_management(md_path, "ProjectMDLayer2", where_clause="", template="#", band_index="#",
                                                                     mosaic_method="NORTH_WEST", order_field=CMDRConfig.PROJECT_DATE, order_base_value="",
                                                                     lock_rasterid="#", sort_order="ASCENDING", mosaic_operator="LAST", cell_size="1")
                                    Utility.addToolMessages()
                                    
                                    arcpy.AddMessage("Removing unnecessary vertices from Footprints")
    
                                    footprint_simple = os.path.join(filegdb_path, "FOOTPRINT_{}".format(imageDir))
                                    arcpy.Delete_management(footprint_simple)
                                    Utility.addToolMessages()
                                    
                                    arcpy.SimplifyPolygon_cartography(in_features=r"ProjectMDLayer2/Footprint", out_feature_class=footprint_simple,
                                                                    algorithm="POINT_REMOVE", tolerance=Raster.boundary_interval, minimum_area="0 SquareMeters",
                                                                    error_option="RESOLVE_ERRORS", collapsed_point_option="KEEP_COLLAPSED_POINTS")
                                    Utility.addToolMessages()
                
                                    # import simplified Footprints
                                    arcpy.ImportMosaicDatasetGeometry_management(md_path, target_featureclass_type="FOOTPRINT", target_join_field="OBJECTID",
                                                                                 input_featureclass=footprint_simple, input_join_field="OBJECTID")
                                    Utility.addToolMessages()
                                    
                                    # The spatial reference of the Mosaic Dataset 
                                    descMD = arcpy.Describe(md_path)
                                    SpatRefMD = descMD.SpatialReference
                
                                    # Determine the cell size of the Mosaic Dataset
                                    cellsizeResult = arcpy.GetRasterProperties_management(md_path, property_type="CELLSIZEX", band_index="")
                                    Utility.addToolMessages()
                                    cellsize = float(cellsizeResult.getOutput(0))
                                    arcpy.AddMessage("Cell size of MD:    {0} {1}".format(cellsize, SpatRefMD.linearUnitName))
                                    
                                    # Set the cell size of the first level overview according to the cell size of the Mosaic Dataset
                                    # Do this by doubling cell size and finding the next ArcGIS Online cache scale            
                                    cellsizeOVR = Raster.getOverviewCellSize(cellsize)
                                    arcpy.AddMessage("Cell size of First level Overview:    {0} {1}".format(cellsizeOVR, SpatRefMD.linearUnitName))
                
                                    # If the Rasters have Z-units of FOOT_INTL or FOOT_US then append the appropriate function to their
                                    # function chain to convert the elevation values from feet to meters 
                                    if raster_Z_Unit.upper() == "International Feet".upper():  # and "METER" in SpatRefMD.linearUnitName.upper():        
                                        arcpy.EditRasterFunction_management(md_path, edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET_ITEM", edit_options="INSERT",
                                                                            function_chain_definition=Raster.Intl_ft2mtrs_function_chain_path,
                                                                            location_function_name="#")
                                        Utility.addToolMessages()
                                    elif raster_Z_Unit.upper() == "Survey Feet".upper():  # and "METER" in SpatRefMD.linearUnitName.upper():        
                                        arcpy.EditRasterFunction_management(md_path, edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET_ITEM", edit_options="INSERT",
                                                                            function_chain_definition=Raster.Us_ft2mtrs_function_chain_path,
                                                                            location_function_name="#")
                                        Utility.addToolMessages()
                                    else:
                                        arcpy.AddMessage("Raster vertical unit is meters, no need for conversion.")
                                
                                    arcpy.CalculateStatistics_management(md_path, x_skip_factor="1", y_skip_factor="1", ignore_values="#", skip_existing="OVERWRITE",
                                                                         area_of_interest="Feature Set")
                                    Utility.addToolMessages()
                    
                                    # This tool is re-run because sometimes the clip_to_footprints="NOT_CLIP" gets re-set to "CLIP" for some reason
                                    arcpy.SetMosaicDatasetProperties_management(md_path, rows_maximum_imagesize="25000", columns_maximum_imagesize="25000",
                                                                                allowed_compressions="None;LZ77;JPEG;LERC", default_compression_type="LERC", JPEG_quality="75",
                                                                                LERC_Tolerance="0.001", resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP",
                                                                                footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA", clip_to_boundary="NOT_CLIP",
                                                                                color_correction="NOT_APPLY", allowed_mensuration_capabilities="#",
                                                                                default_mensuration_capabilities="NONE",
                                                                                allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None",
                                                                                default_mosaic_method="NorthWest", order_field=CMDRConfig.PROJECT_DATE, order_base="#",
                                                                                sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width="0", view_point_x="600",
                                                                                view_point_y="300", max_num_per_mosaic="20", cell_size_tolerance="0.8", cell_size="#",
                                                                                metadata_level="BASIC",
                                                                                transmission_fields=transmissionFields,
                                                                                use_time="DISABLED", start_time_field=CMDRConfig.PROJECT_DATE, end_time_field="#", time_format="#",
                                                                                geographic_transform="#", max_num_of_download_items="40", max_num_of_records_returned="2000",
                                                                                data_source_type="ELEVATION", minimum_pixel_contribution="1", processing_templates="None",
                                                                                default_processing_template="None")
                                    Utility.addToolMessages()
                
                                    # Location of Mosaic Dataset overview TIFF files (Note: this folder needs to be in the ArcGIS Server Data Store)
                                    mosaic_dataset_overview_path = os.path.join(TargetFolder.path, "{}.Overviews".format(md_name))
                                    arcpy.AddMessage("Mosaic Dataset Overview Folder: {0}".format(mosaic_dataset_overview_path))
                                
                                    # Define how Overviews will be created and sets
                                    # the location of Mosaic Dataset overview TIFF files
                                    #     pixel size of the first level overview is cellsizeOVR
                                    #     overview_factor="2"
                                    #     force_overview_tiles="FORCE_OVERVIEW_TILES"
                                    #     compression_method="LZW"
                                    arcpy.DefineOverviews_management(md_path, mosaic_dataset_overview_path,
                                                                     in_template_dataset="#", extent="#",
                                                                     pixel_size=cellsizeOVR, number_of_levels="#", tile_rows="5120", tile_cols="5120", overview_factor="2",
                                                                     force_overview_tiles="FORCE_OVERVIEW_TILES", resampling_method="BILINEAR", compression_method="LZW",
                                                                     compression_quality="100")
                                    Utility.addToolMessages()
                                
                                    # Build Overviews as defined in the previous step
                                    #    define_missing_tiles="NO_DEFINE_MISSING_TILES"
                                    arcpy.BuildOverviews_management(md_path, where_clause="#", define_missing_tiles="NO_DEFINE_MISSING_TILES",
                                                                    generate_overviews="GENERATE_OVERVIEWS", generate_missing_images="GENERATE_MISSING_IMAGES",
                                                                    regenerate_stale_images="REGENERATE_STALE_IMAGES")
                                    Utility.addToolMessages()
                                
                                    # Get a count to determine how many service overviews were generated
                                    result = arcpy.GetCount_management(md_path)
                                    countRows = int(result.getOutput(0))
                                    countOverviews = countRows - countRasters
                                    if countOverviews == 0:
                                            arcpy.AddError("No service overviews were created for {0}".format(md_path))
                                    else:
                                            arcpy.AddMessage("{0} has {1} service overview(s).".format(md_path, countOverviews))
                                
                                    out_las_dataset = ProjectFolder.derived.lasd_path
                                    if out_las_dataset is not None: 
                                        descFirstLAS = arcpy.Describe(out_las_dataset)
                                        SpatRefFirstLAS = descFirstLAS.SpatialReference
                                        SpatRefStringFirstLAS = descFirstLAS.SpatialReference.exportToString()
                                
                                        arcpy.AddMessage("Spatial Reference name of first LAS file:    {0}".format(SpatRefFirstLAS.name))
                                        arcpy.AddMessage("Spatial Reference X,Y Units of first LAS file: {0}".format(SpatRefFirstLAS.linearUnitName))
                                        arcpy.AddMessage("Spatial Reference PCS code first LAS file: {0}".format(SpatRefFirstLAS.PCSCode))
    
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
                                
                                        arcpy.AddMessage("Maximum value for ItemTS before adding LAS to Project Mosaic Dataset:             {0}".format(MaxItemTSValue))
                                    
                                        # Add the LAS files to the Mosaic Dataset, but don't recalculate cell size ranges, since MaxPS will be
                                        # set in a subsequent step. Don't update the boundary.
                                        LAS_Raster_type = LAS.LAS_raster_type_1_all_bin_mean_idw
                                        LAS_Folder = ProjectFolder.delivered.lasUnclassified_path
                                        if isClassified:
                                            LAS_Folder = ProjectFolder.delivered.lasClassified_path
                                        arcpy.AddRastersToMosaicDataset_management(md_path, LAS_Raster_type, LAS_Folder,
                                                                                     update_cellsize_ranges="NO_CELL_SIZES", update_boundary="NO_BOUNDARY", update_overviews="NO_OVERVIEWS",
                                                                                     maximum_pyramid_levels="#", maximum_cell_size="0", minimum_dimension="1500",
                                                                                     spatial_reference=SpatRefStringFirstLAS,
                                                                                     filter="#", sub_folder="NO_SUBFOLDERS", duplicate_items_action="ALLOW_DUPLICATES",
                                                                                     build_pyramids="NO_PYRAMIDS", calculate_statistics="NO_STATISTICS",
                                                                                     build_thumbnails="NO_THUMBNAILS", operation_description="#",
                                                                                     force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE")
                                    
                                        Utility.addToolMessages()
                                        
                                        # This tool is re-run because sometimes the clip_to_footprints="NOT_CLIP" gets re-set to "CLIP" for some reason
                                        arcpy.SetMosaicDatasetProperties_management(md_path, rows_maximum_imagesize="25000", columns_maximum_imagesize="25000",
                                                                                    allowed_compressions="None;LZ77;JPEG;LERC", default_compression_type="LERC", JPEG_quality="75",
                                                                                    LERC_Tolerance="0.001", resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP",
                                                                                    footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA", clip_to_boundary="NOT_CLIP",
                                                                                    color_correction="NOT_APPLY", allowed_mensuration_capabilities="#",
                                                                                    default_mensuration_capabilities="NONE",
                                                                                    allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None",
                                                                                    default_mosaic_method="NorthWest", order_field=CMDRConfig.PROJECT_DATE, order_base="#",
                                                                                    sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width="0", view_point_x="600",
                                                                                    view_point_y="300", max_num_per_mosaic="20", cell_size_tolerance="0.8", cell_size="#",
                                                                                    metadata_level="BASIC",
                                                                                    transmission_fields=transmissionFields,
                                                                                    use_time="DISABLED", start_time_field=CMDRConfig.PROJECT_DATE, end_time_field="#", time_format="#",
                                                                                    geographic_transform="#", max_num_of_download_items="40", max_num_of_records_returned="2000",
                                                                                    data_source_type="ELEVATION", minimum_pixel_contribution="1", processing_templates="None",
                                                                                    default_processing_template="None")
                                    
                                        # Get a count to determine how many LAS tiles were added to the MD
                                        result = arcpy.GetCount_management(md_path)
                                        countRowsWithLAS = int(result.getOutput(0))
                                        countLAS = countRowsWithLAS - countRows
                                        if countLAS == 0:
                                                arcpy.AddWarning("No LAS rows were ingested into {0}".format(md_path))
                                        else:
                                                arcpy.AddMessage("{0} has {1} LAS row(s).".format(md_path, countLAS))
                                                    
                                        # If the LAS have Z-units of FOOT_INTL or FOOT_US then append the appropriate function to their
                                        # function chain to convert the elevation values from feet to meters                                
    #                                     if raster_Z_Unit.upper().endswith('FEET') and "METER" in SpatRefMD.linearUnitName.upper():
                                        if raster_Z_Unit.upper() == "Survey Feet".upper() or raster_Z_Unit.upper() == "International Feet".upper():
                                            where_clause = "ItemTS > " + str(MaxItemTSValue) + " AND CATEGORY = 1"
                                            arcpy.AddMessage("\nMosaic Layer where clause: {0}".format(where_clause))
                                            arcpy.MakeMosaicLayer_management(md_path, "ProjectMDLayer1", where_clause, template="#", band_index="#",
                                                                                 mosaic_method="NORTH_WEST", order_field=CMDRConfig.PROJECT_DATE, order_base_value="",
                                                                                 lock_rasterid="#", sort_order="ASCENDING", mosaic_operator="LAST", cell_size="1")
                                
                                            Utility.addToolMessages()
                                            
                                            arcpy.AddMessage("Inserting a function to convert LAS Feet to Meters, since LAS has Z_Unit of:    {0}".format(raster_Z_Unit))
                                
                                            # Use the applicable function chain for either Foot_US or Foot_Intl
                                            if raster_Z_Unit.upper() == "Survey Feet".upper():
                                                    functionChainDef = Raster.Us_ft2mtrs_function_chain_path
                                            else:
                                                    functionChainDef = Raster.Intl_ft2mtrs_function_chain_path  # Intl_ft2mtrs_function_chain
                                                    
                                            arcpy.EditRasterFunction_management("ProjectMDLayer1", edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET_ITEM", edit_options="INSERT",
                                                                                function_chain_definition=functionChainDef, location_function_name="#")
                                            Utility.addToolMessages()
    
#                                         las_footprints = os.path.join(ProjectFolder.derived.fgdb_path, "FOOTPRINT_LAS")
                                        if "IDW" in LAS_Raster_type.upper():
                                            # Build the LAS footprints as long as we're using Binning w/ IDW void filling
                                            # Otherwise if the LAS data has many NoData Holes then the footprints are too jagged
                                            arcpy.BuildFootprints_management(md_path, where_clause="MinPS IS NULL", reset_footprint="RADIOMETRY", min_data_value="1",
                                                                                                         max_data_value="4294967295", approx_num_vertices="100", shrink_distance="0", maintain_edges="MAINTAIN_EDGES",
                                                                                                         skip_derived_images="SKIP_DERIVED_IMAGES", update_boundary="NO_BOUNDARY", request_size="2000",
                                                                                                         min_region_size="20", simplification_method="NONE", edge_tolerance="#", max_sliver_size="20", min_thinness_ratio="0.05")
                                
                                            Utility.addToolMessages()
                                        else:
                                            arcpy.AddMessage("No Footprints built on LAS, since the LAS Raster type does not use IDW void filling")
                                             
                                    else:
                                        arcpy.AddMessage("No LAS files found in {0}".format(LAS_Folder))
                                            
                                    # Calculate the value of certain metadata fields using an update cursor:
                                    fields = ['OBJECTID', 'MINPS', 'MAXPS', CMDRConfig.PROJECT_ID, CMDRConfig.PROJECT_DATE, CMDRConfig.PROJECT_SOURCE, CMDRConfig.PROJECT_SR_XY_NAME, CMDRConfig.PROJECT_SR_XY_UNITS, CMDRConfig.PROJECT_SR_Z_UNITS, 'ZORDER', CMDRConfig.PROJECT_SR_XY_CODE, CMDRConfig.RASTER_PATH, 'CATEGORY']
                                    
                                    with arcpy.da.UpdateCursor(md_path, fields) as rows:  # @UndefinedVariable
                                        for row in rows:
                                            # all new rows (primary raster, LAS, and overviews) will have ProjectID and ProjectDate
                                            row[3] = ProjectID
                                            row[4] = ProjectDate
                                
                                            category = row[12]
                                
                                            # If MinPS is set (i.e. > 0), the row is a primary raster or an overview
                                            minps = row[1]
                                            if minps >= 0:
                                                # Set ZORDER = -1 for these raster items. This will ensure that the rasters are given priority over LAS data, if they are added.
                                                # This will ensure performance is maintained, in case LAS is also ingested (since LAS data takes longer to render).
                                                row[9] = -1
                                                # The source of the raster data (PROJECTSOURCE as specified on the UI)
                                                row[5] = raster_source
                                                if category == 1 or category == 2:
                                                        # The short name of the Spatial reference of the Mosaic Dataset (which is the same as the raster data)
                                                        spatialReference = arcpy.Describe(row[11]).spatialReference
                                                        row[6] = spatialReference.name
                                                        row[7] = spatialReference.linearUnitName
                                                if category == 1:
                                                        row[8] = raster_Z_Unit
                                                        row[5] = RasterConfig.PROJECT_SOURCE_LAS
                                                elif category == 2:
                                                        row[8] = "METER"
                                                        row[5] = RasterConfig.PROJECT_SOURCE_LAS
                                                # if any rasters have PCSCode = 0 then output a warning message at the end
                                                if spatialReference.PCSCode == 0:
                                                        PCSCodeZeroFlag = 1
                                                        arcpy.AddError("Raster has a PCSCode (EPSG code) of 0: Category={} Path={}".format(row[12], row[11]))
                                                row[10] = spatialReference.PCSCode
                                            else:
                                                # If MINPS is Null then the row was just ingested (and is LAS)
                                                # set LAS MINPS to 0.0
                                                row[1] = 0.0000
                                                # Set LAS MAXPS to 0.25 Meter 
                                                row[2] = 0.25000
                                                # PROJECTSOURCE
                                                row[5] = RasterConfig.PROJECT_SOURCE_LAS
                                                # Name, Linear Unit, and PCSCode are based on the Spatial reference of the first LAS in the LAS Folder
                                                row[6] = SpatRefFirstLAS.name
                                                row[7] = SpatRefFirstLAS.linearUnitName
                                                row[10] = SpatRefFirstLAS.PCSCode
                                                # Entered at the UI
                                                row[8] = raster_Z_Unit
                                            rows.updateRow(row)
                                    del row
                                    del rows
                                
                                    arcpy.GetRasterProperties_management(md_path, property_type="MINIMUM", band_index="Band_1")
                                    Utility.addToolMessages()
                                
                                    arcpy.GetRasterProperties_management(md_path, property_type="MAXIMUM", band_index="Band_1")
                                    Utility.addToolMessages()
                                
                                    # Calculate statistics and histogram for all rows in the Mosaic Dataset
                                    arcpy.BuildPyramidsandStatistics_management(md_path, include_subdirectories="INCLUDE_SUBDIRECTORIES",
                                                                                build_pyramids="NONE", calculate_statistics="CALCULATE_STATISTICS",
                                                                                BUILD_ON_SOURCE="NONE", block_field="#", estimate_statistics="NONE",
                                                                                x_skip_factor="10", y_skip_factor="10", ignore_values="#", pyramid_level="-1",
                                                                                SKIP_FIRST="NONE", resample_technique="BILINEAR", compression_type="DEFAULT",
                                                                                compression_quality="75", skip_existing="SKIP_EXISTING")
                                
                                    Utility.addToolMessages()
                                    
                                    # This tool is re-run because sometimes the clip_to_footprints="NOT_CLIP" gets re-set to "CLIP" for some reason
                                    arcpy.SetMosaicDatasetProperties_management(md_path, rows_maximum_imagesize="25000", columns_maximum_imagesize="25000",
                                                                                allowed_compressions="None;LZ77;JPEG;LERC", default_compression_type="LERC", JPEG_quality="75",
                                                                                LERC_Tolerance="0.001", resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP",
                                                                                footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA", clip_to_boundary="NOT_CLIP",
                                                                                color_correction="NOT_APPLY", allowed_mensuration_capabilities="#",
                                                                                default_mensuration_capabilities="NONE",
                                                                                allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None",
                                                                                default_mosaic_method="NorthWest", order_field=CMDRConfig.PROJECT_DATE, order_base="#",
                                                                                sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width="0", view_point_x="600",
                                                                                view_point_y="300", max_num_per_mosaic="20", cell_size_tolerance="0.8", cell_size="#",
                                                                                metadata_level="BASIC",
                                                                                transmission_fields=transmissionFields,
                                                                                use_time="DISABLED", start_time_field=CMDRConfig.PROJECT_DATE, end_time_field="#", time_format="#",
                                                                                geographic_transform="#", max_num_of_download_items="40", max_num_of_records_returned="2000",
                                                                                data_source_type="ELEVATION", minimum_pixel_contribution="1", processing_templates="None",
                                                                                default_processing_template="None")
                                    Utility.addToolMessages()
                                    
                                    minResult = arcpy.GetRasterProperties_management(md_path, property_type="MINIMUM", band_index="Band_1")
                                    Utility.addToolMessages()
                                    minMDValue = float(minResult.getOutput(0))
                                
                                    maxResult = arcpy.GetRasterProperties_management(md_path, property_type="MAXIMUM", band_index="Band_1")
                                    Utility.addToolMessages()
                                    maxMDValue = float(maxResult.getOutput(0))
                                
                                    # Only Calculate Statistics if they are corrupted (The constants can apply to Meters or Feet)
                                    if minMDValue < -300.0 or maxMDValue > 30000.0:
                                        # Calculate stats on the Mosaic Dataset (note: if this takes too long, enlarge skip factors)
                                        arcpy.CalculateStatistics_management(md_path, x_skip_factor="1", y_skip_factor="1", ignore_values="#", skip_existing="OVERWRITE",
                                                                                                                 area_of_interest="Feature Set")
                                
                                        Utility.addToolMessages()
                                
                                        arcpy.GetRasterProperties_management(md_path, property_type="MINIMUM", band_index="Band_1")
                                        Utility.addToolMessages()
                                
                                        arcpy.GetRasterProperties_management(md_path, property_type="MAXIMUM", band_index="Band_1")
                                        Utility.addToolMessages()
                                    
                                    elif minMDValue < minZ or maxMDValue > maxZ:
                                        arcpy.AddWarning("Min/Max MD values ({},{}) don't match expected values ({},{})".format(minMDValue, maxMDValue, minZ, maxZ))
                                    
                                    # Analyze the Mosaic Dataset in preparation for publishing it
                                    arcpy.AnalyzeMosaicDataset_management(md_path, where_clause="#", checker_keywords="FOOTPRINT;FUNCTION;RASTER;PATHS;SOURCE_VALIDITY;STALE;PYRAMIDS;STATISTICS;PERFORMANCE;INFORMATION")    
                                
                                    Utility.addToolMessages()
                                    arcpy.AddMessage("To view detailed results, Add the MD to the map, rt-click --> Data --> View Analysis Results")
                                
                                    if PCSCodeZeroFlag == 1:
                                        arcpy.AddWarning("*** Refer to the PCSCode column in the Footprint table for specifics.***")
                                        arcpy.AddWarning("*** PCSCode = 0 indicates a non-standard datum or unit of measure.     ***")
                                        arcpy.AddError("One or more rasters has a PCSCode (EPSG code) of 0.")
                                else:
                                    arcpy.AddError("Failed to add rasters to Project Mosaic Dataset {}.".format(md_path))
                            else:
                                arcpy.AddError("Project Mosaic Dataset exists {}. Please remove before continuing.".format(md_path))
                        else:
                            arcpy.AddError("One or more rasters has a PCSCode (EPSG code) that does not match. Can not continue.")
                    else:
                        arcpy.AddWarning("No Raster files found in folder {}. Can not process this folder..".format(imagePath))
                # End FOR folders (DSM, DTM...)
            else:
                arcpy.AddError("Project vertical unit is NULL. Please add this to the CMDR before proceeding.")
        else:
            arcpy.AddError("Project End Date is NULL. Please add this to the CMDR before proceeding.")
    else:
        arcpy.AddError("Project not found in the CMDR. Please add this to the CMDR before proceeding.")         

    arcpy.AddMessage("Operation complete")


if __name__ == '__main__':
    CreateProjectMosaicDataset(2402)
