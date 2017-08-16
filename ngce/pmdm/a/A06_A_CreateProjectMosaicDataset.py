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
from ngce.las import LAS
from ngce.pmdm.a import A04_C_ConsolidateLASInfo, A05_A_RemoveDEMErrantValues, A05_C_ConsolidateRasterInfo
from ngce.raster import Raster, RasterConfig
from ngce.raster.RasterConfig import PROJECT_SOURCE_LAS


Utility.setArcpyEnv(True)


A04_C_ConsolidateLASInfo


def mergeFootprints(las_footprints, raster_footprints, el_type, fgdb_path):
    output_path = os.path.join(fgdb_path, "FootprintAll_{}".format(el_type))
    arcpy.Merge_management(inputs=";".join([las_footprints, raster_footprints]), output=output_path,
                           field_mappings='path "path" true true false 254 Text 0 0 ,First,#,{1},path,-1,-1,{0},path,-1,-1;name "name" true true false 100 Text 0 0 ,First,#,{1},name,-1,-1,{0},name,-1,-1;area "area" true true false 8 Double 0 0 ,First,#,{1},area,-1,-1,{0},area,-1,-1;el_type "el_type" true true false 254 Text 0 0 ,First,#,{1},el_type,-1,-1;zran "zran" true true false 8 Double 0 0 ,First,#,{1},zran,-1,-1,{0},zran,-1,-1;zmax "zmax" true true false 8 Double 0 0 ,First,#,{1},zmax,-1,-1,{0},zmax,-1,-1;zmean "zmean" true true false 8 Double 0 0 ,First,#,{1},zmean,-1,-1,{0},zmean,-1,-1;zmin "zmin" true true false 8 Double 0 0 ,First,#,{1},zmin,-1,-1,{0},zmin,-1,-1;zdev "zdev" true true false 8 Double 0 0 ,First,#,{1},zdev,-1,-1,{0},zdev,-1,-1;width "width" true true false 8 Double 0 0 ,First,#,{1},width,-1,-1;height "height" true true false 8 Double 0 0 ,First,#,{1},height,-1,-1;cell_h "cell_h" true true false 8 Double 0 0 ,First,#,{1},cell_h,-1,-1;cell_w "cell_w" true true false 8 Double 0 0 ,First,#,{1},cell_w,-1,-1;comp_type "comp_type" true true false 50 Text 0 0 ,First,#,{1},comp_type,-1,-1;format "format" true true false 50 Text 0 0 ,First,#,{1},format,-1,-1;pixel "pixel" true true false 100 Text 0 0 ,First,#,{1},pixel,-1,-1;unc_size "unc_size" true true false 8 Double 0 0 ,First,#,{1},unc_size,-1,-1;xmin "xmin" true true false 8 Double 0 0 ,First,#,{1},xmin,-1,-1,{0},xmin,-1,-1;ymin "ymin" true true false 8 Double 0 0 ,First,#,{1},ymin,-1,-1,{0},ymin,-1,-1;xmax "xmax" true true false 8 Double 0 0 ,First,#,{1},xmax,-1,-1,{0},xmax,-1,-1;ymax "ymax" true true false 8 Double 0 0 ,First,#,{1},ymax,-1,-1,{0},ymax,-1,-1;v_name "v_name" true true false 100 Text 0 0 ,First,#,{1},v_name,-1,-1,{0},v_name,-1,-1;v_unit "v_unit" true true false 100 Text 0 0 ,First,#,{1},v_unit,-1,-1,{0},v_unit,-1,-1;h_name "h_name" true true false 100 Text 0 0 ,First,#,{1},h_name,-1,-1,{0},h_name,-1,-1;h_unit "h_unit" true true false 100 Text 0 0 ,First,#,{1},h_unit,-1,-1,{0},h_unit,-1,-1;h_wkid "h_wkid" true true false 100 Text 0 0 ,First,#,{1},h_wkid,-1,-1,{0},h_wkid,-1,-1;nodata "nodata" true true false 8 Double 0 0 ,First,#,{1},nodata,-1,-1;Project_ID "Project_ID" true true false 50 Text 0 0 ,First,#,{1},Project_ID,-1,-1,{0},Project_ID,-1,-1;Project_Dir "Project_Dir" true true false 1000 Text 0 0 ,First,#,{1},Project_Dir,-1,-1,{0},Project_Dir,-1,-1;Project_GUID "Project_GUID" true true false 38 Guid 0 0 ,First,#,{1},Project_GUID,-1,-1,{0},Project_GUID,-1,-1;is_class "is_class" true true false 10 Text 0 0 ,First,#,{0},is_class,-1,-1;ra_pt_ct "ra_pt_ct" true true false 8 Double 0 0 ,First,#,{0},ra_pt_ct,-1,-1;ra_pt_sp "ra_pt_sp" true true false 8 Double 0 0 ,First,#,{0},ra_pt_sp,-1,-1;ra_zmin "ra_zmin" true true false 8 Double 0 0 ,First,#,{0},ra_zmin,-1,-1;ra_zmax "ra_zmax" true true false 8 Double 0 0 ,First,#,{0},ra_zmax,-1,-1;ra_zran "ra_zran" true true false 8 Double 0 0 ,First,#,{0},ra_zran,-1,-1'.format(las_footprints, raster_footprints))
    
    out_layer = arcpy.MakeFeatureLayer_management(in_features=output_path, out_layer="FootprintAll_DTM_LAS", where_clause="el_type IS NULL")
    arcpy.CalculateField_management(in_table=out_layer, field="el_type", expression='"LAS"', expression_type="PYTHON_9.3", code_block="")
    return output_path





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


















# def CreateProjectMosaicDataset(jobID, publish=True):
#     Utility.printArguments(["WMX Job ID", "publish"], [jobID, publish], "A06 Create Project Mosaic Dataset")
#     
#     Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
#     
#     ProjectJob = CMDR.ProjectJob()
#     project, ProjectUID = ProjectJob.getProject(jobID)  # @UnusedVariable
# #     ProjectYear = ProjectJob.getYear(project)
#     if project is not None:
# #         ProjectID = ProjectJob.getProjectID(project)
# #         ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
# #         TargetFolder = ProjectFolder.derived
# #         if publish:
# #             TargetFolder = ProjectFolder.published
#         
#         processProject(ProjectJob, project, ProjectUID)
#     
# #         Deliver = CMDR.Deliver()
# #         delivery = list(Deliver.getDeliver(ProjectID))
# #         
# #         # Note: the default minimum of 1 was excluding some shoreline, since elevation is < 1 at the shoreline
# #         minZ = Deliver.getValidZMin(delivery)
# #         maxZ = Deliver.getValidZMax(delivery)
# #         minZ, maxZ = LAS.validateZRange(minZ, maxZ)
        





def processProject(ProjectJob, project, ProjectUID, dateDeliver, dateStart, dateEnd):
    
    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    ProjectID = ProjectJob.getProjectID(project)
#     ProjectUID = ProjectJob.getUID(project)
    project_year = ProjectJob.getYear(project)
    
    
    publish_folder = ProjectFolder.published
    fgdb_path = ProjectFolder.derived.fgdb_path
    
    lasd_boundary_path = A04_C_ConsolidateLASInfo.getLasdBoundaryPath(fgdb_path)
    las_footprint_path = A04_C_ConsolidateLASInfo.getLasFootprintPath(fgdb_path)
    
    
    raster_boundary_main = A05_C_ConsolidateRasterInfo.getRasterBoundaryPath(fgdb_path)
    
    las_z_min, las_z_max, las_v_name, las_v_unit, las_h_name, las_h_unit, las_h_wkid, isClassified = A05_A_RemoveDEMErrantValues.getLasdBoundData(lasd_boundary_path)
    
    PCSCodeZeroFlag = 0
        
    if dateDeliver is None and project_year is not None:
        dateDeliver = "6/1/{}".format(project_year)
    if dateStart is None:
        if dateEnd is not None:
            dateStart = dateEnd
        else:
            dateStart = dateDeliver
    if dateEnd is None:
        if dateStart is not None:
            dateEnd = dateStart
        else:
            dateEnd = dateDeliver
                    
    ImageDirectories = [DSM, DTM]
    
    for imageDir in ImageDirectories:
        raster_z_min, raster_z_max, raster_v_name, raster_v_unit, raster_h_name, raster_h_unit, raster_h_wkid = A05_A_RemoveDEMErrantValues.getRasterBoundData(raster_boundary_main, imageDir, False)
        filegdb_name, filegdb_ext = os.path.splitext(publish_folder.fgdb_name)
        filegdb_name = "{}_{}.gdb".format(filegdb_name, imageDir)
        target_path = os.path.join(publish_folder.path, imageDir) 
        filegdb_path = os.path.join(target_path, filegdb_name)
        
        raster_footprint_path = A05_C_ConsolidateRasterInfo.getRasterFootprintPath(fgdb_path, imageDir)
        footprint_path = mergeFootprints(las_footprint_path, raster_footprint_path, imageDir, filegdb_path)
                   
                    
        # If the file gdb doesn't exist, then create it
        if not os.path.exists(filegdb_path):
            arcpy.CreateFileGDB_management(target_path, filegdb_name)
            Utility.addToolMessages()
                    
        imagePath = target_path
        SRMatchFlag, ras_count = isSpatialRefSameForAll(imagePath)
        if not SRMatchFlag:
            arcpy.AddError("SR doesn't match for all images, aborting.")
        elif ras_count <= 0:
            arcpy.AddError("No rasters for selected elevation type {}.".format(imageDir))
        else:
            arcpy.AddMessage("Working on {} rasters for elevation type {}.".format(ras_count, imageDir))            
                            
            md_name = imageDir
            md_path = os.path.join(filegdb_path, md_name)
                             
            A05_C_ConsolidateRasterInfo.deleteFileIfExists(md_path, True)
                   
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
            countRasters = int(arcpy.GetCount_management(md_path).getOutput(0))
            Utility.addToolMessages()
            arcpy.AddMessage("{} has {} raster dataset(s).".format(md_path, countRasters))
                
            # Calculate the values of MinPS and MaxPS with max_range_factor = 3 (for performance)
            arcpy.CalculateCellSizeRanges_management(md_path,
                                                     where_clause="#",
                                                     do_compute_min="MIN_CELL_SIZES",
                                                     do_compute_max="MAX_CELL_SIZES",
                                                     max_range_factor="3",
                                                     cell_size_tolerance_factor="0.8",
                                                     update_missing_only="UPDATE_ALL")
            Utility.addToolMessages()
                
#             # Build footprints with up to 3000 vertices (to eliminate possible slivers)
#             # These footprints will be simplified
#             # Note: minimum elevation value is set lower than the default value (1) to get at accurate value at the coast
#             # EI added 10% of range (10% below min and 10% above max)
#             arcpy.BuildFootprints_management(md_path, 
#                                              where_clause="#", 
#                                              reset_footprint="RADIOMETRY", 
#                                              min_data_value=(minZ - rangeZ10p), 
#                                              max_data_value=(maxZ + rangeZ10p),
#                                              approx_num_vertices="3000", 
#                                              shrink_distance="0", 
#                                              maintain_edges="NO_MAINTAIN_EDGES",
#                                              skip_derived_images="SKIP_DERIVED_IMAGES", 
#                                              update_boundary="NO_BOUNDARY", 
#                                              request_size="2000",
#                                              min_region_size="100", 
#                                              simplification_method="NONE", 
#                                              edge_tolerance="#", 
#                                              max_sliver_size="20", 
#                                              min_thinness_ratio="0.05")
#             Utility.addToolMessages()
                
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
                                
#                                     # Export, simplify, and Import Footprints
#                                     arcpy.MakeMosaicLayer_management(md_path, "ProjectMDLayer2", where_clause="", template="#", band_index="#",
#                                                                      mosaic_method="NORTH_WEST", order_field=CMDRConfig.PROJECT_DATE, order_base_value="",
#                                                                      lock_rasterid="#", sort_order="ASCENDING", mosaic_operator="LAST", cell_size="1")
#                                     Utility.addToolMessages()
#                                     
#                                     arcpy.AddMessage("Removing unnecessary vertices from Footprints")
#     
#                                     footprint_simple = os.path.join(filegdb_path, "FOOTPRINT_{}".format(imageDir))
#                                     arcpy.Delete_management(footprint_simple)
#                                     Utility.addToolMessages()
#                                     
#                                     arcpy.SimplifyPolygon_cartography(in_features=r"ProjectMDLayer2/Footprint", out_feature_class=footprint_simple,
#                                                                     algorithm="POINT_REMOVE", tolerance=Raster.boundary_interval, minimum_area="0 SquareMeters",
#                                                                     error_option="RESOLVE_ERRORS", collapsed_point_option="KEEP_COLLAPSED_POINTS")
#                                     Utility.addToolMessages()
                
            
                                    
            # The spatial reference of the Mosaic Dataset 
            descMD = arcpy.Describe(md_path)
            SpatRefMD = descMD.SpatialReference
                
            # Determine the cell size of the Mosaic Dataset
            cellsizeResult = arcpy.GetRasterProperties_management(md_path, property_type="CELLSIZEX", band_index="")
            Utility.addToolMessages()
            cellsize = float(cellsizeResult.getOutput(0))
            arcpy.AddMessage("Cell size of MD:    {0} {1}".format(cellsize, SpatRefMD.linearUnitName))
            
                
            # If the Rasters have Z-units of FOOT_INTL or FOOT_US then append the appropriate function to their
            # function chain to convert the elevation values from feet to meters 
            if raster_v_unit.upper() == "International Feet".upper():  # and "METER" in SpatRefMD.linearUnitName.upper():        
                arcpy.EditRasterFunction_management(md_path, edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET_ITEM", edit_options="INSERT",
                                                    function_chain_definition=Raster.Intl_ft2mtrs_function_chain_path,
                                                    location_function_name="#")
                Utility.addToolMessages()
            elif raster_v_unit.upper() == "Survey Feet".upper():  # and "METER" in SpatRefMD.linearUnitName.upper():        
                arcpy.EditRasterFunction_management(md_path, edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET_ITEM", edit_options="INSERT",
                                                    function_chain_definition=Raster.Us_ft2mtrs_function_chain_path,
                                                    location_function_name="#")
                Utility.addToolMessages()
            else:
                arcpy.AddMessage("Raster vertical unit is meters, no need for conversion.")
        
            arcpy.CalculateStatistics_management(md_path,
                                                 x_skip_factor="1",
                                                 y_skip_factor="1",
                                                 ignore_values="#",
                                                 skip_existing="SKIP_EXISTING",
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
            
            
            # @TODO: Move this to Overview configuration method
            
            # Set the cell size of the first level overview according to the cell size of the Mosaic Dataset
            # Do this by doubling cell size and finding the next ArcGIS Online cache scale            
            cellsizeOVR = Raster.getOverviewCellSize(cellsize)
            arcpy.AddMessage("Cell size of First level Overview:    {0} {1}".format(cellsizeOVR, SpatRefMD.linearUnitName))
            
                
            # Location of Mosaic Dataset overview TIFF files (Note: this folder needs to be in the ArcGIS Server Data Store)
            mosaic_dataset_overview_path = os.path.join(publish_folder.path, md_name, "{}.Overviews".format(md_name))
            arcpy.AddMessage("Mosaic Dataset Overview Folder: {0}".format(mosaic_dataset_overview_path))
        
            # Define how Overviews will be created and sets
            # the location of Mosaic Dataset overview TIFF files
            #     pixel size of the first level overview is cellsizeOVR
            #     overview_factor="2"
            #     force_overview_tiles="FORCE_OVERVIEW_TILES"
            #     compression_method="LZW"
            arcpy.DefineOverviews_management(md_path,
                                             mosaic_dataset_overview_path,
                                             in_template_dataset="#",
                                             extent="#",
                                             pixel_size=cellsizeOVR,
                                             number_of_levels="#",
                                             tile_rows="5120",
                                             tile_cols="5120",
                                             overview_factor="2",
                                             force_overview_tiles="FORCE_OVERVIEW_TILES",
                                             resampling_method="BILINEAR",
                                             compression_method="LZ77",
                                             compression_quality="100")
            Utility.addToolMessages()
        
            # Build Overviews as defined in the previous step
            #    define_missing_tiles="NO_DEFINE_MISSING_TILES"
            arcpy.BuildOverviews_management(md_path,
                                            where_clause="#",
                                            define_missing_tiles="NO_DEFINE_MISSING_TILES",
                                            generate_overviews="GENERATE_OVERVIEWS",
                                            generate_missing_images="GENERATE_MISSING_IMAGES",
                                            regenerate_stale_images="REGENERATE_STALE_IMAGES")
            Utility.addToolMessages()
            
            arcpy.BuildPyramidsandStatistics_management(in_workspace=mosaic_dataset_overview_path,
                                                        build_pyramids="BUILD_PYRAMIDS",
                                                        calculate_statistics="CALCULATE_STATISTICS",
                                                        BUILD_ON_SOURCE="BUILD_ON_SOURCE",
                                                        pyramid_level="-1",
                                                        SKIP_FIRST="NONE",
                                                        resample_technique="BILINEAR",
                                                        compression_type="LZ77",
                                                        compression_quality="75",
                                                        skip_existing="SKIP_EXISTING",
                                                        include_subdirectories="INCLUDE_SUBDIRECTORIES")
                                
            # Get a count to determine how many service overviews were generated
            result = arcpy.GetCount_management(md_path)
            countRows = int(result.getOutput(0))
            countOverviews = countRows - countRasters
            if countOverviews == 0:
                    arcpy.AddError("No service overviews were created for {0}".format(md_path))
            else:
                    arcpy.AddMessage("{} has {} service overview(s).".format(md_path, countOverviews))
                                
                                
                                
                                
                                
                                
                                
                                
            # @TODO: Move this to LAS Dataset configuration method
                                
                                
                                
                                
                                
                                
            out_las_dataset = ProjectFolder.derived.lasd_path
                                        
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
            arcpy.AddRastersToMosaicDataset_management(md_path,
                                                       LAS_Raster_type,
                                                       LAS_Folder,
                                                         update_cellsize_ranges="NO_CELL_SIZES",
                                                         update_boundary="NO_BOUNDARY",
                                                         update_overviews="NO_OVERVIEWS",
                                                         maximum_pyramid_levels="#",
                                                         maximum_cell_size="0",
                                                         minimum_dimension="1500",
                                                         spatial_reference=SpatRefStringFirstLAS,
                                                         filter="#",
                                                         sub_folder="NO_SUBFOLDERS",
                                                         duplicate_items_action="ALLOW_DUPLICATES",
                                                         build_pyramids="NO_PYRAMIDS",
                                                         calculate_statistics="NO_STATISTICS",
                                                         build_thumbnails="NO_THUMBNAILS",
                                                         operation_description="#",
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
            
            if las_v_unit.upper() == "Survey Feet".upper() or las_v_unit.upper() == "International Feet".upper():
                where_clause = "ItemTS > " + str(MaxItemTSValue) + " AND CATEGORY = 1"
                arcpy.AddMessage("\nMosaic Layer where clause: {0}".format(where_clause))
                arcpy.MakeMosaicLayer_management(md_path, "ProjectMDLayer1", where_clause, template="#", band_index="#",
                                                     mosaic_method="NORTH_WEST", order_field=CMDRConfig.PROJECT_DATE, order_base_value="",
                                                     lock_rasterid="#", sort_order="ASCENDING", mosaic_operator="LAST", cell_size="1")
    
                Utility.addToolMessages()
                
                arcpy.AddMessage("Inserting a function to convert LAS Feet to Meters, since LAS has Z_Unit of:    {0}".format(las_v_unit))
    
                # Use the applicable function chain for either Foot_US or Foot_Intl
                if las_v_unit.upper() == "Survey Feet".upper():
                        functionChainDef = Raster.Us_ft2mtrs_function_chain_path
                else:
                        functionChainDef = Raster.Intl_ft2mtrs_function_chain_path  # Intl_ft2mtrs_function_chain
                        
                arcpy.EditRasterFunction_management("ProjectMDLayer1", edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET_ITEM", edit_options="INSERT",
                                                    function_chain_definition=functionChainDef, location_function_name="#")
                Utility.addToolMessages()


#                                         if "IDW" in LAS_Raster_type.upper():
#                                             # Build the LAS footprints as long as we're using Binning w/ IDW void filling
#                                             # Otherwise if the LAS data has many NoData Holes then the footprints are too jagged
#                                             arcpy.BuildFootprints_management(md_path, where_clause="MinPS IS NULL", reset_footprint="RADIOMETRY", min_data_value="1",
#                                                                                                          max_data_value="4294967295", approx_num_vertices="100", shrink_distance="0", maintain_edges="MAINTAIN_EDGES",
#                                                                                                          skip_derived_images="SKIP_DERIVED_IMAGES", update_boundary="NO_BOUNDARY", request_size="2000",
#                                                                                                          min_region_size="20", simplification_method="NONE", edge_tolerance="#", max_sliver_size="20", min_thinness_ratio="0.05")
#                                 
#                                             Utility.addToolMessages()
#                                         else:
#                                             arcpy.AddMessage("No Footprints built on LAS, since the LAS Raster type does not use IDW void filling")
                                             
#                                     else:
#                                         arcpy.AddMessage("No LAS files found in {0}".format(LAS_Folder))
            # @TODO: Import the las file footprints here
            
            # import simplified Footprints
            arcpy.ImportMosaicDatasetGeometry_management(md_path,
                                                         target_featureclass_type="FOOTPRINT",
                                                         target_join_field="Name",
                                                         input_featureclass=footprint_path,
                                                         input_join_field="name")
            Utility.addToolMessages()
            
            arcpy.ImportMosaicDatasetGeometry_management(md_path,
                                                         target_featureclass_type="BOUNDARY",
                                                         target_join_field="OBJECTID",
                                                         input_featureclass=lasd_boundary_path,
                                                         input_join_field="OBJECTID")
            Utility.addToolMessages()
            
            arcpy.JoinField_management(in_data=md_path, in_field="Name", join_table=footprint_path, join_field="name", fields="area;el_type;zran;zmax;zmean;zmin;zdev;width;height;cell_h;cell_w;comp_type;format;pixel;unc_size;xmin;ymin;xmax;ymax;v_name;v_unit;h_name;h_unit;h_wkid;nodata;Project_ID;Project_Dir;Project_GUID;is_class;ra_pt_ct;ra_pt_sp;ra_zmin;ra_zmax;ra_zran")
            
            
            # @TODO: Move this to a metadata method        
            # @TODO: Add a start and an end date?
            # Calculate the value of certain metadata fields using an update cursor:
            fields = [
                      'OBJECTID',  # 0 
                      'MINPS',  # 1
                      'MAXPS',  # 2
                      'CATEGORY',  # 3
                      'ZORDER',  # 4
                      CMDRConfig.PROJECT_ID,  # 5
                      CMDRConfig.PROJECT_DATE,  # 6
                      CMDRConfig.PROJECT_SOURCE,  # 7
                      CMDRConfig.RASTER_PATH  # 8
                      ]
                     
            with arcpy.da.UpdateCursor(md_path, fields) as rows:  # @UndefinedVariable
                for row in rows:
                    # all new rows (primary raster, LAS, and overviews) will have ProjectID and ProjectDate
                    row[5] = ProjectID
                    row[6] = dateDeliver
        
                    category = row[3]
                    spatialReference = arcpy.Describe(row[8]).spatialReference
                    # If MinPS is set (i.e. > 0), the row is a primary raster or an overview
                    minps = row[1]
                    if minps >= 0:
                        # Set ZORDER = -1 for these raster items. This will ensure that the rasters are given priority over LAS data, if they are added.
                        # This will ensure performance is maintained, in case LAS is also ingested (since LAS data takes longer to render).
                        row[4] = -1
                        # The source of the raster data (PROJECTSOURCE as specified on the UI)
                        row[7] = PROJECT_SOURCE_LAS
#                         if category == 1 or category == 2:
#                                 # The short name of the Spatial reference of the Mosaic Dataset (which is the same as the raster data)
#                                 spatialReference = arcpy.Describe(row[8]).spatialReference
#                                 row[6] = spatialReference.name
#                                 row[7] = spatialReference.linearUnitName
                        if category == 1:
#                                 row[8] = raster_v_unit
                                row[7] = RasterConfig.PROJECT_SOURCE_RASTER
                        elif category == 2:
#                                 row[8] = "METER"
                                row[7] = RasterConfig.PROJECT_SOURCE_OVERVIEW
                        # if any rasters have PCSCode = 0 then output a warning message at the end
                        if spatialReference.PCSCode == 0:
                                PCSCodeZeroFlag = 1
                                arcpy.AddError("Raster has a PCSCode (EPSG code) of 0: Category={} Path={}".format(row[12], row[11]))
#                         row[10] = spatialReference.PCSCode
                    else:
                        # If MINPS is Null then the row was just ingested (and is LAS)
                        # set LAS MINPS to 0.0
                        row[1] = 0.0000
                        # Set LAS MAXPS to 0.25 Meter 
                        row[2] = 0.25000
                        # PROJECTSOURCE
                        row[7] = RasterConfig.PROJECT_SOURCE_LAS
                        # Name, Linear Unit, and PCSCode are based on the Spatial reference of the first LAS in the LAS Folder
#                         row[6] = SpatRefFirstLAS.name
#                         row[7] = SpatRefFirstLAS.linearUnitName
#                         row[10] = SpatRefFirstLAS.PCSCode
#                         # Entered at the UI
#                         row[8] = raster_v_unit
                        
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
        
                minResult = arcpy.GetRasterProperties_management(md_path, property_type="MINIMUM", band_index="Band_1")
                Utility.addToolMessages()
                minMDValue = float(minResult.getOutput(0))
        
                maxResult = arcpy.GetRasterProperties_management(md_path, property_type="MAXIMUM", band_index="Band_1")
                Utility.addToolMessages()
                maxMDValue = float(maxResult.getOutput(0))
            
            if minMDValue < raster_z_min or maxMDValue > raster_z_max:
                arcpy.AddWarning("Min/Max MD values ({},{}) don't match expected values ({},{})".format(minMDValue, maxMDValue, raster_z_min, raster_z_max))
            
            # Analyze the Mosaic Dataset in preparation for publishing it
            arcpy.AnalyzeMosaicDataset_management(md_path, where_clause="#", checker_keywords="FOOTPRINT;FUNCTION;RASTER;PATHS;SOURCE_VALIDITY;STALE;PYRAMIDS;STATISTICS;PERFORMANCE;INFORMATION")    
            Utility.addToolMessages()
            arcpy.AddMessage("To view detailed results, Add the MD to the map, rt-click --> Data --> View Analysis Results")
        
            if PCSCodeZeroFlag == 1:
                arcpy.AddWarning("*** Refer to the PCSCode column in the Footprint table for specifics.***")
                arcpy.AddWarning("*** PCSCode = 0 indicates a non-standard datum or unit of measure.     ***")
                arcpy.AddError("One or more rasters has a PCSCode (EPSG code) of 0.")


    arcpy.AddMessage("Operation complete")


if __name__ == '__main__':
    #     
#     a = datetime.now()
#     dateStart,dateEnd= None, None
#     jobID = sys.argv[1]
#     dateDeliver= sys.argv[2]
#
#     if len(sys.argv)>=4:
#         dateStart= sys.argv[3]
#     if len(sys.argv)>=5:
#         dateEnd = sys.argv[4]     
#     RemoveDEMErrantValues(jobID, dateDeliver, dateStart,dateEnd)
    
          
    dateStart, dateEnd = None, None
    dateDeliver = "04/09/1971"
    UID = None  # field_ProjectJob_UID
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
    ProjectJob = CMDR.ProjectJob()
    project = [
               UID,  # field_ProjectJob_UID
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
    
    processProject(ProjectJob, project, UID, dateDeliver, dateStart, dateEnd)
