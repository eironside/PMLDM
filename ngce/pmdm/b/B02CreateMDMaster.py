#-------------------------------------------------------------------------------
# Name:        NRCS_CreateMasterMD
#
# Purpose:     To create a Master mosaic dataset, set its boundary,
#              add fields and indexes, and set the MD properties
#
#
# Author:       Roslyn Dunn
# Organization: Esri Inc.
#
# Created:     01/13/2015
#
# Last Edited: 09/24/2015  Create MD with NoData value of -3.40282346639e+038
#                           instead of -3.40282306074E+38 (for consistency)
# *
#-------------------------------------------------------------------------------
import arcpy
import os

from ngce import Utility
from ngce.cmdr import CMDRConfig, CMDR
from ngce.folders import FoldersConfig
from ngce.raster import RasterConfig, Raster


def CreateMasterMosaicDatasets(wmxJobID):
    Utility.printArguments(["wmxJobID"],
                           [wmxJobID], "B02 CreateMasterMosaicDatasets")
    
    Utility.setWMXJobDataAsEnvironmentWorkspace(wmxJobID)
    
    MDMaster = CMDR.MDMaster()
    mdMaster_row = MDMaster.getMDMaster(wmxJobID)
    
    parent_path = MDMaster.getMDParentPath(mdMaster_row)
    mdMaster_path = MDMaster.getMDPath(mdMaster_row)
    MasterMDName = MDMaster.getMDName(mdMaster_row)
    MasterMDCellSize_Meters = MDMaster.getMDCellSize(mdMaster_row)
    
    
    
#     mdMaster_aoi = MDMaster.getMDAOI(mdMaster_row)
    if arcpy.Exists(parent_path):
        if not os.path.exists(mdMaster_path):
            os.makedirs(mdMaster_path)
#         master_fgdb_path = os.path.join(mdMaster_path, MasterMDName)
        
        md_list = [FoldersConfig.DTM, FoldersConfig.DSM, FoldersConfig.DLM, FoldersConfig.DHM, FoldersConfig.DCM, FoldersConfig.INT]
        for md_name in md_list:
            local_fgdb_name = "{}_{}.gdb".format(MasterMDName, md_name)
            arcpy.AddMessage("local_fgdb_name '{}'".format(local_fgdb_name))
            local_fgdb_path = os.path.join(mdMaster_path, local_fgdb_name)
            arcpy.AddMessage("local_fgdb_path '{}'".format(local_fgdb_path))
            
            if not os.path.exists(local_fgdb_path):
                arcpy.AddMessage("creating MD master fGDB '{} / {}'".format(mdMaster_path, local_fgdb_name))
                arcpy.CreateFileGDB_management(mdMaster_path, local_fgdb_name)
                Utility.addToolMessages()
            
            where_clause = "{} = {}".format(arcpy.AddFieldDelimiters(MDMaster.fclass, CMDRConfig.field_MDMaster_WMXJobID), wmxJobID)
#             mdMasterLayer = "MDMasterLayer"
#             arcpy.MakeFeatureLayer_management(in_features= MDMaster.fclass, out_layer = mdMasterLayer, where_clause=where_clause)
            
            local_fgdb_MDMasterFC = os.path.join(local_fgdb_path, MasterMDName)
            if not arcpy.Exists(local_fgdb_MDMasterFC):
                arcpy.FeatureClassToFeatureClass_conversion (in_features=MDMaster.fclass, out_path=local_fgdb_path, out_name=MasterMDName, where_clause=where_clause)
            
                CreateMasterMosaicDataset(local_fgdb_path, md_name, local_fgdb_MDMasterFC, MasterMDCellSize_Meters)
    else:
        arcpy.AddError("MD Master parent path doesn't exist '{}'. Cannot continue.".format(parent_path))
        
    
    

def addMasterBoundary(master_fgdb_path, master_md_name, local_fgdb_MDMasterFC):
#     This is necessary, since the ImportMosaicDatasetGeometry tool won't replace an empty boundary
#     MasterBoundaryFC = RasterConfig.MasterBoundaryFC
    if arcpy.Exists(local_fgdb_MDMasterFC):
        arcpy.AddMessage("MD Master Boundary Feature Class: {0}".format(local_fgdb_MDMasterFC))
        descBound = arcpy.Describe(local_fgdb_MDMasterFC)
        SpatRefBound = descBound.SpatialReference.name
        arcpy.AddMessage("Master Boundary Feature Class has spatial reference: {0}".format(SpatRefBound))
        if SpatRefBound == "WGS_1984_Web_Mercator_Auxiliary_Sphere":
            # record count of the specified boundary feature class should be 1
#             result = arcpy.GetCount_management(local_fgdb_MDMasterFC)
#             Utility.addToolMessages()
#             countRows = int(result.getOutput(0))
#             # @TODO Not sure we need this check
#             if countRows == 1:
            fc = master_fgdb_path + r"\AMD_" + master_md_name + r"_BND"
            arcpy.AddMessage("Master Mosaic Boundary feature class name:             {0}".format(fc))
            fields = ['SHAPE@']
            array = arcpy.Array([arcpy.Point(0, 0),
                    arcpy.Point(1, 0),
                    arcpy.Point(1, 1)])
            polygon = arcpy.Polygon(array)
            with arcpy.da.InsertCursor(fc, fields) as cursor:  # @UndefinedVariable
                cursor.insertRow([polygon])
            del cursor
            # Replace the boundary row (just created) with the row in MasterBoundaryFC
            master_md_path = os.path.join(master_fgdb_path, master_md_name)
            arcpy.AddMessage("Importing boundary to Master GDB...")
            arcpy.ImportMosaicDatasetGeometry_management(master_md_path, target_featureclass_type="BOUNDARY", target_join_field="OBJECTID", input_featureclass=local_fgdb_MDMasterFC, input_join_field="OBJECTID")
            Utility.addToolMessages()
#             else:
#                 arcpy.AddError("\nExiting: {0} should contain only 1 row.".format(local_fgdb_MDMasterFC))
        else:
            arcpy.AddError("Spatial reference of the supplied Master Boundary is not Web Mercator ('{}') Cannot continue.".format(SpatRefBound))
    else:
        arcpy.AddWarning("Master Boundary feature class not found: {0}. Continuing without it".format(local_fgdb_MDMasterFC))

def CreateMasterMosaicDataset(master_fgdb_path, master_md_name, MDMasterFC_path, masterCellSize_meters):
    Utility.printArguments(["master_fgdb_path", "master_md_name", "MDMasterFC_path", "masterCellSize_meters"],
                           [master_fgdb_path, master_md_name, MDMasterFC_path, masterCellSize_meters], "B02 CreateMasterMosaicDataset")
    
    
    # Ensure the Master gdb exists
    if os.path.exists(master_fgdb_path):
        master_md_path = os.path.join(master_fgdb_path, master_md_name)
        arcpy.AddMessage("Full Master Mosaic Name:             {0}".format(master_md_path))
        
        if not arcpy.Exists(master_md_path):
            
                        
            # SpatRefMaster = "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0],AUTHORITY['EPSG',3857]]"
            SpatRefMaster = RasterConfig.SpatRef_WebMercator
            
            # Create the Master Mosaic Dataset
            arcpy.CreateMosaicDataset_management(master_fgdb_path, master_md_name,
                                                 coordinate_system=SpatRefMaster,
                                                 num_bands="1", pixel_type="32_BIT_FLOAT", product_definition="NONE", product_band_definitions="#")
            Utility.addToolMessages()
            
            # If a boundary is specified (it is optional)...
            # Write one record to the boundary so it can be subsequently replaced by the import Mosaic Dataset Geometry tool
            addMasterBoundary(master_fgdb_path, master_md_name, MDMasterFC_path)
                        
            Raster.addStandardMosaicDatasetFields(md_path=master_md_path)
            
                        
                        
                        
                        
            
#                         arcpy.AddField_management(master_md_path, field_name="ProjectID", field_type="TEXT", field_precision="#", field_scale="#",
#                                                   field_length="100", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                         Utility.addToolMessages()
#                         arcpy.AddField_management(master_md_path, field_name="ProjectDate", field_type="DATE", field_precision="#", field_scale="#",
#                                                   field_length="#", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                         Utility.addToolMessages()
#                         arcpy.AddField_management(master_md_path, field_name="RasterPath", field_type="TEXT", field_precision="#", field_scale="#",
#                                                   field_length="512", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
# #                         Utility.addToolMessages()
#                         arcpy.AddField_management(master_md_path, field_name="ProjectSrs", field_type="TEXT", field_precision="#", field_scale="#",
#                                                   field_length="100", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                         Utility.addToolMessages()
#                         arcpy.AddField_management(master_md_path, field_name="ProjectSrsUnits", field_type="TEXT", field_precision="#", field_scale="#",
#                                                   field_length="20", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                         Utility.addToolMessages()
#                         arcpy.AddField_management(master_md_path, field_name="ProjectSrsUnitsZ", field_type="TEXT", field_precision="#", field_scale="#",
#                                                   field_length="20", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                         Utility.addToolMessages()
#                         arcpy.AddField_management(master_md_path, field_name="ProjectSource", field_type="TEXT", field_precision="#", field_scale="#",
#                                                   field_length="20", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                         Utility.addToolMessages()
#                         arcpy.AddField_management(master_md_path, field_name="PCSCode", field_type="TEXT", field_precision="#", field_scale="#",
#                                                   field_length="20", field_alias="#", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="#")
#                         Utility.addToolMessages()
            
#                         arcpy.AddMessage("Creating Indexes on previously created fields in Master GDB...")
            
            # Create indexes on all metadata fields to facilitate query
            
#                         arcpy.AddIndex_management(master_md_path, fields="ProjectID", index_name="ProjectID", unique="NON_UNIQUE", ascending="ASCENDING")
#                         Utility.addToolMessages()
#                         arcpy.AddIndex_management(master_md_path, fields="ProjectDate", index_name="ProjectDate", unique="NON_UNIQUE", ascending="ASCENDING")
#                         Utility.addToolMessages()
#                         arcpy.AddIndex_management(master_md_path, fields="ProjectSrs", index_name="ProjectSrs", unique="NON_UNIQUE", ascending="ASCENDING")
#                         Utility.addToolMessages()
#                         arcpy.AddIndex_management(master_md_path, fields="ProjectSrsUnits", index_name="ProjectSrsUnits", unique="NON_UNIQUE", ascending="ASCENDING")
#                         Utility.addToolMessages()
#                         arcpy.AddIndex_management(master_md_path, fields="ProjectSrsUnitsZ", index_name="ProjectSrsUnitsZ", unique="NON_UNIQUE", ascending="ASCENDING")
#                         Utility.addToolMessages()
#                         arcpy.AddIndex_management(master_md_path, fields="ProjectSource", index_name="ProjectSource", unique="NON_UNIQUE", ascending="ASCENDING")
#                         Utility.addToolMessages()
#                         arcpy.AddIndex_management(master_md_path, fields="PCSCode", index_name="PCSCode", unique="NON_UNIQUE", ascending="ASCENDING")
#                         Utility.addToolMessages()
            
            # Set the desired Master MD properties (non-default parameters are listed below):
            #   default mosaic method is "BYATTRIBUTE" w ProjectDate
            #      order_base = 3000 (a year far into the future)
            #   default_compression_type="LERC"
            #   limited the transmission_fields
            #   start_time_field="ProjectDate" (in case we decide to enable time later)
            #   max_num_of_records_returned="2000" (default is 1000)
            #   max_num_of_download_items="40" (default is 20)
            #   max_num_per_mosaic = "40"      (default is 20)
            #   data_source_type="ELEVATION"
            #   cell_size = 1
            #   rows_maximum_imagesize="25000"
            #   columns_maximum_imagesize="25000"
            #   metadata_level = "BASIC"
            
            transmissionFields = CMDRConfig.TRANSMISSION_FIELDS
            arcpy.AddMessage("transmissionFields: {0}".format(transmissionFields))
            
            arcpy.AddRastersToMosaicDataset_management(in_mosaic_dataset=master_md_path, raster_type="Raster Dataset", input_path=RasterConfig.MasterTempRaster, update_cellsize_ranges="UPDATE_CELL_SIZES", update_boundary="UPDATE_BOUNDARY", update_overviews="NO_OVERVIEWS", maximum_pyramid_levels="", maximum_cell_size="0", minimum_dimension="1500", spatial_reference="", filter="#", sub_folder="SUBFOLDERS", duplicate_items_action="ALLOW_DUPLICATES", build_pyramids="NO_PYRAMIDS", calculate_statistics="NO_STATISTICS", build_thumbnails="NO_THUMBNAILS", operation_description="#", force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE")
            Utility.addToolMessages()
            
            arcpy.SetMosaicDatasetProperties_management(master_md_path, rows_maximum_imagesize="25000", columns_maximum_imagesize="25000",
                                                      allowed_compressions="LERC;JPEG;None;LZ77", default_compression_type="LERC", JPEG_quality="75",
                                                      LERC_Tolerance="0.001", resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP",
                                                      footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA", clip_to_boundary="NOT_CLIP",
                                                      color_correction="NOT_APPLY", allowed_mensuration_capabilities="#",
                                                      default_mensuration_capabilities="NONE",
                                                      allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None",
                                                      default_mosaic_method="ByAttribute", order_field=CMDRConfig.PROJECT_DATE, order_base="3000",
                                                      sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width="0", view_point_x="600",
                                                      view_point_y="300", max_num_per_mosaic="40", cell_size_tolerance="0.8", cell_size="{0} {0}".format(masterCellSize_meters),
                                                      metadata_level="BASIC",
                                                      transmission_fields=transmissionFields,
                                                      use_time="DISABLED", start_time_field=CMDRConfig.PROJECT_DATE, end_time_field="#", time_format="#",
                                                      geographic_transform="#",
                                                      max_num_of_download_items="40", max_num_of_records_returned="2000",
                                                      data_source_type="ELEVATION", minimum_pixel_contribution="1", processing_templates="None",
                                                      default_processing_template="None")
            Utility.addToolMessages()
            
#             arcpy.SetMosaicDatasetProperties_management(in_mosaic_dataset="C:/temp/MDMaster/MDMaster_DSM.gdb/DSM", rows_maximum_imagesize="25000", columns_maximum_imagesize="25000", allowed_compressions="None;JPEG;LZ77;LERC", default_compression_type="None", JPEG_quality="75", LERC_Tolerance="0.001", resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP", footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA", clip_to_boundary="NOT_CLIP", color_correction="NOT_APPLY", allowed_mensuration_capabilities="#", default_mensuration_capabilities="NONE", allowed_mosaic_methods="ByAttribute;NorthWest;Center;LockRaster;Nadir;Viewpoint;Seamline;None", default_mosaic_method="ByAttribute", order_field="Project_Date", order_base="3000", sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width="0", view_point_x="600", view_point_y="300", max_num_per_mosaic="40", cell_size_tolerance="0.8", cell_size="1 1", metadata_level="BASIC", transmission_fields="Name;MinPS;MaxPS;LowPS;HighPS;Tag;GroupName;ProductName;CenterX;CenterY;ZOrder;Shape_Length;Shape_Area;Project_ID;Project_Date;Porject_Source;Project_SR_XY;Project_SR_XY_Units;Project_SR_XY_Code;Project_SR_Z_Units", use_time="DISABLED", start_time_field="Project_Date", end_time_field="", time_format="", geographic_transform="", max_num_of_download_items="40", max_num_of_records_returned="2000", data_source_type="ELEVATION", minimum_pixel_contribution="1", processing_templates="None", default_processing_template="None")
            # set statistics Min = -300 and Max = 2000M
            # set nodata = default no data value
            arcpy.SetRasterProperties_management(master_md_path, data_type="ELEVATION", statistics="1 0 2000 # #", stats_file="#", nodata="1 {}".format(RasterConfig.NODATA_DEFAULT))
            Utility.addToolMessages()
            
            arcpy.RemoveRastersFromMosaicDataset_management(in_mosaic_dataset=master_md_path, where_clause="1=1", update_boundary="UPDATE_BOUNDARY", mark_overviews_items="MARK_OVERVIEW_ITEMS", delete_overview_images="DELETE_OVERVIEW_IMAGES", delete_item_cache="DELETE_ITEM_CACHE", remove_items="REMOVE_MOSAICDATASET_ITEMS", update_cellsize_ranges="UPDATE_CELL_SIZES")
            Utility.addToolMessages()
                        
        else:
            arcpy.AddWarning("Master Mosaic Dataset already exists: {0}. Cannot continue".format(master_md_path))
    else:
        arcpy.AddError("Master Geodatabase doesn't exist {0}".format(master_fgdb_path))
    
    arcpy.AddMessage("Operation complete")
  
if __name__ == '__main__':
    CreateMasterMosaicDatasets(7201)
    
# if __name__ == '__main__':
#         
#     arcpy.AddMessage(inspect.getfile(inspect.currentframe()))
#     arcpy.AddMessage(sys.version)
#     arcpy.AddMessage(sys.executable)
#     arcpy.AddMessage("len(sys.argv): {0}".format(len(sys.argv)))
#     executedFrom = sys.executable.upper()
#     
#     #if len(sys.argv) == 4:
#     if not ("ARCMAP" in executedFrom or "ARCCATALOG" in executedFrom or "RUNTIME" in executedFrom):
#         arcpy.AddMessage("Getting parameters from command line...")
#         master_fgdb_path = sys.argv[1]
#         arcpy.AddMessage("Master GDB\n (in which to create the Master MD): {0}".format(master_fgdb_path))
# 
#         MasterMDName = sys.argv[2]
#         arcpy.AddMessage("Master Mosaic Dataset Name to be created: {0}".format(MasterMDName))
# 
#         if MasterMDName[0].isdigit():
#             arcpy.AddError("\nExiting: The first character of the Master Mosaic Dataset name cannot be a number")
#             sys.exit(0)
# 
#         MasterBoundaryFC = sys.argv[3]
#         arcpy.AddMessage("Master Boundary Feature Class (optional): {0}".format(MasterBoundaryFC))
#     else:
#         arcpy.AddMessage("Getting parameters from GetParameterAsText...")
#         master_fgdb_path = arcpy.GetParameterAsText(0)
#         arcpy.AddMessage("Master GDB\n (in which to create the Master MD): {0}".format(master_fgdb_path))
# 
#         MasterMDName = arcpy.GetParameterAsText(1)
#         arcpy.AddMessage("Master Mosaic Dataset Name to be created: {0}".format(MasterMDName))
# 
#         if MasterMDName[0].isdigit():
#             arcpy.AddError("\nExiting: The first character of the Master Mosaic Dataset name cannot be a number")
#             sys.exit(0)
#             
#         MasterBoundaryFC = arcpy.GetParameterAsText(2)
#         arcpy.AddMessage("Master Boundary Feature Class (optional): {0}".format(MasterBoundaryFC))
#         
#     main (master_fgdb_path, MasterMDName, MasterBoundaryFC)
