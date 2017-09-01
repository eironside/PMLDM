'''
Created on Dec 14, 2015

@author: eric5946

master_md_path =  Master Mosaic Dataset
ProjectMDs = Project Mosaic Dataset(s)

'''
#-------------------------------------------------------------------------------
# Name:        NRCS_AddProjectDataToMaster.py
#
# Purpose:     To 
#
# Author:       Roslyn Dunn
# Organization: Esri Inc.
#
# Created:     01/13/2015
#
# Last Edited: 05/04/15
#                Ingest multiple projects into Master
#                Assume Project MD is in Web Mercator. Because Project rows
#                are already in Web Mercator, use the overviews directly from
#                the Project MD. Rebuild Overviews at low scale only.
# *
#-------------------------------------------------------------------------------
import arcpy
from datetime import datetime
import os

from ngce import Utility
from ngce.Utility import doTime
from ngce.cmdr import CMDR, CMDRConfig
from ngce.cmdr.CMDR import getProjectFromWMXJobID
from ngce.folders import ProjectFolders, FoldersConfig


# arcpy.env.parallelProcessingFactor = "0"
def DefineBuildOverviews (cellsizeOVR, MasterMD, MasterMD_overview_path, AreaToBuildOVR):
    arcpy.AddMessage("\nCell size of First level Overview:  {0}".format(cellsizeOVR))
    
    # Define overviews
    # pixel size of the first level overview is cellsizeOVR
    # in_template_dataset=AreaToBuildOVR (this can be just the extent of the project, but
    #         for now this is the extent of the entire Master Mosaic Dataset)
    # overview_factor = 2
    # compression method = "LZW"
    # Sets the location of Mosaic Dataset overview TIFF files
    #  (Note: this folder needs to be in the ArcGIS Server Data Store)

    arcpy.DefineOverviews_management(MasterMD, MasterMD_overview_path, AreaToBuildOVR, extent="#", pixel_size=cellsizeOVR,
                                     number_of_levels="#", tile_rows="5120", tile_cols="5120", overview_factor="2",
                                     force_overview_tiles="NO_FORCE_OVERVIEW_TILES", resampling_method="BILINEAR",
                                     compression_method="LZW", compression_quality="100")   
    messages = arcpy.GetMessages()
    arcpy.AddMessage("\nResults output from DefineOverviews are: \n{0}\n".format(messages))
   
    whereClauseOVR = "#"
    
    arcpy.BuildOverviews_management(MasterMD, whereClauseOVR, define_missing_tiles="NO_DEFINE_MISSING_TILES",
                                    generate_overviews="GENERATE_OVERVIEWS", generate_missing_images="GENERATE_MISSING_IMAGES",
                                    regenerate_stale_images="IGNORE_STALE_IMAGES")
#                                    regenerate_stale_images="REGENERATE_STALE_IMAGES")
    messages = arcpy.GetMessages()
    arcpy.AddMessage("\nResults output from BuildOverviews are: \n{0}\n".format(messages))

    # Get another record count from the Master MD 
    result = arcpy.GetCount_management(MasterMD)
    countMasterRastersOVR = int(result.getOutput(0))

    arcpy.AddMessage("After Building Overviews Master Mosaic Dataset: {0} has {1} row(s).".format(MasterMD, countMasterRastersOVR))

    return


def processJob(ProjectJob, project, ProjectUID, masterParentDir, masterService):
    masterServiceFolder = None
    masterName = masterService
    index = masterService.find("/")
    if index < 0:
        index = masterService.find("\\")
    
    if index >= 0:
        masterServiceFolder = masterService[0:index]
        masterName = masterService[index + 1:]
    
    master_md_name = masterName  # RasterConfig.MASTER_MD_NAME
    
    Utility.printArguments(["Master Folder", "Master Name", "Master MD Name"],
                           [masterServiceFolder, masterName, master_md_name], "A08 AddPrjectToMaster")
    
    
    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
#         projectID = ProjectJob.getProjectID(project)
        
        
    ProjectMDs_fgdb_path = ProjectFolder.published.fgdb_path
    arcpy.AddMessage("Project file GDB Path: {}".format(ProjectMDs_fgdb_path))
        
    md_list = [FoldersConfig.DTM, FoldersConfig.DSM, FoldersConfig.DLM, FoldersConfig.DHM, FoldersConfig.INT]
          
    # Ensure the master_md_path exists
    for md_name in md_list:
            
        projectMD_path = os.path.join("{}_{}.gdb".format(ProjectMDs_fgdb_path[:-4], md_name),md_name)
        arcpy.AddMessage("Project {} Mosaic Dataset Path: {}".format(md_name, projectMD_path))
        
        if arcpy.Exists(projectMD_path):
            master_md_path = os.path.join(masterParentDir, masterService, "{}_{}.gdb".format(masterName, md_name), md_name)
            arcpy.AddMessage("Master {} MD Path: {}".format(md_name, master_md_path))
            if arcpy.Exists(master_md_path):
#                     project_md_path = os.path.join(ProjectMDs_fgdb_path, md_name)
#                     if arcpy.Exists(project_md_path):
                
                    # Get a record count from the Master MD 
                result = arcpy.GetCount_management(master_md_path)
                countMasterRasters = int(result.getOutput(0))
                arcpy.AddMessage("Before ingest Master Mosaic Dataset: {0} has {1} row(s)".format(master_md_path, countMasterRasters))
                    
                    # #      # Get the maximum value of ItemTS From the Master Mosaic Dataset
                    # #      #  The value of ItemTS is based on the last time the row was modified. Knowing
                    # #      #  the current maximum value of ItemTS in the Master will help us determine which rows were
                    # #      #  added as a result of the subsequent call to "Add Raster"
                    # #      if countMasterRasters > 0:
                    # #          fc = r"in_memory/MaxItemTS"
                    # #          arcpy.Statistics_analysis(master_md_path,fc,statistics_fields="ItemTS MAX",case_field="#")
                    # #
                    # #          fields = ['MAX_ITEMTS']
                    # #          with arcpy.da.SearchCursor(fc, fields) as cursor:
                    # #              for row in cursor:
                    # #                  MaxItemTSValue = float(row[0])
                    # #      else:
                    # #          MaxItemTSValue = 0.0
                    # #
                    # #      arcpy.AddMessage("Maximum value for ItemTS before adding Project MD rows to Master:       {0}".format(MaxItemTSValue))
                        
#                         project_md_path = project_md_path.strip("'")
#                         # Ensure the project_md_path exists
#                         if not arcpy.Exists(project_md_path):
#                             arcpy.AddError("\nExiting: Project Mosaic Dataset doesn't exist: {0}".format(project_md_path))
#                             continue
                
                    # Get a record count from the Project MD just to be sure we have data to ingest
                result = arcpy.GetCount_management(projectMD_path)
                countProjRasters = int(result.getOutput(0))
            
                if countProjRasters > 0:
                    arcpy.AddMessage("{0} has {1} raster product(s).".format(projectMD_path, countProjRasters))
        
                    # Gather project_md_path metadata such as spatial reference and cell size
                    descProjectMD = arcpy.Describe(projectMD_path)
                    descProjectMDSR = descProjectMD.SpatialReference
                    ProjectMDSpatialRef = descProjectMD.SpatialReference.exportToString()
                    arcpy.AddMessage("Ingesting: {0}".format(projectMD_path))
                    # arcpy.AddMessage("Spatial reference of the Project MD is: \n\n{0}\n".format(ProjectMDSpatialRef))
                    # arcpy.AddMessage("Length of SR string is {0}:".format(len(ProjectMDSpatialRef)))
            
                    # Ensure the project_md_path is 1-band 32-bit floating point (i.e. is an elevation raster)
                    bandCountresult = arcpy.GetRasterProperties_management(projectMD_path, property_type="BANDCOUNT", band_index="")
                    bandCount = int(bandCountresult.getOutput(0))
                    if bandCount == 1:
            
                        bitDepthresult = arcpy.GetRasterProperties_management(projectMD_path, property_type="VALUETYPE", band_index="")
                        bitDepth = int(bitDepthresult.getOutput(0))
                        if bitDepth == 9:
                            
                            # Determine the cell size of the Project Mosaic Dataset 
                            cellsizeResult = arcpy.GetRasterProperties_management(projectMD_path, property_type="CELLSIZEX", band_index="")
                            cellsize = float(cellsizeResult.getOutput(0))
                            arcpy.AddMessage("Cell size of Project MD:  {0} {1}".format(cellsize, descProjectMDSR.linearUnitName))
                            
                            # Add the rows from the Project MD to the Master MD using the
                            # Table raster type, and don't update the cell size ranges or the boundary
                            raster_type = "Table"          
                            arcpy.AddRastersToMosaicDataset_management(master_md_path, raster_type, projectMD_path, update_cellsize_ranges="NO_CELL_SIZES",
                                                                       update_boundary="NO_BOUNDARY", update_overviews="NO_OVERVIEWS",
                                                                       maximum_pyramid_levels="#", maximum_cell_size="0", minimum_dimension="1500",
                                                                       spatial_reference=ProjectMDSpatialRef, filter="#", sub_folder="NO_SUBFOLDERS",
                                                                       duplicate_items_action="ALLOW_DUPLICATES", build_pyramids="NO_PYRAMIDS",
                                                                       calculate_statistics="NO_STATISTICS", build_thumbnails="NO_THUMBNAILS",
                                                                       operation_description="#", force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE")
                            Utility.addToolMessages()
#                                 messages = arcpy.GetMessages()
#                                 arcpy.AddMessage("\nResults output from AddRastersToMosaicDataset are: \n{0}\n".format(messages))
                        
                                # Get another record count from the Master MD 
                            result = arcpy.GetCount_management(master_md_path)
                            countMasterRasters = int(result.getOutput(0))
                            arcpy.AddMessage("After ingest Master Mosaic Dataset: {0} has {1} row(s)".format(master_md_path, countMasterRasters))
                        
                            # NOTE: The following section is commented, as setting Category to 2 for overviews created on the project_md_path doesn't work well
                            # #      # Reset Category to 2 for all overview records ingested from the Project MD (for some reason
                            # #      #   the table raster type sets all rows to Category of 1).
                            # #      where_clause = "ItemTS > " + str(MaxItemTSValue) + " AND UPPER(Name) LIKE 'OV_%'"
                            # #      arcpy.AddMessage("Mosaic Layer where clause: {0}".format(where_clause))
                            # #      arcpy.MakeMosaicLayer_management(master_md_path,"MasterMDLayer",where_clause,template="#",band_index="#",
                            # #                                       mosaic_method="BY_ATTRIBUTE",order_field="ProjectDate",order_base_value="3000",
                            # #                                       lock_rasterid="#",sort_order="ASCENDING",mosaic_operator="LAST",cell_size="1")
                            # #    
                            # #      messages =  arcpy.GetMessages()
                            # #      arcpy.AddMessage("\nResults output from MakeMosaicLayer are: \n{0}\n".format(messages))
                            # #
                            # #      arcpy.CalculateField_management("MasterMDLayer", field="Category", expression="2", expression_type="VB", code_block="")
                            # #      messages =  arcpy.GetMessages()
                            # #      arcpy.AddMessage("\nResults output from CalculateField are: \n{0}\n".format(messages))
                    
                            # Build the boundary
                            # NOTE: if the boundary has been set to a large shape, then APPEND should have no effect
                            #       on the existing boundary
                            arcpy.BuildBoundary_management(master_md_path, where_clause="", append_to_existing="APPEND", simplification_method="NONE")
                            
                            messages = arcpy.GetMessages()
                            arcpy.AddMessage("\nResults output from BuildBoundary are: \n{0}\n".format(messages))
                            
                            # set mosaic properties on the master *AGAIN* to ensure that clip to footprint doesn't get re-set
                            #  Clip to footprint is somehow getting reset in 10.3.  It should be set so that footprints are NOT clipping data (NOT_CLIP)
                            transmissionFields = "Name;LowPS;CenterX;CenterY;ProjectID;ProjectDate;ProjectSrs;ProjectSrsUnits;ProjectSrsUnitsZ;ProjectSource;PCSCode"
                            arcpy.AddMessage("\ntransmissionFields: {0}".format(transmissionFields))
                        
                            arcpy.SetMosaicDatasetProperties_management(master_md_path, rows_maximum_imagesize="25000", columns_maximum_imagesize="25000",
                                                                          allowed_compressions="LERC;JPEG;None;LZ77", default_compression_type="LERC", JPEG_quality="75",
                                                                          LERC_Tolerance="0.001", resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP",
                                                                          footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA", clip_to_boundary="NOT_CLIP",
                                                                          color_correction="NOT_APPLY", allowed_mensuration_capabilities="#",
                                                                          default_mensuration_capabilities="NONE",
                                                                          allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None",
                                                                          default_mosaic_method="ByAttribute", order_field=CMDRConfig.PROJECT_DATE, order_base="3000",
                                                                          sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width="0", view_point_x="600",
                                                                          view_point_y="300", max_num_per_mosaic="40", cell_size_tolerance="0.8", cell_size="1 1",
                                                                          metadata_level="BASIC",
                                                                          transmission_fields=transmissionFields,
                                                                          use_time="DISABLED", start_time_field=CMDRConfig.PROJECT_DATE, end_time_field="#", time_format="#",
                                                                          geographic_transform="#",
                                                                          max_num_of_download_items="40", max_num_of_records_returned="2000",
                                                                          data_source_type="ELEVATION", minimum_pixel_contribution="1", processing_templates="None",
                                                                          default_processing_template="None")
                            
                            messages = arcpy.GetMessages()
                            arcpy.AddMessage("\nResults output from SetMosaicDatasetProperties are: \n{0}\n".format(messages))
                        
                        
                            # define the location of the mosaic dataset overviews
                            loc = master_md_path.rfind(".gdb")
                            # arcpy.AddMessage("loc = {0}".format(loc))
                            # MasterMD_overview_path = master_md_path[:loc] + r".Overviews" + master_md_path[loc+4:]
                            MasterMD_overview_path = master_md_path[:loc] + r".Overviews"
                            arcpy.AddMessage("Mosaic Dataset Overview Location: {0}".format(MasterMD_overview_path))
                        
                            # Define and Build overviews 
                                 
                            # Begin building service overviews at low scale (305.74811 Meters)
                            
                            cellsizeOVR = 305.74811
                            
                            DefineBuildOverviews (cellsizeOVR, master_md_path, MasterMD_overview_path, "#")
                        else:
                            arcpy.AddWarning("\nProject Mosaic bit depth is not 32-bit Floating Point. Ingoring mosaic dataset.")
                    else:
                        arcpy.AddWarning("Project band count is not 1 (expecting single band elevation data). Ingoring mosaic dataset.")
                else:
                    arcpy.AddWarning("Count of rasters in project mosaic dataset is 0. Please add some rasters to the project.")
#                     else:
#                         arcpy.AddWarning("Project Mosaic Dataset path is not found '{}'. Please create it before proceeding.".format(project_md_path))
            else:
                arcpy.AddError("Master Mosaic Dataset path is not found '{}'. Please create it before proceeding.".format(master_md_path))
        else:
            arcpy.AddWarning("Project Mosaic Dataset path is not found '{}'. Please create it before proceeding.".format(projectMD_path))
        # For loop
    
    


def AddProjectToMaster(strJobId, masterParentDir, masterService):
    aa = datetime.now()
    Utility.printArguments(["WMX Job ID", "masterParentDir", "masterService"],
                           [strJobId, masterParentDir, masterService], "A08 AddPrjectToMaster")
    
    ProjectJob, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable
    
    processJob(ProjectJob, project, ProjectUID)
    
    doTime(aa, "Operation Complete: A06 Publish Mosaic Dataset")


if __name__ == '__main__':
     jobID = sys.argv[1]
     MasterMDs_parent_path = sys.argv[2]
     master_md_name = sys.argv[3] 
 
     AddProjectToMaster(jobID, MasterMDs_parent_path, master_md_name)

#    MasterMDs_parent_path = "\\\\ngcedev\DAS1\RasterData\Elevation\LiDar"
#    master_md_name = "MASTER\ELEVATION_1M"
#    ProjectUID = None  # field_ProjectJob_UID
#    wmx_job_id = 1
#    project_Id = "OK_SugarCreek_2008"
#    alias = "Sugar Creek"
#    alias_clean = "SugarCreek"
#    state = "OK"
#    year = 2008
#    parent_dir = r"E:\NGCE\RasterDatasets"
#    archive_dir = r"E:\NGCE\RasterDatasets"
#    project_dir = r"E:\NGCE\RasterDatasets\OK_SugarCreek_2008"
#    project_AOI = None
#    ProjectJob = CMDR.ProjectJob()
#    project = [
#               ProjectUID,  # field_ProjectJob_UID
#               wmx_job_id,  # field_ProjectJob_WMXJobID,
#               project_Id,  # field_ProjectJob_ProjID,
#               alias,  # field_ProjectJob_Alias
#               alias_clean,  # field_ProjectJob_AliasClean
#               state ,  # field_ProjectJob_State
#               year ,  # field_ProjectJob_Year
#               parent_dir,  # field_ProjectJob_ParentDir
#               archive_dir,  # field_ProjectJob_ArchDir
#               project_dir,  # field_ProjectJob_ProjDir
#               project_AOI  # field_ProjectJob_SHAPE
#               ]
#    processJob(ProjectJob, project, ProjectUID, MasterMDs_parent_path, master_md_name)
