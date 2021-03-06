# # Script for importing a project cached service into the master cached service
# Import system modules
import arcpy
import datetime
import os
import sys

from ngce import Utility
from ngce.Utility import doTime
from ngce.cmdr import CMDR
from ngce.contour import ContourConfig

from ngce.folders import ProjectFolders
from ngce.folders.FoldersConfig import DTM
from ngce.pmdm.a import A05_C_ConsolidateRasterInfo


def ImportContourCacheToMaster(jobID, serverConnectionFilePath, masterServiceName, update=False, runCount=0):
    a = datetime.datetime.now()
    aa = a
    cache_dir = ContourConfig.CACHE_FOLDER
    #@TODO: Remove this workaround once fix is validated on NGCE
    if serverConnectionFilePath is None or len(str(serverConnectionFilePath)) <= 1 or str(serverConnectionFilePath).lower().find("aiotxftw3gi013".lower()) < 0:
        serverConnectionFilePath = "//aiotxftw6na01data/SMB03/elevation/WorkflowManager/arcgis on aiotxftw3gi013.usda.net"
    Utility.printArguments(
        ["WMX Job ID",
         "serverConnectionFilePath",
         "cache_dir",
         "masterServiceName",
         "update",
         "runCount"],
        [jobID,
         serverConnectionFilePath,
         cache_dir,
         masterServiceName,
         update,
         runCount],
        "C04 ImportContourCacheToMaster"
    )

    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)

    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)  # @UnusedVariable

    if project is not None:
        projectID = ProjectJob.getProjectID(project)

        ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
#         con_folder = ProjectFolder.derived.contour_path
#         contour_file_gdb_path = os.path.join(con_folder, CONTOUR_GDB_NAME)
#         PublishFolder = ProjectFolder.published.path
        derived_filegdb_path = ProjectFolder.derived.fgdb_path
#         contourMerged_Name = (ContourConfig.MERGED_FGDB_NAME).format(projectID)
#         contourMerged_Name = in_cont_fc = os.path.join(contour_file_gdb_path, CONTOUR_NAME_WM)
#         contour_pub_file_gdb_path = os.path.join(PublishFolder, contourMerged_Name)
#         contourMxd_Name = ContourConfig.CONTOUR_MXD_NAME
#         contourMxd_path = os.path.join(PublishFolder, contourMxd_Name)
#         ContourFC = os.path.join(contour_pub_file_gdb_path, ContourConfig.CONTOUR_FC_WEBMERC)
#         ContourBoundFC = os.path.join(contour_pub_file_gdb_path, ContourConfig.CONTOUR_BOUND_FC_WEBMERC)ContourBoundFC = A05_C_ConsolidateRasterInfo.getRasterBoundaryPath(derived_filegdb_path, DTM)
        ContourBoundFC = A05_C_ConsolidateRasterInfo.getRasterBoundaryPath(derived_filegdb_path, DTM)

        projectServiceName = "{}_{}".format(projectID, ContourConfig.CONTOUR_2FT_SERVICE_NAME)  # arcpy.GetParameterAsText(3)
        projectFolder = ProjectJob.getState(project)  # arcpy.GetParameterAsText(4)

        # Get input parameters
        projectCache = os.path.join(ContourConfig.CACHE_FOLDER, projectServiceName, "Layers")
        if projectFolder is not None and len(projectFolder) > 0:
            projectCache = os.path.join(ContourConfig.CACHE_FOLDER, "{}_{}".format(projectFolder, projectServiceName), "Layers")  # arcpy.GetParameterAsText(0)          #YES
        areaOfInterest = ContourBoundFC  # arcpy.GetParameterAsText(1)   #YES
#         serverConnectionFilePath = serverConnectionFilePath  # arcpy.GetParameterAsText(2)

        masterService = os.path.join(serverConnectionFilePath, "{}_{}.MapServer".format(masterServiceName, ContourConfig.CONTOUR_2FT_SERVICE_NAME))  # YES
        if serverConnectionFilePath.endswith(".ags"):
            masterService = os.path.join(serverConnectionFilePath[:-4], "{}_{}.MapServer".format(masterServiceName, ContourConfig.CONTOUR_2FT_SERVICE_NAME))
        arcpy.AddMessage("Location of master service is: {0}".format(masterService))
#         scales = ContourConfig.CONTOUR_SCALES_STRING

        #-------------------------------------------------------------------------------
        #-------------------------------------------------------------------------------
        # The following paths and values can be modified if needed

        # Other map service properties
#         cachingInstances =   ContourConfig.CACHE_INSTANCES# This should be increased based on server resources
        #-------------------------------------------------------------------------------
        #-------------------------------------------------------------------------------
        Utility.printArguments(
            ["projectCache",
             "areaOfInterest",
             "projectFolder",
             "projectServiceName",
             "ContourBoundFC",
             "masterService"],
            [projectCache,
             areaOfInterest,
             projectFolder,
             projectServiceName,
             ContourBoundFC,
             masterService],
            "C04 ImportContourCacheToMaster"
        )

        # Import cache tiles from a project service into the master service
#         ts = time.time()
#         st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
#         arcpy.AddMessage("Import started at: {0}".format(st))
        a = doTime(a, "Ready to start import of '{}' into '{}'".format(projectCache, masterService))
        arcpy.ImportMapServerCache_server(
            input_service=masterService,
            source_cache_type="CACHE_DATASET",
            source_cache_dataset=projectCache,
            source_tile_package="",
            upload_data_to_server="DO_NOT_UPLOAD",
            scales=ContourConfig.CONTOUR_SCALES_STRING,
            num_of_caching_service_instances=ContourConfig.CACHE_INSTANCES,
            area_of_interest=areaOfInterest,
            overwrite="OVERWRITE"  # @TODO: Verify this is right
        )

#         arcpy.ImportMapServerCache_server(input_service="//aiotxftw6na01data/SMB03/elevation/WorkflowManager/arcgis on aiotxftw3gi013.usda.net/Master/Elevation_1M_CONT_2FT.MapServer",
#                                           source_cache_type="CACHE_DATASET",
#                                           source_cache_dataset="//aiotxftw6na01data/SMB03/elevation/LiDAR/cache/OK_OK_SugarCreekEric_2008_CONT_2FT/Layers",
#                                           source_tile_package="",
#                                           upload_data_to_server="DO_NOT_UPLOAD",
#                                           scales="9027.977411;4513.988705;2256.994353;1128.497176",
#                                           num_of_caching_service_instances="6",
#                                           area_of_interest="//aiotxftw6na01data/sql1/elevation/OK_SugarCreekEric_2008/DERIVED/OK_SugarCreekEric_2008.gdb/BoundaryLASDataset",
#                                           overwrite="OVERWRITE")
#         arcpy.ImportMapServerCache_server(input_service="//aiotxftw6na01data/SMB03/elevation/WorkflowManager/arcgis on aiotxftw3gi013.usda.net/Master/Elevation_1M_CONT_2FT.MapServer",
#                                           source_cache_type="CACHE_DATASET",
#                                           source_cache_dataset="//aiotxftw6na01data/SMB03/elevation/LiDAR/cache/OK_OK_SugarCreekEric_2008_CONT_2FT/Layers",
#                                           source_tile_package="",
#                                           upload_data_to_server="DO_NOT_UPLOAD",
#                                           scales="9027.977411;4513.988705;2256.994353;1128.497176",
#                                           num_of_caching_service_instances="6",
#                                           area_of_interest="//aiotxftw6na01data/sql1/elevation/OK_SugarCreekEric_2008/DERIVED/OK_SugarCreekEric_2008.gdb/BoundaryLASDataset",
#                                           overwrite="OVERWRITE")
        a = doTime(a, "TWO: Finished import of '{}' into '{}'".format(projectCache, masterService))
















#         ts = time.time()
#         st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
#         arcpy.AddMessage("Import completed at: {0}".format(st))
        a = doTime(a, "Import of '{}' into '{}' finished".format(projectCache, masterService))



    else:
        a = doTime(a, "Failed to find project for job.")

    doTime(aa, "Operation complete")

if __name__ == '__main__':
    jobID = sys.argv[1]
    serverConnectionFile = sys.argv[2]
    masterServiceName = sys.argv[3]

    # jobID = 4801
    # serverConnectionFile = "C:\\Users\\eric5946\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog\\arcgis on NGCEDEV_6080 (publisher).ags"
    # masterServiceName = "MASTER\ELEVATION_1M"

    ImportContourCacheToMaster(jobID, serverConnectionFile, masterServiceName)
