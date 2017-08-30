# # Script for importing a project cached service into the master cached service
# Import system modules
import arcpy
import datetime
import os
import time

from ngce import Utility
from ngce.cmdr import CMDR
from ngce.contour import ContourConfig
from ngce.folders import ProjectFolders


def ImportContourCacheToMaster(jobID, serverConnectionFilePath, masterServiceName, update=False, runCount=0):
    cache_dir = ContourConfig.CACHE_FOLDER
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
    project, ProjectUID = ProjectJob.getProject(jobID)
    
    if project is not None:
        projectID = ProjectJob.getProjectID(project)
        
        ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
        ContourFolder = ProjectFolder.derived.contour_path
        PublishFolder = ProjectFolder.published.path
        contourMerged_Name = (ContourConfig.MERGED_FGDB_NAME).format(projectID)
        contourMerged_file_gdb_path = os.path.join(PublishFolder, contourMerged_Name)
        contourMxd_Name = ContourConfig.CONTOUR_MXD_NAME 
        contourMxd_path = os.path.join(PublishFolder, contourMxd_Name)
        ContourFC = os.path.join(contourMerged_file_gdb_path, ContourConfig.CONTOUR_FC_WEBMERC)
        ContourBoundFC = os.path.join(contourMerged_file_gdb_path, ContourConfig.CONTOUR_BOUND_FC_WEBMERC)
        projectServiceName = "{}_{}".format(projectID, ContourConfig.CONTOUR_2FT_SERVICE_NAME)  # arcpy.GetParameterAsText(3)
        projectFolder = ProjectJob.getState(project)  # arcpy.GetParameterAsText(4)
        
        # Get input parameters
        projectCache = os.path.join(ContourConfig.CACHE_FOLDER, projectServiceName, "Layers")
        if projectFolder is not None and len(projectFolder) > 0:
            projectCache = os.path.join(ContourConfig.CACHE_FOLDER, "{}_{}".format(projectFolder, projectServiceName), "Layers")  # arcpy.GetParameterAsText(0)
        areaOfInterest = ContourBoundFC  # arcpy.GetParameterAsText(1)
        serverConnectionFilePath = serverConnectionFilePath  # arcpy.GetParameterAsText(2)
        
        masterService = os.path.join(serverConnectionFilePath,   "{}_{}.MapServer".format(masterServiceName, ContourConfig.CONTOUR_2FT_SERVICE_NAME))
        if serverConnectionFilePath.endswith(".ags"):
            masterService = os.path.join(serverConnectionFilePath[:-4], "{}_{}.MapServer".format(masterServiceName, ContourConfig.CONTOUR_2FT_SERVICE_NAME))
        arcpy.AddMessage("Location of master service is: {0}".format(masterService))
        scales = ContourConfig.CONTOUR_SCALES_STRING
        
        #-------------------------------------------------------------------------------
        #-------------------------------------------------------------------------------
        # The following paths and values can be modified if needed
        
        # Other map service properties
        cachingInstances = ContourConfig.CACHE_INSTANCES  # This should be increased based on server resources
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
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        arcpy.AddMessage("Import started at: {0}".format(st))
        arcpy.ImportMapServerCache_server(
            input_service=masterService,
            source_cache_type="CACHE_DATASET",
            source_cache_dataset=projectCache,
            source_tile_package="",
            upload_data_to_server="DO_NOT_UPLOAD",
            scales=scales,
            num_of_caching_service_instances=cachingInstances,
            area_of_interest=areaOfInterest,
            overwrite="OVERWRITE"
        )
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        arcpy.AddMessage("Import completed at: {0}".format(st))




    else:
        arcpy.AddError("Failed to find project for job.")
    
    arcpy.AddMessage("Operation complete")

if __name__ == '__main__':
	jobID = sys.argv[1]
	serverConnectionFile = sys.argv[2]
	masterServiceName = sys.argv[3]
    
    # jobID = 4801
    #serverConnectionFile = "C:\\Users\\eric5946\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog\\arcgis on NGCEDEV_6080 (publisher).ags"
    #masterServiceName = "MASTER\ELEVATION_1M"   

    ImportContourCacheToMaster(jobID, serverConnectionFile, masterServiceName)
