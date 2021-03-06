# # Script for creating new cached map services for elevation projects

import arcpy
import datetime
import os
import shutil
import sys
import time

from ngce import Utility
from ngce.cmdr import CMDR
from ngce.contour import ContourConfig
from ngce.folders import ProjectFolders
from ngce.folders.FoldersConfig import DTM
from ngce.pmdm import RunUtil
from ngce.pmdm.a import A05_C_ConsolidateRasterInfo
import xml.dom.minidom as DOM


arcpy.env.overwriteOutput = True


def updateSDDraft(sddraftPath, outsddraft, update=False):
##    sddraftPath = sddraftPath.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN
##    outsddraft = outsddraft.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN
    Utility.printArguments(["sddraftPath", "outsddraft", 'update'], [sddraftPath, outsddraft, update], "C03 Update SD Draft XML")

    newAntialiasingMode = "Fast"

    xml = sddraftPath
    dom = DOM.parse(xml)
    keys = dom.getElementsByTagName('Key')
    arcpy.AddMessage("Editing minInstances setting in service definition draft file...")
    for key in keys:
        # Set the min and max instances
        if key.firstChild.data == 'MinInstances': key.nextSibling.firstChild.data = 0
        elif key.firstChild.data == 'MaxInstances': key.nextSibling.firstChild.data = ContourConfig.CACHE_INSTANCES

        # Set the antialiasing mode to 'Fast'
        if key.hasChildNodes():
            if key.firstChild.data == 'antialiasingMode':
                # Modify the antialiasing mode
                arcpy.AddMessage("Updating anti-aliasing to: {}".format(newAntialiasingMode))
                key.nextSibling.firstChild.data = newAntialiasingMode


    if update:
        arcpy.AddMessage("Changing publish from CREATE to UPDATE service...")
        tagsType = dom.getElementsByTagName('Type')
        for tagType in tagsType:
            if tagType.parentNode.tagName == 'SVCManifest':
                if tagType.hasChildNodes():
                    tagType.firstChild.data = "esriServiceDefinitionType_Replacement"

        tagsState = dom.getElementsByTagName('State')
        for tagState in tagsState:
            if tagState.parentNode.tagName == 'SVCManifest':
                if tagState.hasChildNodes():
                    tagState.firstChild.data = "esriSDState_Published"


    # Save a new SDDraft file
    f = open(outsddraft, 'w')
    dom.writexml(f)
    f.close()

def processJob(ProjectJob, project, ProjectUID, serverConnectionFile):
    cache_path = ContourConfig.CACHE_FOLDER

    projectID = ProjectJob.getProjectID(project)
    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    derived_filegdb_path = ProjectFolder.derived.fgdb_path
    contour_folder = ProjectFolder.derived.contour_path
#     PublishFolder = ProjectFolder.published.path
#     contour_file_gdb_path = os.path.join(contour_folder, CONTOUR_GDB_NAME)
#     contourMerged_file_gdb_path = os.path.join(PublishFolder, CONTOUR_NAME_WM)
    # @TODO: move all the derived contour stuff to a published location
    # P:\OK_SugarCreekElaine_2006\DERIVED\CONTOUR\SCRATCH\RESULTS\Results.mxd
    contourMxd_Name = "Results.mxd"  # ContourConfig.CONTOUR_MXD_NAME
    contourMxd_path = os.path.join(contour_folder, "C02Scratch", "RESULTS", contourMxd_Name)
#     ContourBoundFC = os.path.join(contourMerged_file_gdb_path, ContourConfig.CONTOUR_BOUND_FC_WEBMERC)
    ContourBoundFC = A05_C_ConsolidateRasterInfo.getRasterBoundaryPath(derived_filegdb_path, DTM)

    temp = os.path.join(contour_folder, "temp")
    if os.path.exists(temp):
        shutil.rmtree(temp)
    os.mkdir(temp)

    # Get input parameters
    mxd = contourMxd_path #.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN  # arcpy.GetParameterAsText(0)
    ## 2018051 EI: Switched to using envelope here to create all cache tiles. Use AOI for import in C04
    #areaOfInterest = ContourBoundFC  # arcpy.GetParameterAsText(1)
    areaOfInterest = ""
    updateExtents = ContourBoundFC #.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN  # arcpy.GetParameterAsText(1)

    localServer = serverConnectionFile  # arcpy.GetParameterAsText(2)

    serviceName = "{}_{}".format(projectID, ContourConfig.CONTOUR_2FT_SERVICE_NAME)  # arcpy.GetParameterAsText(3)
    folder = ProjectJob.getState(project)  # arcpy.GetParameterAsText(4)

    # Using the temp folder to create service definition files
    sddraft = os.path.join(temp , "{}.sddraft".format(serviceName)) #.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN
    sd = os.path.join(temp , "{}.sd".format(serviceName)) #.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN

    tilingScheme = ContourConfig.TILING_SCHEME #.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN  # cwd + "\\NRCS_tilingScheme.xml" #Cache template file

    #-------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------
    # The following paths and values can be modified if needed

    # Path to the local cache folder where project tiles will be created
#     cacheFolder = cache_path  # r"C:\arcgisserver\directories\arcgiscache"
#     cacheDir = os.path.join(cache_path, serviceName)
#     if folder is not None and len(folder) > 0:
#         cacheDir = os.path.join(cache_path, "{}_{}".format(folder, serviceName))
#     if os.path.exists(cacheDir):
#         now = datetime.datetime.now()
#         updatedCacheDir = "{}_{}{}{}_{}{}{}".format(cacheDir,
#                                                     ("0000{}".format(now.year))[-4:],
#                                                     ("00{}".format(now.month))[-2:],
#                                                     ("00{}".format(now.day))[-2:],
#                                                     ("00{}".format(now.hour))[-2:],
#                                                     ("00{}".format(now.minute))[-2:],
#                                                     ("00{}".format(now.second))[-2:])
#         arcpy.AddMessage("The existing cache folder will be moved to: {0}".format(updatedCacheDir))
#         shutil.move(cacheDir, updatedCacheDir)

    # Other map service properties
    cachingInstances = ContourConfig.CACHE_INSTANCES  # This should be increased based on server resources
    #-------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------
    Utility.printArguments(
        ["mxd",
         "areaOfInterest",
         "updateExtents",
         "serviceName",
         "folder",
         "sddraft",
         "sd",
         "tilingScheme",
         "cache_path"],
        [mxd,
         areaOfInterest,
         updateExtents,
         serviceName,
         folder,
         sddraft,
         sd,
         tilingScheme,
         cache_path],
        "C03 CreateContourCache"
    )
    # List of scales to create tiles at. If additional scales are needed, the tiling
    # scheme file needs to be updated as well as this list
    scales = ContourConfig.CONTOUR_SCALES_STRING

    # Other map service properties that should not be modified
    updateMode = "RECREATE_ALL_TILES"  # @TODO: Can we change this to recreate missing?
    waitForJobCompletion = "WAIT"  # @TODO: What if we don't wait??
    cache_dir_path = os.path.join(cache_path, "{}_{}".format(folder, serviceName))
    if os.path.exists(cache_dir_path):
        arcpy.AddMessage("Cache directory already exists, only recreating empty tiles: {0}".format(cache_dir_path))
        updateMode = "RECREATE_EMPTY_TILES"
        waitForJobCompletion = "DO_NOT_WAIT"



    # Construct path for local cached service
    inputService = os.path.join(localServer, folder, serviceName + ".MapServer")
    if localServer.endswith(".ags"):
        inputService = os.path.join(localServer[:-4], folder, serviceName + ".MapServer")
    arcpy.AddMessage("Location of new service will be: {0}".format(inputService))

    # Create a MapDocument object from the input MXD
    mapDoc = arcpy.mapping.MapDocument(mxd)

    # Create the SDDraft file for the local cached service
    arcpy.AddMessage("Creating draft service definition: {0}".format(sddraft))
    arcpy.mapping.CreateMapSDDraft(
        mapDoc,
        sddraft,
        serviceName,
        "ARCGIS_SERVER",
        localServer,
        folder_name=folder
    )


#     # Parse the SDDraft file in order to modify service properties before publishing
#     doc = DOM.parse(sddraft)
#     # Set the antialiasing mode to 'Fast'
#     newAntialiasingMode = "Fast"
#     keys = doc.getElementsByTagName('Key')
#     for key in keys:
#         if key.hasChildNodes():
#             if key.firstChild.data == 'antialiasingMode':
#                 # Modify the antialiasing mode
#                 arcpy.AddMessage("Updating anti-aliasing to: {}".format(newAntialiasingMode))
#                 key.nextSibling.firstChild.data = newAntialiasingMode
#
#     # Save a new SDDraft file
    outsddraft = os.path.join(temp + "\\" + serviceName + "_aa.sddraft")
#     f = open(outsddraft, 'w')
#     doc.writexml(f)
#     f.close()
    updateSDDraft(sddraft, outsddraft)

    # Analyze the SDDraft file
    arcpy.AddMessage("Analyzing draft service definition: {}".format(outsddraft))
    analysis = arcpy.mapping.AnalyzeForSD(outsddraft)

    # Check for analyzer errors
    if analysis['errors'] == {}:

        RunUtil.runTool(r'ngce\pmdm\c\C03_B_StageSD.py', [outsddraft, sd, localServer], bit32=True, log_path=ProjectFolder.derived.path)
# #        arcpy.AddMessage("Staging service definition {}".format(sd))
# #        arcpy.StageService_server(outsddraft, sd)
# #        arcpy.AddMessage("Uploading service definition {} to server {}".format(sd, localServer))
# #        arcpy.UploadServiceDefinition_server(sd, localServer)
# #        arcpy.AddMessage("Service publishing completed")
    else:
        # If the SDDraft analysis contained errors, display them
        arcpy.AddError("\nERROR\nErrors encountered during analysis of the MXD: " + str(analysis['errors']))
        os.remove(sddraft)
        os.remove(outsddraft)
        raise Exception("\nERROR\nErrors encountered during analysis of the MXD: " + str(analysis['errors']))

    try:
        # Create the cache schema for the local project service
        arcpy.AddMessage("Creating cache schema for service {} in: {}".format(inputService, cache_path))
        arcpy.CreateMapServerCache_server(
            inputService,
            cache_path,
            "PREDEFINED",
            predefined_tiling_scheme=tilingScheme,
            scales=scales
        )  # , scales_type="STANDARD", num_of_scales=len(scales))
        arcpy.AddMessage("Cache schema created for local project service")
    except arcpy.ExecuteError:
        arcpy.AddWarning(arcpy.GetMessages(2))

    # Create the cache tiles for the local project service
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    arcpy.AddMessage("Cache creation started at: {0}".format(st))
    Utility.printArguments(
        ["inputService",
        "scales",
        "updateMode",
        "cachingInstances",
        "areaOfInterest",
        "updateExtents",
        "waitForJobCompletion"],
        [inputService,
        scales,
        updateMode,
        cachingInstances,
        areaOfInterest,
        updateExtents,
        waitForJobCompletion],
        'arcpy.ManageMapServerCasheTiles_server') #Added 16 April 2016 BJN
    arcpy.ManageMapServerCacheTiles_server(
        inputService,
        scales,
        updateMode,
        cachingInstances,
        areaOfInterest,
        updateExtents,
        waitForJobCompletion
    )
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    arcpy.AddMessage("Cache creation completed at: {0}".format(st))

    # Clean up the Service Definition file from the temp folder
    os.remove(sd)

def CreateContourCache(jobID, serverConnectionFile):

    Utility.printArguments(
        ["WMX Job ID",
         "serverConnectionFile"],
        [jobID,
         serverConnectionFile],
        "C03 CreateContourCache")

    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)

    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)  # @UnusedVariable

    if project is not None:
        processJob(ProjectJob, project, ProjectUID, serverConnectionFile)


    else:
        arcpy.AddError("Failed to find project for job.")

    arcpy.AddMessage("Operation complete")

if __name__ == '__main__':
    if len(sys.argv) > 2:
        jobID = sys.argv[1]
        serverConnectionFile = sys.argv[2] #.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN
    else:
        jobID = 16402
        serverConnectionFile = None

    CreateContourCache(jobID, serverConnectionFile)


#    jobID = 4801
#    serverConnectionFile = "C:\\Users\\eric5946\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog\\arcgis on NGCEDEV_6080 (publisher).ags"
#    UID = None  # field_ProjectJob_UID
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
#
#    ProjectJob = ProjectJob()
#    project = [
#               UID,  # field_ProjectJob_UID
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
#
#
#    processJob(ProjectJob, project, UID, serverConnectionFile)
