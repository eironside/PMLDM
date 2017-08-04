# # Script for publishing an empty master contour map service
# Import system modules
import arcpy
import datetime
import os
import shutil

from ngce import Utility
from ngce.contour import ContourConfig


arcpy.env.overwriteOutput = True


def publishContourMaster(deploymentFolderPath, serverConnectionFilePath, serviceName, serviceFolder):
    serviceName = "{}_{}".format(serviceName, ContourConfig.CONTOUR_2FT_SERVICE_NAME)
    mpk = ContourConfig.EMPTY_MASTER_MPK
    cache_dir = ContourConfig.CACHE_FOLDER
    tilingScheme = ContourConfig.TILING_SCHEME
    # # Get input parameters
    # serverConnectionFilePath = arcpy.GetParameterAsText(0)
    # serviceName = arcpy.GetParameterAsText(1)
    # serviceFolder = arcpy.GetParameterAsText(2)
    Utility.printArguments(["deploymentFolderPath", "serverConnectionFilePath", "serviceName", "serviceFolder", "cache_dir", "Template MPK", "tilingScheme"],
                           [deploymentFolderPath, serverConnectionFilePath, serviceName, serviceFolder, cache_dir, mpk, tilingScheme], "B04 PublishContourMaster")
    
    
    # Find the master MPK in the current directory
#     cwd = os.path.dirname(sys.argv[0])
    temp = os.path.join(deploymentFolderPath, "temp")
    if os.path.exists(temp):
        try:
            shutil.rmtree(temp)
        except:
            pass
    if not os.path.exists(temp):
        try:
            os.mkdir(temp)
        except:
            pass
#     mpk = os.path.join(cwd + "\\emptyMaster.mpk")
    
    
    #-------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------
    # The following path can be modified if needed
    # Path to the cache serviceFolder where project tiles will be stored for this service
    cacheFolder = cache_dir  # r"C:\arcgisserver\directories\arcgiscache"
    cacheDir = os.path.join(cache_dir, serviceName)
    if serviceFolder is not None and len(serviceFolder) > 0:
        cacheDir = os.path.join(cache_dir, "{}_{}".format(serviceFolder, serviceName))
    if os.path.exists(cacheDir):
        now = datetime.datetime.now()
        updatedCacheDir = "{}_{}{}{}_{}{}{}".format(cacheDir,
                                                    ("0000{}".format(now.year))[-4:],
                                                    ("00{}".format(now.month))[-2:],
                                                    ("00{}".format(now.day))[-2:],
                                                    ("00{}".format(now.hour))[-2:],
                                                    ("00{}".format(now.minute))[-2:],
                                                    ("00{}".format(now.second))[-2:])
        arcpy.AddMessage("The existing cache folder will be moved to: {0}".format(updatedCacheDir))
        shutil.move(cacheDir, updatedCacheDir) 
    #-------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------
    
    # Construct path for master contour service
    inputService = os.path.join(serverConnectionFilePath, serviceFolder, "{}.MapServer".format(serviceName))
    if serverConnectionFilePath.endswith(".ags"):
        inputService = os.path.join(serverConnectionFilePath[:-4], serviceFolder, "{}.MapServer".format(serviceName))
    arcpy.AddMessage("Location of new service will be: {0}".format(inputService))
    
    # Extract the master map package
    arcpy.ExtractPackage_management(mpk, temp)
    
    mapDoc = os.path.join(temp, "v103", "emptyMaster.mxd")
    sddraft = os.path.join(temp, "{}.sddraft".format(serviceName))
    sd = os.path.join(temp, "{}.sd".format(serviceName))
#     tilingScheme = os.path.join(cwd + "\\NRCS_tilingScheme.xml")  # Cache template file
    
    arcpy.AddMessage("Creating Map Service Definition Draft {}".format(sddraft))
    # Create the SDDraft file for the empty master contour service
    analysis = arcpy.mapping.CreateMapSDDraft(mapDoc, sddraft, serviceName, "ARCGIS_SERVER", serverConnectionFilePath, folder_name=serviceFolder)
    
    # Check for analyzer errors
    if analysis['errors'] == {}:
        arcpy.AddMessage("Staging Map Service Definition {}".format(sd))
        arcpy.StageService_server(sddraft, sd)
        arcpy.AddMessage("Uploading Map Service Definition {} to {}".format(sd, serverConnectionFilePath))
        arcpy.UploadServiceDefinition_server(sd, serverConnectionFilePath)
        arcpy.AddMessage("Service publishing completed")
    else:
        # If the SDDraft analysis contained errors, display them
        arcpy.AddMessage("\nERROR\nThe following errors were encountered during analysis of the map document: " + str(analysis['errors']))
        os.remove(sddraft)
    
    # Create the empty cache schema for the master contour service
    arcpy.AddMessage("Creating map service cache at {}".format(cacheFolder))
    
    
    
    # List of input variables for map service properties
#     tilingSchemeType = "PREDEFINED"
#     scalesType = ""
#     tileOrigin = ""
#     numOfScales = ContourConfig.CONTOUR_SCALES_NUM
    scales = ContourConfig.CONTOUR_SCALES_STRING
#     dotsPerInch = "96"
#     tileSize = "256 x 256"
#     cacheTileFormat = "PNG"
#     tileCompressionQuality = "75"
#     storageFormat = "COMPACT"
    
    
    arcpy.CreateMapServerCache_server(input_service=inputService,
                                      service_cache_directory=cacheFolder,
                                      tiling_scheme_type="PREDEFINED",
                                      predefined_tiling_scheme=tilingScheme,
#                                       scales_type=scalesType ,
#                                       dots_per_inch=dotsPerInch,
#                                     num_of_scales=numOfScales,
#                                       tile_size=tileSize,
#                                       cache_tile_format=cacheTileFormat,
#                                       storage_format=storageFormat,
#                                       tile_compression_quality=tileCompressionQuality,
                                    scales=scales
                                      )
                                            
    
    # Clean up the Service Definition file from the temp serviceFolder
    os.remove(sd)
    
    arcpy.AddMessage("Operation complete")
    
    
if __name__ == '__main__':
    # jobID = arcpy.GetParameterAsText(0)
    deploymentFolderPath = '\\\\Ngcedev\\DAS1\\RasterData\\Elevation\\LiDar\\MASTER\\ELEVATION_1M'
    serverConnectionFilePath = '\\\\NGCEDEV.esri.com\\ArcGIS\\arcgis on NGCEDEV_6080 (publisher).ags'
    serviceName = 'ELEVATION_1M'
    serviceFolder = "Master"
    publishContourMaster(deploymentFolderPath, serverConnectionFilePath, serviceName, serviceFolder)
