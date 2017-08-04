'''
Created on Dec 14, 2015

@author: eric5946

serverConnectionFile 
mdPath 
serviceName 
startupType 
enableDownload 
folderName = 
ssFunctions = 
serviceDescription = 
serviceTags = 
'''
#-------------------------------------------------------------------------------
# Name: NRCS_PublishMosaicDataset.py
#
# Purpose: Publish Image services 
#
#          Requires an existing ArcGIS Server Connection
#
# Author: Roslyn Dunn
# Organization: Esri Inc.
#
# Created: 04/23/2015
# Updated: 04/20/2015
#          Don't publish MD if RasterPath is in allowed fields (security issue)
#          Service isn't started by default
#          Add 'Elevation' to Item Description
# *
#-------------------------------------------------------------------------------
import arcpy
import os

from ngce import Utility
from ngce.cmdr import CMDR
from ngce.folders import ProjectFolders, FoldersConfig
from ngce.raster import Raster
import xml.dom.minidom as DOM 


def updateSDServerSideFunctions(ssFunctionsLst, ssFunctionsList, sddraftPath, update=False):
    Utility.printArguments(["ssFunctionsLst", "ssFunctionsList", "sddraftPath", 'update'], [ssFunctionsLst, ssFunctionsList, sddraftPath, update], "A07 updateSDServerSideFunctions")
    
    xml = sddraftPath
    dom = DOM.parse(xml)
    keys = dom.getElementsByTagName('Key')
    arcpy.AddMessage("Editing minInstances setting in service definition draft file...")
    for key in keys:
        if key.firstChild.data == 'MinInstances': key.nextSibling.firstChild.data = 0
    
    
    
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
        
        
    if len(ssFunctionsLst) > 0:        
        arcpy.AddMessage("Editing rasterFunctions setting in service definition draft file...")
        # Add the user-supplied raster function templates
        properties = dom.getElementsByTagName('PropertySetProperty')
        for prop in properties:
            keynodes = prop.getElementsByTagName("Key")
            for keynode in keynodes:
                # Check the key-value pair which stores the raster function setting
                if keynode.firstChild.nodeValue == "rasterFunctions":
                    valnodes = prop.getElementsByTagName("Value")
                    for valnode in valnodes:
                        if valnode.firstChild == None:
                            valnode.appendChild(dom.createTextNode(ssFunctionsList))
                        else:
                            valnode.firstChild.replaceWholeText(ssFunctionsList)
        
        # REMOVED BY EI
        #                         if enableDownload:
        #                             properties = dom.getElementsByTagName('PropertySetProperty')
        #                             for prop in properties:
        #                                 keynodes = prop.getElementsByTagName("Key")
        #                                 for keynode in keynodes:
        #                                     # Check the key-value pair which stores the raster function setting
        #                                     if keynode.firstChild.nodeValue == "webCapabilities":
        #                                         valnodes = prop.getElementsByTagName("Value")
        #                                         for valnode in valnodes:
        #                                             if valnode.firstChild == None:
        #                                                 valnode.appendChild(dom.createTextNode("Image,Metadata,Catalog,Download"))
        #                                             else:
        #                                                 valnode.firstChild.replaceWholeText("Image,Metadata,Catalog,Download")
    xml_filew = open(xml, "w")
    xml_filew.write(dom.toxml())
    xml_filew.close()


def PublishMosaicDataset(jobID, serverConnectionFile, serverFunctionPath, update=False, runCount=0):
    Utility.printArguments(["jobID", "serverConnectionFile", "serverFunctionPath", "update","runCount"], [jobID, serverConnectionFile, serverFunctionPath, update, runCount], "A07 PublishMosaicDataset")
    
#     serverConnectionFile = None
    startupType = None
    
    
    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
    
    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)  # @UnusedVariable
    
    if project is not None:
        ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
        ProjectID = ProjectJob.getProjectID(project)
        ProjectState = ProjectJob.getState(project)
        ProjectYear = ProjectJob.getYear(project)
        ProjectAlias = ProjectJob.getAlias(project)
        ProjectAliasClean = ProjectJob.getAliasClean(project)
        
#         serverFunctionPath = Raster.getServerRasterFunctionsPath(jobID)
        
        ssFunctions = None
        if serverFunctionPath is not None:
            ssFunctions = Raster.getServerSideFunctions(serverFunctionPath)
        
        folderName = ProjectState
        # @TODO Add more info here!
        serviceDescription = "for project '{}' within state {} published in {}.".format(ProjectAlias, ProjectState, ProjectYear)
        serviceTags = ",".join([ProjectID, ProjectAliasClean, ProjectState, str(ProjectYear)])
        
        md_list = [FoldersConfig.DTM, FoldersConfig.DSM]
        for md_name in md_list:
            
            filegdb_name = "{}_{}.gdb".format(ProjectFolder.published.fgdb_name, md_name)
            if ProjectFolder.published.fgdb_name.endswith(".gdb"):
                filegdb_name = "{}_{}.gdb".format(ProjectFolder.published.fgdb_name[:-4], md_name)
            ProjectMDs_fgdb_path = os.path.join(ProjectFolder.published.path, filegdb_name)  
            arcpy.AddMessage("File Geodatabase Path:  {0}".format(ProjectMDs_fgdb_path))
        
          
            # Ensure the master_md_path exists
            if arcpy.Exists(ProjectMDs_fgdb_path):
            
                project_md_path = os.path.join(ProjectMDs_fgdb_path, md_name)
                arcpy.AddMessage("Mosaic Dataset Path:  {0}".format(project_md_path))
                if arcpy.Exists(project_md_path):
                    serviceName = "{}_{}".format(ProjectID, md_name) 
                    arcpy.AddMessage("Service Name:  {0}".format(serviceName))
                    # Retrieve some properties from the Mosaic Dataset to place in the tags field
                    cellsizeResult = arcpy.GetRasterProperties_management(project_md_path, property_type="CELLSIZEX", band_index="")
                    Utility.addToolMessages()
                    cellsizeX = cellsizeResult.getOutput(0)
                    
                    # Get the units of the Mosaic Dataset
                    descMD = arcpy.Describe(project_md_path)
                    SpatRefMD = descMD.SpatialReference
                    SpatRefUnitsMD = SpatRefMD.linearUnitName
                    SpatRefNameMD = SpatRefMD.name
                    arcpy.AddMessage("Spatial Reference name of Mosaic Dataset:  {0}".format(SpatRefNameMD))
                    arcpy.AddMessage("Spatial Reference X,Y Units of Mosaic Dataset: {0}".format(SpatRefUnitsMD))
                
                    # append the cellsize and units of the Mosaic Dataset to the tags
                    serviceTags = "{}, {}, {}".format(serviceTags, cellsizeX, SpatRefUnitsMD)
                    serviceDescription = "{} {} horizontal spatial reference is {} and cell size is {} {}".format(md_name, serviceDescription, SpatRefNameMD, cellsizeX, SpatRefUnitsMD)
                    
                    arcpy.AddMessage("Service Tags: {0}".format(serviceTags))
                    arcpy.AddMessage("Service description: {0}".format(serviceDescription))
                
                    # Look for RasterPath in the list of allowed fields, and if found, don't publish
                    # the mosaic dataset. Exposing the contents of RasterPath could compromise the
                    # security of the Image Service.
                    allowedFieldListMD = descMD.AllowedFields
                    arcpy.AddMessage("AllowedFields in MD Properties:  {0}".format(allowedFieldListMD))      
                    if True or "RASTERPATH;" not in allowedFieldListMD.upper():
                        
                        
                        # Create a list to manipulate server-side functions
                        # Bring Hillshade to the top of the list so it is default
                        ssFunctionsLst = list([])
                        ssFunctionsList = ""
                        if ssFunctions is not None:
                            ssFunctionsLst = ssFunctions.split(";")
                            if len(ssFunctionsLst) > 0:
                                foundHillshade = False
                                for i, s in enumerate(ssFunctionsLst):
                                    if 'HILLSHADE' in s.upper():
                                        arcpy.AddMessage("Will re-order SS Functions so Hillshade is default")
                                        foundHillshade = True
                                        break
                        
                                # if Hillshade is found then re-order the list
                                if foundHillshade:
                                    ssFunctionsLst.insert(0, ssFunctionsLst.pop(i))
                                    arcpy.AddMessage("Re-ordered SS Functions so Hillshade is default")
                                    
                                # convert the list of server-side functions into a comma delimited string
                                ssFunctionsList = ",".join(ssFunctionsLst)
                                arcpy.AddMessage("Server-side Functions: {0}\n".format(ssFunctionsList))
                        
                        # Create image service definition draft
                        arcpy.AddMessage("Creating image service definition draft file: ")
                        
                        wsPath = os.path.dirname(os.path.dirname(project_md_path))
                        sddraftPath = os.path.join(wsPath, serviceName + ".sddraft")
                        arcpy.Delete_management(sddraftPath)
                        
                        arcpy.AddMessage("\tMDPath='{}'".format(project_md_path))
                        arcpy.AddMessage("\tSDPath='{}'".format(sddraftPath))
                        arcpy.AddMessage("\tServiceName='{}'".format(serviceName))
                        arcpy.AddMessage("\tFolderName='{}'".format(folderName))
                        arcpy.AddMessage("\tSummary='{}'".format(serviceDescription))
                        arcpy.AddMessage("\tTags='{}'".format(serviceTags))
                        arcpy.CreateImageSDDraft(
                            project_md_path, sddraftPath, serviceName, "ARCGIS_SERVER",
                            connection_file_path=None, copy_data_to_server=False, folder_name=folderName,
                            summary=serviceDescription, tags=serviceTags)
                        
                        # Edit the service definition draft if user specified server-side functions 
                        #   or if user wants to enable download on the Image Service
                        
                        updateSDServerSideFunctions(ssFunctionsLst, ssFunctionsList, sddraftPath, update)        
            
                        # Analyze service definition draft
                        arcpy.AddMessage("Analyzing service definition draft file...")
                        analysis = arcpy.mapping.AnalyzeForSD(sddraftPath)
                        for key in ('messages', 'warnings', 'errors'):            
                            arcpy.AddMessage("----" + key.upper() + "---")
                            analysis_vars = analysis[key]
                            for ((message, code), data) in analysis_vars.iteritems():  # @UnusedVariable
                                msg = ("    ", message, " (CODE %i)" % code)
                                arcpy.AddMessage("".join(msg))
                    
                        if analysis['errors'] == {}:
                            arcpy.AddMessage("Staging and publishing service definition...") 
                            # StageService
                            arcpy.AddMessage("Staging sddraft file to sd file")
                            sdPath = sddraftPath.replace(".sddraft", ".sd")
                            arcpy.Delete_management(sdPath)
                            arcpy.StageService_server(sddraftPath, sdPath)
                            
                            try:
                                # UploadServiceDefinition
                                arcpy.AddMessage("Publishing mosaic data set as image service.")
                                arcpy.UploadServiceDefinition_server(sdPath, serverConnectionFile, "#", "#", "#", "#", startupType)
                            except Exception as e: 
                                if runCount < 1:
                                    PublishMosaicDataset(jobID, serverConnectionFile, True, 1)
                                    break
                                else:
                                    raise e
                        else:
                            # if the sddraft analysis contained errors, display them
                            arcpy.AddError(analysis['errors'])
                    else:        
                        arcpy.AddError("Exiting: Found 'RasterPath' in list of allowed MD fields. Please remove this field from the list before publishing.")
                        arcpy.AddError("         To remove RasterPath from the list, go to Mosaic Dataset Properties, Defaults tab, Allowed Fields...")
                else:
                    arcpy.AddWarning("Project mosaic dataset not found '{}'.".format(project_md_path))
            else:
                arcpy.AddError("Project file geodatabase not found '{}'. Please add this before proceeding.".format(ProjectMDs_fgdb_path))
        
        # FOR LOOP
        #
    else:
        arcpy.AddError("Project not found in the CMDR. Please add this to the CMDR before proceeding.")
            
    arcpy.AddMessage("Operation complete")

  
if __name__ == '__main__':
    #serverConnectionFile = "C:\\Users\\eric5946\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog\\arcgis on NGCEDEV_6080 (publisher).ags"
    jobID = sys.argv[1]
    serverConnectionFile = sys.argv[2]
    serverFunctionPath = sys.argv[3] 

    
    
    PublishMosaicDataset(jobID, serverConnectionFile, serverFunctionPath)  

# if __name__ == '__main__':
#     
#     arcpy.AddMessage(inspect.getfile(inspect.currentframe()))
#     arcpy.AddMessage(sys.version)
#     arcpy.AddMessage(sys.executable)
#     
#     executedFrom = sys.executable.upper()
#     
#     if not ("ARCMAP" in executedFrom or "ARCCATALOG" in executedFrom or "RUNTIME" in executedFrom):
#         arcpy.AddMessage("Getting parameters from command line...")
# 
#         # Read user input
#         #
#         serverConnectionFile = sys.argv[1] 
#         arcpy.AddMessage("\nServer Connection file: {0}\n".format(serverConnectionFile))
#                 
#         mdPath = sys.argv[2] 
#         arcpy.AddMessage("Mosaic Dataset: {0}\n".format(mdPath))
#         
#         serviceName = sys.argv[3] 
#         arcpy.AddMessage("Service Name: {0}\n".format(serviceName))
# 
#         startupType = sys.argv[4] 
#         arcpy.AddMessage("Startup Type: {0}\n".format(startupType))
# 
#         enableDownload = sys.argv[5] 
#         arcpy.AddMessage("Enable Download:  {0}\n".format(enableDownload))
#         
#         folderName = sys.argv[6] 
#         arcpy.AddMessage("Server Folder Name: {0}\n".format(folderName))
#         
#         ssFunctions = sys.argv[7] 
#         #arcpy.AddMessage("Server-side functions: {0}\n".format(ssFunctions))
# 
#         serviceDescription = sys.argv[8] 
#         arcpy.AddMessage("Service Description: {0}\n".format(serviceDescription))
# 
#         serviceTags = sys.argv[9] 
#     else:
#         arcpy.AddMessage("Getting parameters from GetParameterAsText...")
#         # Read user input
#         #
#         serverConnectionFile = arcpy.GetParameterAsText(0) 
#         arcpy.AddMessage("\nServer Connection file: {0}\n".format(serverConnectionFile))
#                 
#         mdPath = arcpy.GetParameterAsText(1) 
#         arcpy.AddMessage("Mosaic Dataset: {0}\n".format(mdPath))
#         
#         serviceName = arcpy.GetParameterAsText(2) 
#         arcpy.AddMessage("Service Name: {0}\n".format(serviceName))
# 
#         startupType = arcpy.GetParameterAsText(3) 
#         arcpy.AddMessage("Startup Type: {0}\n".format(startupType))
# 
#         enableDownload = arcpy.GetParameterAsText(4) 
#         arcpy.AddMessage("Enable Download:  {0}\n".format(enableDownload))
#         
#         folderName = arcpy.GetParameterAsText(5) 
#         arcpy.AddMessage("Server Folder Name: {0}\n".format(folderName))
#         
#         ssFunctions = arcpy.GetParameterAsText(6) 
#         #arcpy.AddMessage("Server-side functions: {0}\n".format(ssFunctions))
# 
#         serviceDescription = arcpy.GetParameterAsText(7) 
#         arcpy.AddMessage("Service Description: {0}\n".format(serviceDescription))
# 
#         serviceTags = arcpy.GetParameterAsText(8) 
#     main(serverConnectionFile, mdPath, serviceName, startupType, enableDownload, folderName, ssFunctions, serviceDescription, serviceTags)
#     
