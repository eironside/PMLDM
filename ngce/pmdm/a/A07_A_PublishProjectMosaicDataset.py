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
from datetime import datetime
import os
import sys  # @UnusedImport

from ngce import Utility
from ngce.pmdm import RunUtil
from ngce.Utility import doTime
from ngce.cmdr.JobUtil import getProjectFromWMXJobID
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


def processJob(ProjectJob, project, ProjectUID, serverConnectionFile, serverFunctionPath, update=False, runCount=0):
    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    ProjectID = ProjectJob.getProjectID(project)
    ProjectState = ProjectJob.getState(project)
    ProjectYear = ProjectJob.getYear(project)
    ProjectAlias = ProjectJob.getAlias(project)
    ProjectAliasClean = ProjectJob.getAliasClean(project)
    startupType = "STARTED"
    Utility.printArguments(["ProjectJob", "project", "ProjectUID", "serverConnectionFile", "serverFunctionPath", "update", "runCount", "ProjectFolder", "ProjectID", "ProjectState", "ProjectYear", "ProjectAlias", "ProjectAliasClean", "startupType"],
                   [ProjectJob, project, ProjectUID, serverConnectionFile, serverFunctionPath, update, runCount, ProjectFolder, ProjectID, ProjectState, ProjectYear, ProjectAlias, ProjectAliasClean, startupType], "A07_A Publish Project")
#         serverFunctionPath = Raster.getServerRasterFunctionsPath(jobID)
    
    ssFunctions = None
    if serverFunctionPath is not None:
        ssFunctions = Raster.getServerSideFunctions(serverFunctionPath)
    
    folderName = ProjectState
    # @TODO Add more info here!
    serviceDescription = "for project '{}' within state {} published in {}.".format(ProjectAlias, ProjectState, ProjectYear)
    serviceTags = ",".join([ProjectID, ProjectAliasClean, ProjectState, str(ProjectYear)])
    
    md_list = [FoldersConfig.DTM, FoldersConfig.DSM, FoldersConfig.DLM, FoldersConfig.DHM, FoldersConfig.DCM, FoldersConfig.INT]
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
                            # Don't apply hillshade to intensity
                            if foundHillshade and md_name <> FoldersConfig.INT:
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
                        RunUtil.runTool(r'ngce\pmdm\a\A07_B_StageSD.py', [sddraftPath, sdPath, serverConnectionFile, startupType], bit32=True, log_path=ProjectFolder.derived.path)
                        # arcpy.StageService_server(sddraftPath, sdPath)
                        
# #                        try:
# #                            # UploadServiceDefinition
# #                            arcpy.AddMessage("Publishing mosaic data set as image service.")
# #                            arcpy.UploadServiceDefinition_server(sdPath, serverConnectionFile, "#", "#", "#", "#", startupType)
# #                        except Exception as e: 
# #                            if runCount < 1:
# ##                                 PublishMosaicDataset(jobID, serverConnectionFile, True, 1)
# #                                processJob(ProjectJob, project, ProjectUID, serverConnectionFile, serverFunctionPath, update=True, runCount=1)
# #                                break
# #                            else:
# #                                raise e
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


def PublishMosaicDataset(strJobId, serverConnectionFile, serverFunctionPath, update=False, runCount=0):
    aa = datetime.now()
    Utility.printArguments(["jobID", "serverConnectionFile", "serverFunctionPath", "update", "runCount"], [strJobId, serverConnectionFile, serverFunctionPath, update, runCount], "A07 PublishMosaicDataset")
    
    ProjectJob, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable

    processJob(ProjectJob, project, strUID, serverConnectionFile, serverFunctionPath)
            
    doTime(aa, "Operation Complete: A06 Publish Mosaic Dataset")

  
if __name__ == '__main__':
    jobID = sys.argv[1]
    serverConnectionFile = sys.argv[2]
    serverFunctionPath = sys.argv[3] 

    PublishMosaicDataset(jobID, serverConnectionFile, serverFunctionPath)  

#    serverConnectionFile = r"C:\Users\eric5946\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\arcgis on localhost_6080 (admin).ags"
#    serverFunctionPath = r"C:\inetpub\wwwroot\ngce\raster\ServerSide_Functions"
#
#    dateStart, dateEnd = None, None
#    dateDeliver = "04/09/1971"
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
#    processJob(ProjectJob, project, ProjectUID,serverConnectionFile, serverFunctionPath, update=False, runCount=0)
