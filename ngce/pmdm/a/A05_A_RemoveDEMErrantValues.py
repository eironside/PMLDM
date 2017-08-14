'''
Created on Dec 9, 2015

@author: eric5946

'''
#-------------------------------------------------------------------------------
# Name:        NRCS_RemoveErrantValues
#
# Purpose:     To remove any errant values in raster data.
#              This conversion is performed
#              in preparation for ingestion into a Mosaic Dataset. 
#
#              
#              This GP tool executes the "Con" Spatial Analyst tool
#               to effectively eliminate very low values which
#               have difficulty rendering in ArcGIS
#
# Author:       Roslyn Dunn
# Organization: Esri Inc.
#
#
# Created:     06/25/2015
# Modified     09/24/2015   Take two values of NoData into account
#                           Get threshold values from UI
#
# *
#-------------------------------------------------------------------------------
# def processRastersInFolder(minZ, maxZ, InputFolder, OutputFolder, elevation_type, rows, ProjectID, ProjectUID):
#     count = 0
#     cellSize = 0
#     if os.path.exists(InputFolder):
#         SRFactoryCodeFlag = 1
#         arcpy.env.workspace = InputFolder
#         current_raster_list = arcpy.ListRasters("*", "ALL")
#         if current_raster_list is not None and len(current_raster_list) > 0:
#             Utility.clearFolder(OutputFolder);
#             
#             for curr_raster in current_raster_list:
#                 SRFactoryCode, cellSize = RevalueRaster(OutputFolder, curr_raster, minZ, maxZ, elevation_type, rows, ProjectID, ProjectUID)
#                 count = count + 1
#                 if SRFactoryCode <= 0:
#                     SRFactoryCodeFlag = 0
#                 del curr_raster
#             
#             arcpy.AddMessage("\nOperation Complete, output Rasters can be found in: {}".format(OutputFolder))
#         else:
#             arcpy.AddMessage("No rasters found at '{}'".format(InputFolder))
#         if SRFactoryCodeFlag == 0:
#             # TODO set an error in the DB ?
#             arcpy.AddWarning("WARNING: One or more rasters didn't have a SR set".format(InputFolder)) 
#     else:
#         arcpy.AddMessage("Input path does not exist '{}'".format(InputFolder))
# 
#     return count, cellSize
'''
------------------------------------------------------------
iterate through the list of raster files revalue and convert to .tif
------------------------------------------------------------
'''

import arcpy
from datetime import datetime
import os
import shutil
import sys
import time
import traceback

from ngce import Utility
from ngce.cmdr import CMDR, CMDRConfig
from ngce.cmdr.CMDRConfig import DSM, DTM, fields_RasterFileStat, \
    field_RasterFileStat_ProjID, field_RasterFileStat_Name, \
    field_RasterFileStat_ElevType, field_RasterFileStat_Group
from ngce.folders import ProjectFolders
from ngce.folders.FoldersConfig import delivered_dir, published_dir
from ngce.las.LAS import validateZRange
from ngce.pmdm import RunUtil
from ngce.pmdm.a import A05_B_RevalueRaster, A04_A_GenerateQALasDataset, \
    A04_C_ConsolidateLASInfo
from ngce.pmdm.a.A04_A_GenerateQALasDataset import grouper
from ngce.pmdm.a.A05_B_RevalueRaster import FIELD_INFO, MIN, MAX, V_NAME, V_UNIT, \
    H_NAME, H_UNIT, H_WKID, doTime


PROCESS_DELAY = 2
PROCESS_CHUNKS = 4  # files per thread. Factor of 2 please
PROCESS_SPARES = 0  # processors to leave as spares, no more than 4!

def getBoundData(bound_path):
    
    z_min = None
    z_max = None
    v_name = None
    v_unit = None
    h_name = None
    h_unit = None
    h_wkid = None
    bound_fields = [
                     FIELD_INFO[MIN][0],
                     FIELD_INFO[MAX][0],
                     FIELD_INFO[V_NAME][0],
                     FIELD_INFO[V_UNIT][0],
                     FIELD_INFO[H_NAME][0],
                     FIELD_INFO[H_UNIT][0],
                     FIELD_INFO[H_WKID][0]
                     ]
    
    for row in arcpy.da.SearchCursor(bound_path, bound_fields):  # @UndefinedVariable
        z_min = row[0]
        z_max = row[1]
        v_name = row[2]
        v_unit = row[3]
        h_name = row[4]
        h_unit = row[5]
        h_wkid = row[6]
    
    arcpy.AddMessage("\tZ is between {} and {}, adding 10% buffer on each end...".format(z_min, z_max))
    z_min = (z_min * 0.9 if z_min > 0 else z_min * 1.1)
    z_max = (z_max * 1.2 if z_max > 0 else z_max * 0.8)
    if z_min < 0 :
        arcpy.AddMessage("WARNING: Z MIN is less than 0")
    if z_max < 0:
        arcpy.AddMessage("WARNING: Z MAX is less than 0")
    
    return z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid


def processRastersInFolder(fileList, target_path, publish_path, elev_type, bound_path, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, runAgain=True):
    a = datetime.now()
    path = os.path.join(A04_A_GenerateQALasDataset.TOOLS_PATH, "A05_B_RevalueRaster.py")
    Utility.printArguments(["fileList", "target_path", "publish_path", "elev_type", "bound_path", "runAgain"],
                           [(None if fileList is None else len(fileList)), target_path, publish_path, elev_type, bound_path, runAgain], "createLasStatistics")
    
    grouping = PROCESS_CHUNKS
    if not runAgain:
        grouping = int(PROCESS_CHUNKS/2)
    if grouping <=1:
        grouping = 2
    total = len(fileList)
    if total > 0:

        fileList_repeat = []
        
        procCount = int(os.environ['NUMBER_OF_PROCESSORS'])
        if procCount > 4:
            procCount = procCount - PROCESS_SPARES
        if procCount <=0:
            procCount = 1
        arcpy.AddMessage("processRastersInFolder: Using {}/{} Processors to process {} files in groups of {}".format(procCount, (procCount + PROCESS_SPARES), total, grouping))
        processList = []
        
        indx = 0
        for f_paths in grouper(fileList, grouping):
            f_paths = [x for x in f_paths if x is not None]
            f_path = ",".join(f_paths)
            indx = indx + len(f_paths)
            
            arcpy.AddMessage('       processRastersInFolder: Working on {}/{}: {}'.format(indx, total, f_path))
            args = [f_path, elev_type, target_path, publish_path, bound_path, str(z_min), str(z_max), v_name, v_unit, h_name, h_unit, str(h_wkid)]
            
            try:
                processList.append(RunUtil.runToolx64_async(path, args, "A05_B", target_path))
                # give time for things to wake up
                time.sleep(PROCESS_DELAY)
            except:
                tb = sys.exc_info()[2]
                tbinfo = traceback.format_tb(tb)[0]
                pymsg = "processRastersInFolder: PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
                        str(sys.exc_type) + ": " + str(sys.exc_value) + "\n"
                arcpy.AddWarning(pymsg)
                msgs = "processRastersInFolder: GP ERRORS:\n" + arcpy.GetMessages(2) + "\n"
                arcpy.AddWarning(msgs)
                sys.exit(1)
    
            waitForResults = True
            first = True
            while waitForResults:
                if not first:
                    time.sleep(1)
                first = False   
                # arcpy.AddMessage('processRastersInFolder: Looping LEN Process List = {} ProcCount = {} is greater = {}'.format(len(processList), procCount, (len(processList) >= procCount)))
                for i, [p, l] in enumerate(processList):
                    if p.poll() is not None:
                        # error log messages are handled in 
                        retCode = RunUtil.endRun_async(path, p, l)
                        if retCode <> 0:
                            fileList_repeat.append(f_path)
                        del processList[i]
                        
                
                waitForResults = (len(processList) >= int(procCount))
                        
        # Wait for last subprocesses to complete
        arcpy.AddMessage("       processRastersInFolder: Waiting for process list to clear {} jobs".format(len(processList)))
        while len(processList) > 0:
            for  i, [p, l] in enumerate(processList):
                if p.poll() is not None:
                    RunUtil.endRun_async(path, p, l)
                    del processList[i]
                    arcpy.AddMessage("       processRastersInFolder: Waiting for process list to clear {} jobs".format(len(processList)))
    
                else:
                    # arcpy.AddMessage("processRastersInFolder: Waiting for process list to clear {} jobs".format(len(processList)))
                    time.sleep(PROCESS_DELAY)
        
        if runAgain and len(fileList_repeat) > 0:
            # try to clean up any errors along the way
            processRastersInFolder(fileList, target_path, publish_path, elev_type, bound_path, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, runAgain=False)

    try:
        shutil.rmtree(os.path.join(target_path, elev_type, "TEMP"), True)
    except:
        pass
    
    doTime(a, 'processRastersInFolder: All jobs completed.')
    

'''
------------------------------------------------------------
Create a standard set of analysis folders before the threads start
------------------------------------------------------------
'''
def createFolders(target_path):
    value_field = ["ELEVATION"]
    dataset_name = ["FIRST", "LAST", "ALAST"]
    A04_A_GenerateQALasDataset.createFolder(target_path, value_field, dataset_name)
    
    value_field = ["INTENSITY"]
    dataset_name = ["FIRST"]
    A04_A_GenerateQALasDataset.createFolder(target_path, value_field, dataset_name)
    
    value_field = ["STATS"]
    dataset_name = ['RASTER']
    A04_A_GenerateQALasDataset.createFolder(target_path, value_field, dataset_name)
    
    value_field = ["DTM", "DSM", "DHM"]
    dataset_name = ['TEMP']
    A04_A_GenerateQALasDataset.createFolder(target_path, value_field, dataset_name, True)


def getFileProcessList(start_dir, elev_type, target_path, publish_path):
    createFolders(target_path)
    
    workspace = arcpy.env.workspace  # @UndefinedVariable
    
    try:
        fileList = []
        index = 0
        for root, dirs, files in os.walk(start_dir):  # @UnusedVariable
            arcpy.env.workspace = root
            rasters = arcpy.ListRasters("*", "ALL")
            for f_name in rasters:
                # GRIDs show up inside themselves, ignore files with same name as end of root
                if not (root.upper().endswith(f_name.upper())):
                    f_path = os.path.join(root, f_name)
                    if A05_B_RevalueRaster.isProcessFile(f_path, elev_type, target_path, publish_path):
                        index = index + 1
                        fileList.append(f_path)
                        # arcpy.AddMessage("\t\t{}. {}".format(index, f_path))
                    
            del rasters
    
    except:
        pass
    finally:
        arcpy.env.workspace = workspace
        
    return fileList



def processProject(ProjectJob, project, ProjectUID):
    workspace = arcpy.env.workspace  # @UndefinedVariable
    
    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    ProjectID = ProjectJob.getProjectID(project)

    Deliver = CMDR.Deliver()
    delivery = list(Deliver.getDeliver(ProjectID))
    
    RasterFileStat = CMDR.RasterFileStat()
    
    minZ = Deliver.getValidZMin(delivery)
    maxZ = Deliver.getValidZMax(delivery)
    
    minZ, maxZ = validateZRange(minZ, maxZ)
    
    source_path = ProjectFolder.delivered.path
    target_path = ProjectFolder.derived.path
    publish_path = ProjectFolder.published.path
    
    rows = []
    TotalCount = 0
    targetFolders = [DSM, DTM]
    for targetFolder in targetFolders:
        localrows = []
        InputFolder = os.path.join(source_path, targetFolder)
        OutputFolder = os.path.join(target_path, targetFolder)
        PublishFolder = os.path.join(publish_path, targetFolder)
        
        count, cellSize = processRastersInFolder(minZ, maxZ, InputFolder, OutputFolder, targetFolder, localrows, ProjectID, ProjectUID)
        
        Raster_Files = []
        for row in localrows:
            rows.append(row)
            if RasterFileStat.getGroup(row) == delivered_dir:
                Raster_Files.append(RasterFileStat.getPath(row))
                newRow = list(row)
                RasterFileStat.setGroup(newRow, published_dir)
                RasterFileStat.setFormat(newRow, "TIFF")
                RasterFileStat.setPath(newRow, os.path.join(target_path, publish_path, RasterFileStat.getName(newRow)))
                rows.append(newRow)
        
        
        if len(Raster_Files) > 0:
            Utility.clearFolder(PublishFolder);
                
            Raster_Files = ";".join(Raster_Files)
            arcpy.RasterToOtherFormat_conversion(Raster_Files, PublishFolder, Raster_Format="TIFF")
            Utility.addToolMessages()
        
#         Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
        arcpy.env.workspace = workspace  # @UndefinedVariable
        TotalCount = TotalCount + count
        arcpy.AddMessage("Processed {} rasters in '{}'".format(count, InputFolder))
        
        if targetFolder == DSM:
            Deliver.setDSMCellResolution(delivery, cellSize)
            Deliver.setDSMCountRaster(delivery, count)
            Deliver.setDSMExists(delivery, "No")
            if count > 0:
                Deliver.setDSMExists(delivery, "Yes")
        elif targetFolder == DTM:
            Deliver.setDTMCellResolution(delivery, cellSize)
            Deliver.setDTMCountRaster(delivery, count)
            Deliver.setDTMExists(delivery, "No")
            if count > 0:
                Deliver.setDTMExists(delivery, "Yes")
    
#     Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
    arcpy.env.workspace = workspace  # @UndefinedVariable
    Deliver.setCountRasterFiles(delivery, TotalCount)
    Deliver.updateDeliver(delivery, ProjectID)
    
    
    arcpy.AddMessage("Saving raster property rows")
    for row in rows:
        arcpy.AddMessage("Saving {}".format(row))
                     
        RasterFileStat.saveOrUpdateRasterFileStat(row,
                                                  row[fields_RasterFileStat.index(field_RasterFileStat_ProjID)],
                                                  row[fields_RasterFileStat.index(field_RasterFileStat_Name)],
                                                  row[fields_RasterFileStat.index(field_RasterFileStat_ElevType)],
                                                  row[fields_RasterFileStat.index(field_RasterFileStat_Group)])
    
    fgdb_path = ProjectFolder.derived.fgdb_path
    if not arcpy.Exists(fgdb_path):
        arcpy.AddMessage("creating fGDB '{}'".format(fgdb_path))
        arcpy.CreateFileGDB_management(ProjectFolder.derived.path, ProjectFolder.derived.fgdb_name)
        Utility.addToolMessages()
    rasterFileStat_path = os.path.join(fgdb_path, CMDRConfig.fcName_RasterFileStat)
    if arcpy.Exists(rasterFileStat_path):
        arcpy.Delete_management(rasterFileStat_path)
        Utility.addToolMessages()
    if not arcpy.Exists(rasterFileStat_path):
        arcpy.AddMessage("creating feature class '{}' '{}' ".format(fgdb_path, CMDRConfig.fcName_RasterFileStat))
        sr = arcpy.Describe(RasterFileStat.fclass).spatialReference
        arcpy.AddMessage("using spatial reference '{}'".format(sr))
        arcpy.CreateFeatureclass_management(out_path=fgdb_path, out_name=CMDRConfig.fcName_RasterFileStat, template=RasterFileStat.fclass, geometry_type="POLYGON", spatial_reference=sr)
#             desc = arcpy.Describe(RasterFileStat.fclass)
#             fieldListComplete = desc.fields
#             # limit field list to all fields except OBJECT_ID
#             fieldList = fieldListComplete[1:]
#             # create fields in the output feature class
#             for i in fieldList:
#                 arcpy.AddField_management(rasterFileStat_path, i.name, i.type, "", "", i.length)
#                 Utility.addToolMessages()
#         edit = Utility.startEditingSession()
    cursor_i = arcpy.da.InsertCursor(rasterFileStat_path, CMDRConfig.fields_RasterFileStat)  # @UndefinedVariable
    for row in rows:
        arcpy.AddMessage("Saving {}".format(row))
        cursor_i.insertRow(row)
        arcpy.AddMessage("Updated {} record: {}".format(rasterFileStat_path, row))
#         Utility.stopEditingSession(edit)
    del cursor_i
    
    
    



# jobID = arcpy.GetParameterAsText(0)
# jobID = 16402

def RemoveDEMErrantValues(jobID):
    Utility.printArguments(["WMX Job ID"], [jobID], "A05 RemoveDEMErrantValues")
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")
    
    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
    
    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)
    
    if project is not None:
        processProject(ProjectJob, project, ProjectUID)
        
    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")
    arcpy.AddMessage("Operation complete")


if __name__ == '__main__':
    
    a = datetime.now()
#     projId = sys.argv[1]
#          
#     GenerateQALasDataset(projId, doRasters)
    
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
                
#     ProjectJob = ProjectJob()
#     project = [
#                UID,  # field_ProjectJob_UID
#                wmx_job_id,  # field_ProjectJob_WMXJobID,
#                project_Id,  # field_ProjectJob_ProjID,
#                alias,  # field_ProjectJob_Alias
#                alias_clean,  # field_ProjectJob_AliasClean
#                state ,  # field_ProjectJob_State
#                year ,  # field_ProjectJob_Year
#                parent_dir,  # field_ProjectJob_ParentDir
#                archive_dir,  # field_ProjectJob_ArchDir
#                project_dir,  # field_ProjectJob_ProjDir
#                project_AOI  # field_ProjectJob_SHAPE
#                ]
     
    elev_type = "DTM"
    start_dir = os.path.join(project_dir, "DELIVERED", elev_type)
    target_path = os.path.join(project_dir, "DERIVED")
    publish_path = os.path.join(project_dir, "PUBLISHED")
    fgdb_path = os.path.join(target_path, "{}.gdb".format(project_Id))
    
    lasd_boundary = A04_C_ConsolidateLASInfo.getLasdBoundaryPath(fgdb_path)
    
    z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid = getBoundData(lasd_boundary)
    fileList = getFileProcessList(start_dir, elev_type, target_path, publish_path)     
    processRastersInFolder(fileList, target_path, publish_path, elev_type, lasd_boundary, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid)
    
    
