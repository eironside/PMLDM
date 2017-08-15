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
from ngce.cmdr import CMDR
from ngce.folders import ProjectFolders
from ngce.pmdm import RunUtil
from ngce.pmdm.a import A05_B_RevalueRaster, A04_A_GenerateQALasDataset, \
    A04_C_ConsolidateLASInfo, A05_C_ConsolidateRasterInfo
from ngce.pmdm.a.A04_A_GenerateQALasDataset import grouper
from ngce.pmdm.a.A05_B_RevalueRaster import FIELD_INFO, MIN, MAX, V_NAME, V_UNIT, \
    H_NAME, H_UNIT, H_WKID, doTime


PROCESS_DELAY = 10
PROCESS_CHUNKS = 6  # files per thread. Factor of 2 please
PROCESS_SPARES = 1  # processors to leave as spares, no more than 4!

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
    
    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    ProjectID = ProjectJob.getProjectID(project)
    ProjectUID = ProjectJob.getUID(project)

    elev_types = ["DTM", "DSM"]
    target_path = ProjectFolder.derived.path
    publish_path = ProjectFolder.published.path
    fgdb_path = ProjectFolder.derived.fgdb_path
    
    lasd_boundary = A04_C_ConsolidateLASInfo.getLasdBoundaryPath(fgdb_path)
    raster_footprints, raster_boundaries = [], []
    
    raster_footprint_main = A05_C_ConsolidateRasterInfo.getRasterFootprintPath(fgdb_path)
    raster_boundary_main = A05_C_ConsolidateRasterInfo.getRasterBoundaryPath(fgdb_path)
    
    z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid = getBoundData(lasd_boundary)
    for elev_type in elev_types:
        start_dir = os.path.join(ProjectFolder.delivered.path, elev_type)
        fileList = getFileProcessList(start_dir, elev_type, target_path, publish_path)     
        processRastersInFolder(fileList, target_path, publish_path, elev_type, lasd_boundary, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid)
        raster_footprint, raster_boundary = A05_C_ConsolidateRasterInfo.createRasterBoundaryAndFootprints(fgdb_path, target_path, ProjectID, ProjectFolder.path, ProjectUID, elev_type)
        if raster_footprint is not None:
            raster_footprints.append(raster_footprint)
        if raster_boundary is not None:
            raster_boundaries.append(raster_boundary)
    
    
    if arcpy.Exists(raster_footprint_main):
        A05_C_ConsolidateRasterInfo.deleteFileIfExists(raster_footprint_main, True)
    if len(raster_footprints) > 0:
        arcpy.Merge_management(inputs=raster_footprints, output=raster_footprint_main)
        arcpy.AddMessage("Merged raster footprints {} to {}".format(raster_footprints, raster_footprint_main))
#         for raster_footprint in raster_footprints:
#             try:
#                 A05_C_ConsolidateRasterInfo.deleteFileIfExists(raster_footprint, True)
#             except:
#                 pass
    
    
    
    if arcpy.Exists(raster_boundary_main):
        A05_C_ConsolidateRasterInfo.deleteFileIfExists(raster_boundary_main, True)
    if len(raster_boundaries) > 0:
        arcpy.Merge_management(inputs=raster_boundaries, output=raster_boundary_main)
        arcpy.AddMessage("Merged raster boundaries {} to {}".format(raster_boundaries, raster_boundary_main))
#         for raster_boundary in raster_boundaries:
#             try:
#                 A05_C_ConsolidateRasterInfo.deleteFileIfExists(raster_boundary, True)
#             except:
#                 pass
    
    try:
        out_map_file_path = os.path.join(target_path, "{}.mxd".format(ProjectID))
        if not os.path.exists(out_map_file_path):
            mxd = arcpy.mapping.MapDocument(r"./blank.mxd")
            mxd.saveACopy(out_map_file_path)
                
        mxd = arcpy.mapping.MapDocument(out_map_file_path)
        mxd.relativePaths = True    
        mxd_path = mxd.filePath
        if mxd is not None:
            df = mxd.activeDataFrame    
            if not A04_A_GenerateQALasDataset.isLayerExist(mxd, df, "Raster Boundary"):
                lyr_footprint = arcpy.MakeFeatureLayer_management(raster_boundary_main, "Raster Boundary").getOutput(0)
                arcpy.mapping.AddLayer(df, lyr_footprint, 'TOP')
                arcpy.AddMessage("\tAdded MD {} to MXD {}.".format("Raster Boundary", mxd_path))
            
            if not A04_A_GenerateQALasDataset.isLayerExist(mxd, df, "Raster Footprints"):
                lyr_footprint = arcpy.MakeFeatureLayer_management(raster_footprint_main, "Raster Footprints").getOutput(0)
                arcpy.mapping.AddLayer(df, lyr_footprint, 'TOP')
                arcpy.AddMessage("\tAdded MD {} to MXD {}.".format("Raster Footprints", mxd_path))
            
            
            mxd.save()
    except:
        pass
    
    
    



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
    jobID = sys.argv[1]
    
    RemoveDEMErrantValues(jobID)
    
#          
    
#     
#     UID = None  # field_ProjectJob_UID
#     wmx_job_id = 1
#     project_Id = "OK_SugarCreek_2008"
#     alias = "Sugar Creek"
#     alias_clean = "SugarCreek"
#     state = "OK"
#     year = 2008
#     parent_dir = r"E:\NGCE\RasterDatasets"
#     archive_dir = r"E:\NGCE\RasterDatasets"
#     project_dir = r"E:\NGCE\RasterDatasets\OK_SugarCreek_2008"
#     project_AOI = None
                
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
     
    
