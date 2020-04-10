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
from ngce.Utility import isSrValueValid, grouper, doTime, SDE_CMDR_FILE_PATH
from ngce.cmdr.JobUtil import getProjectFromWMXJobID
from ngce.folders import ProjectFolders
from ngce.folders.FoldersConfig import DTM, DSM, DLM, INT
from ngce.folders.ProjectFolders import createAnalysisFolders, \
    createPublishFolders
from ngce.las import LAS
from ngce.pmdm import RunUtil
from ngce.pmdm.a import A05_B_RevalueRaster, A04_A_GenerateQALasDataset, \
    A04_C_ConsolidateLASInfo, A05_C_ConsolidateRasterInfo, A05_D_UpdateCMDRMetadata
from ngce.raster.Raster import createRasterDatasetStats
from ngce.raster.RasterConfig import FIELD_INFO, MIN, MAX, V_NAME, V_UNIT, \
    H_NAME, H_UNIT, H_WKID, ELEV_TYPE, IS_CLASSIFIED


PROCESS_DELAY = 10
PROCESS_CHUNKS = 6  # files per thread. Factor of 2 please
PROCESS_SPARES = -8  # processors to leave as spares, no more than 4!
#changed from -4 BJN 8 Nov 2018
arcpy.env.parallelProcessingFactor = "100%"

Utility.setArcpyEnv(True)

def isSpatialRefSameForAll(InputFolder):
    arcpy.env.workspace = InputFolder
    rasters = arcpy.ListRasters("*")
    count = len(rasters)

    SpatRefFirstRaster = None
    SRMatchFlag = True


    for raster in rasters:
        describe = arcpy.Describe(raster)
        SRef = describe.SpatialReference.exportToString()
        if SpatRefFirstRaster is None:
            SpatRefFirstRaster = SRef
        if SRef != SpatRefFirstRaster:
            SRMatchFlag = False
            arcpy.AddError("Raster has a PCSCode (EPSG code) that is different than first raster: {}".format(raster))

    return SRMatchFlag, count


def getSRErrorMessage(sr_type, horz_name_isValid, vert_name_isValid, horz_unit_isValid, vert_unit_isValid):
    msg = None
    if not horz_name_isValid:
        if msg is None:
            msg = sr_type
        msg = "{}{}".format(msg, " Horizontal CS is not defined")
    if not horz_unit_isValid:
        if msg is None:
            msg = sr_type
        msg = "{}{}".format(msg, " Horizontal CS unit is not defined")
    if not vert_name_isValid:
        if msg is None:
            msg = sr_type
        msg = "{}{}".format(msg, " Vertical CS is not defined")
    if not vert_unit_isValid:
        if msg is None:
            msg = sr_type
        msg = "{}{}".format(msg, " Vertical CS unit is not defined")

    return msg

def checkSpatialOnRaster(start_dir, elev_type, target_path, v_name, v_unit, h_name, h_unit, h_wkid):

    ras_spatial_ref = None
    prj_spatial_ref = None

    f_path = getFileProcessList(start_dir, elev_type, target_path, None, return_first=True)
    arcpy.AddMessage("\n************\nTesting spatial reference on {} file: '{}'".format(elev_type, f_path))

    desc = arcpy.Describe(f_path)
    if desc is not None:
        ras_spatial_ref = desc.SpatialReference

    raster_props = createRasterDatasetStats(f_path)

    prj_Count, prj_File = Utility.fileCounter(start_dir, '.prj')
    if prj_Count > 0 and prj_File is not None and len(str(prj_File)) > 0:
        prj_file_full_path = os.path.join(start_dir, prj_File)

        prj_spatial_ref = arcpy.SpatialReference(prj_file_full_path)

    ras_horz_cs_name, ras_horz_cs_unit_name, ras_horz_cs_factory_code, ras_vert_cs_name, ras_vert_cs_unit_name = Utility.getSRValues(ras_spatial_ref)
    prj_horz_cs_name, prj_horz_cs_unit_name, prj_horz_cs_factory_code, prj_vert_cs_name, prj_vert_cs_unit_name = Utility.getSRValues(prj_spatial_ref)

    if ras_spatial_ref is not None:
        arcpy.AddMessage("{} File Spatial Reference:\n\tH_Name: '{}'\n\tH_Unit: '{}'\n\tH_WKID: '{}'\n\tV_Name: '{}'\n\tV_Unit: '{}'".format(elev_type, ras_horz_cs_name, ras_horz_cs_unit_name, ras_horz_cs_factory_code, ras_vert_cs_name, ras_vert_cs_unit_name))
        arcpy.AddMessage(getSRErrorMessage("\t{} File Spatial Reference:".format(elev_type), ras_horz_cs_name, ras_vert_cs_name, ras_horz_cs_unit_name, ras_vert_cs_unit_name))
    else:
        arcpy.AddMessage("{} File Spatial Reference DOES NOT EXIST".format(elev_type))



    if prj_spatial_ref is not None:
        try:
            arcpy.AddMessage("PRJ File Spatial Reference:\n\tH_Name: '{}'\n\tH_Unit: '{}'\n\tH_WKID: '{}'\n\tV_Name: '{}'\n\tV_Unit: '{}'".format(prj_horz_cs_name, prj_horz_cs_unit_name, prj_horz_cs_factory_code, prj_vert_cs_name, prj_vert_cs_unit_name))
            errorMsg = getSRErrorMessage("\tPRJ File Spatial Reference:", prj_horz_cs_name, prj_vert_cs_name, prj_horz_cs_unit_name, prj_vert_cs_unit_name)
            if errorMsg is not None:
                arcpy.AddMessage(errorMsg)
        except:
            pass
    else:
        arcpy.AddMessage("PRJ File Spatial Reference DOES NOT EXIST")

    prj_horz_name_isValid = isSrValueValid(prj_horz_cs_name)
    prj_vert_name_isValid = isSrValueValid(prj_vert_cs_name) or (elev_type == 'INTENSITY')
    prj_horz_unit_isValid = isSrValueValid(prj_horz_cs_unit_name)
    prj_vert_unit_isValid = isSrValueValid(prj_vert_cs_unit_name) or (elev_type == 'INTENSITY')

    las_horz_name_isValid = isSrValueValid(ras_horz_cs_name)
    las_vert_name_isValid = isSrValueValid(ras_vert_cs_name) or (elev_type == 'INTENSITY')
    las_horz_unit_isValid = isSrValueValid(ras_horz_cs_unit_name)
    las_vert_unit_isValid = isSrValueValid(ras_vert_cs_unit_name) or (elev_type == 'INTENSITY')

    prj_isValid = prj_horz_name_isValid and prj_vert_name_isValid and prj_horz_unit_isValid and prj_vert_unit_isValid

    las_isValid = las_horz_name_isValid and las_vert_name_isValid and las_horz_unit_isValid and las_vert_unit_isValid

    sr_horz_name_isSame = prj_horz_name_isValid and las_horz_name_isValid and prj_horz_cs_name == ras_horz_cs_name
    sr_horz_unit_isSame = prj_horz_unit_isValid and las_horz_unit_isValid and prj_horz_cs_unit_name == ras_horz_cs_unit_name
    sr_vert_name_isSame = prj_vert_name_isValid and las_vert_name_isValid and prj_vert_cs_name == ras_vert_cs_name
    sr_vert_unit_isSame = prj_vert_unit_isValid and las_vert_unit_isValid and prj_vert_cs_unit_name == ras_vert_cs_unit_name

    sr_horz_isSame = sr_horz_name_isSame and sr_horz_unit_isSame
    sr_vert_isSame = sr_vert_name_isSame and sr_vert_unit_isSame

    sr_isSame = sr_horz_isSame and sr_vert_isSame

    if prj_isValid or las_isValid:
        if sr_horz_isSame:
            arcpy.AddMessage("         The horizontal spatial references MATCH")
        else:
            arcpy.AddWarning("WARNING: The {} and PRJ horizontal spatial references DO NOT MATCH.".format(elev_type))

        if sr_vert_isSame:
            arcpy.AddMessage("         The {} and PRJ vertical spatial references MATCH".format(elev_type))
        else:
            arcpy.AddWarning("WARNING: The {} and PRJ vertical spatial references DO NOT MATCH.".format(elev_type))

        if sr_isSame:
            arcpy.AddMessage("         The {} and PRJ spatial references MATCH".format(elev_type))
        else:
            arcpy.AddWarning("WARNING: The {} and PRJ spatial references DO NOT MATCH.".format(elev_type))

    result = None
    if prj_isValid:
        A05_B_RevalueRaster.CheckRasterSpatialReference(v_name, v_unit, h_name, h_unit, h_wkid, raster_props, prj_spatial_ref)
        arcpy.AddMessage("         Found a valid spatial reference in a PRJ file. Using this as the spatial reference: {}".format(Utility.getSpatialReferenceInfo(prj_spatial_ref)))
        result = os.path.join(start_dir, prj_File)
    elif las_isValid:
        A05_B_RevalueRaster.CheckRasterSpatialReference(v_name, v_unit, h_name, h_unit, h_wkid, raster_props, ras_spatial_ref)
        arcpy.AddMessage("         Found a valid spatial reference in a {} file. Using this as the spatial reference: {}".format(elev_type, Utility.getSpatialReferenceInfo(ras_spatial_ref)))
        result = ras_spatial_ref



    return result

def bufferZValues(z_min, z_max, add_buffer=True):
    if add_buffer:
        arcpy.AddMessage("\tZ is between {} and {}, adding 20% buffer on each end...".format(z_min, z_max))
        z_min = z_min - abs(z_min * 0.2)
        z_max = z_max + abs(z_max * 0.2)

    arcpy.AddMessage("\tZ is between {} and {}.".format(z_min, z_max))

    if z_min < 0 :
        arcpy.AddWarning("WARNING: Z MIN is less than 0")
    if z_max < 0:
        arcpy.AddWarning("WARNING: Z MAX is less than 0")

    if z_min > 9000 :
        arcpy.AddWarning("WARNING: Z MIN is greater than 9000")
    if z_max > 9000:
        arcpy.AddWarning("WARNING: Z MAX is greater than 9000")

    return z_min, z_max

def getRasterBoundData(bound_path, elev_type, add_buffer=True):

    try:
        arcpy.AddMessage("Raster Bound Data: \n\tbound_path={}\n\telev_type={}\n\tadd_buffer={}\n\t".format(
            bound_path, elev_type, add_buffer
            ))
    except UnicodeEncodeError as uer:
        arcpy.AddMessage('Raster Bound Data - Encoding Error Has Truncated Text')
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

    where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(bound_path, FIELD_INFO[ELEV_TYPE][0]), elev_type)
    for row in arcpy.da.SearchCursor(bound_path, bound_fields, where_clause):  # @UndefinedVariable
        z_min = row[0]
        z_max = row[1]
        v_name = row[2]
        v_unit = row[3]
        h_name = row[4]
        h_unit = row[5]
        h_wkid = row[6]

    z_min, z_max = bufferZValues(z_min, z_max, add_buffer)

    try:
        arcpy.AddMessage("Raster Bound Data: \n\tz_min={}\n\tz_max={}\n\tv_name={}\n\tv_unit={}\n\th_name={}\n\th_unit={}\n\th_wkid={}\n\t".format(
            z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid
            ))
    except UnicodeEncodeError as uer:
        arcpy.AddMessage('Raster Bound Data - Encoding Error Has Truncated Text')


    if elev_type is not "INTENSITY":
        if z_min is None or z_max is None or h_name is None or h_unit is None or v_name is None or v_unit is None:
            arcpy.AddError("Raster Elevation Bound Data is not valid, something is wrong with the BoundaryRaster_{} in the DERIVED file geodatabase".format(elev_type))
    elif z_min is None or z_max is None or h_name is None or h_unit is None:
        arcpy.AddError("Raster Intensity Bound Data is not valid, something is wrong with the BoundaryRaster_{} in the DERIVED file geodatabase".format(elev_type))
    return z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid



def getLasdBoundData(bound_path, add_buffer=True):

    z_min = None
    z_max = None
    v_name = None
    v_unit = None
    h_name = None
    h_unit = None
    h_wkid = None
    is_classified = None
    bound_fields = [
                     FIELD_INFO[MIN][0],
                     FIELD_INFO[MAX][0],
                     FIELD_INFO[V_NAME][0],
                     FIELD_INFO[V_UNIT][0],
                     FIELD_INFO[H_NAME][0],
                     FIELD_INFO[H_UNIT][0],
                     FIELD_INFO[H_WKID][0],
                     FIELD_INFO[IS_CLASSIFIED][0]
                     ]

    arcpy.AddMessage(bound_path)
    for row in arcpy.da.SearchCursor(bound_path, bound_fields):  # @UndefinedVariable
        z_min = row[0]
        z_max = row[1]
        v_name = row[2]
        v_unit = row[3]
        h_name = row[4]
        h_unit = row[5]
        h_wkid = row[6]
        is_classified = row[7]

    z_min, z_max = bufferZValues(z_min, z_max, add_buffer)

    arcpy.AddMessage("LAS Boundary Parameters")
    console_checks = [
        ('z_min', z_min),
        ('z_max', z_max),
        ('v_name', v_name),
        ('v_unit', v_unit),
        ('h_name', h_name),
        ('h_unit', h_unit),
        ('h_wkid', h_wkid),
        ('is_classified', is_classified)
        ]

    for check in console_checks:
        try:
            arcpy.AddMessage('{} \t {}'.format(check[0], check[1]))
        except UnicodeEncodeError as e:
            try:
                arcpy.AddMessage('{} \t {}'.format(check[0], check[1].encode('utf-8')))
            except Exception as e:
                arcpy.AddMessage('Encoding Failed: {}'.format(e))

    return z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, is_classified


def processRastersInFolder(fileList, target_path, publish_path, elev_type, bound_path, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, spatial_ref, runAgain=True):
    a = datetime.now()
    path = os.path.join(RunUtil.TOOLS_PATH, "A05_B_RevalueRaster.py")
    Utility.printArguments(["fileList", "target_path", "publish_path", "elev_type", "bound_path", "spatial_ref", "runAgain"],
                           [(None if fileList is None else len(fileList)), target_path, publish_path, elev_type, bound_path, spatial_ref, runAgain], "createLasStatistics")

    grouping = PROCESS_CHUNKS
    if not runAgain:
        grouping = int(PROCESS_CHUNKS / 2)
    if grouping <= 1:
        grouping = 2
    total = len(fileList)
    if total > 0:

        fileList_repeat = []

        procCount = int(os.environ['NUMBER_OF_PROCESSORS'])
        if procCount > 4:
            procCount = procCount - PROCESS_SPARES
        if procCount <= 0:
            procCount = 1
        arcpy.AddMessage("processRastersInFolder: Using {}/{} Processors to process {} files in groups of {}".format(procCount, (procCount + PROCESS_SPARES), total, grouping))
        processList = []

        indx = 0
        for f_paths in grouper(fileList, grouping):
            f_paths = [x for x in f_paths if x is not None]
            f_path = ",".join(f_paths)
            indx = indx + len(f_paths)

            arcpy.AddMessage('       processRastersInFolder: Working on {} {}/{}'.format(elev_type, indx, total))
            args = [f_path, elev_type, target_path, publish_path, bound_path, str(z_min), str(z_max), v_name, v_unit, h_name, h_unit, str(h_wkid), spatial_ref]

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
            processRastersInFolder(fileList, target_path, publish_path, elev_type, bound_path, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, spatial_ref, runAgain=False)

    try:
        shutil.rmtree(os.path.join(target_path, elev_type, "TEMP"), True)
    except:
        pass

    doTime(a, 'processRastersInFolder: All jobs completed.')



def getFileProcessList(start_dir, elev_type, target_path, publish_path, return_first=False, check_sr=False):
    createAnalysisFolders(target_path)
    if publish_path is not None:
        createPublishFolders(publish_path)

    workspace = arcpy.env.workspace  # @UndefinedVariable

    SpatRefFirstRaster = None
    SRMatchFlag = True

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
                    if return_first:
                        return f_path
                    if A05_B_RevalueRaster.isProcessFile(f_path, elev_type, target_path, publish_path):
                        index = index + 1
                        fileList.append(f_path)

                        if check_sr:
                            describe = arcpy.Describe(f_path)
                            SRef = describe.SpatialReference.exportToString()
                            if SpatRefFirstRaster is None:
                                SpatRefFirstRaster = SRef
                            elif SRef != SpatRefFirstRaster:
                                SRMatchFlag = False
                                arcpy.AddError("ERROR: Raster file '{}' has a different spatial reference: \n\tFirst Raster: {}\n\tThis Raster: {}".format(f_path, SpatRefFirstRaster, SRef))

            del rasters

    except:
        pass
    finally:
        arcpy.env.workspace = workspace

    if check_sr:
        return fileList, SRMatchFlag
    elif return_first:
        # if we get to here, there are no rasters in this folder
        return None
    else:
        return fileList


def validateRasterSpaitialRef(ProjectFolder, start_dir, elev_type, target_path, v_name, v_unit, h_name, h_unit, h_wkid):
    las_qainfo = LAS.QALasInfo(ProjectFolder, True)  # isclassified doesn't matter, disposbale las qa info

    las_qainfo.lasd_spatial_ref = checkSpatialOnRaster(start_dir, elev_type, target_path, v_name, v_unit, h_name, h_unit, h_wkid)

    if las_qainfo.lasd_spatial_ref is None:
        arcpy.AddError("ERROR:   Neither spatial reference in PRJ or {} files are valid CANNOT CONTINUE.".format(elev_type))
        arcpy.AddError("ERROR:   Please add a valid projection file (.prj) to the DELIVERED\{} folder.".format(elev_type))

    elif not las_qainfo.isValidSpatialReference():
        las_qainfo.lasd_spatial_ref = None
        arcpy.AddError("ERROR: Spatial Reference for the {} files is not standard: '{}'".format(elev_type, Utility.getSpatialReferenceInfo(las_qainfo.lasd_spatial_ref)))
        arcpy.AddError("ERROR:   Please add a valid projection file (.prj) to the DELIVERED\{} folder.".format(elev_type))

    elif las_qainfo.isUnknownSpatialReference():
        las_qainfo.lasd_spatial_ref = None
        arcpy.AddError("ERROR: Spatial Reference for the {} files is not standard: '{}'".format(elev_type, Utility.getSpatialReferenceInfo(las_qainfo.lasd_spatial_ref)))
        arcpy.AddError("ERROR:   Please add a valid projection file (.prj) to the DELIVERED\{} folder.".format(elev_type))

    if las_qainfo.lasd_spatial_ref is not None:
        f_list, all_matching = getFileProcessList(start_dir, elev_type, target_path, None, return_first=False, check_sr=True)  # @UnusedVariable
        if not all_matching:
            las_qainfo.lasd_spatial_ref = None
            arcpy.AddError("Not all raster files have same spatial reference. Please make sure all files have the same spatial reference.")

    return las_qainfo.lasd_spatial_ref


def processJob(ProjectJob, project, ProjectUID):

    a = datetime.now()
    aa = a
    aaa = aa

    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    ProjectID = ProjectJob.getProjectID(project)
    ProjectUID = ProjectJob.getUID(project)

    elev_types = [DTM, DSM, DLM, INT]
    target_path = ProjectFolder.derived.path
    publish_path = ProjectFolder.published.path
    fgdb_path = ProjectFolder.derived.fgdb_path

    lasd_boundary = A04_C_ConsolidateLASInfo.getLasdBoundaryPath(fgdb_path)
    raster_footprints, raster_boundaries = [], []

    raster_footprint_main = A05_C_ConsolidateRasterInfo.getRasterFootprintPath(fgdb_path)
    raster_boundary_main = A05_C_ConsolidateRasterInfo.getRasterBoundaryPath(fgdb_path)

    spatialRef_error = {}
    z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, is_classified = getLasdBoundData(lasd_boundary)  # @UnusedVariable
    arcpy.AddMessage('TRACKING')
    arcpy.AddMessage(v_name)

    # Explicitely check if DTM images exist. If not bail
    start_dir = os.path.join(ProjectFolder.delivered.path, DTM)
    f_name = getFileProcessList(start_dir, DTM, target_path, publish_path, return_first=True, check_sr=False)
    if f_name is None:
        arcpy.AddError("No DTM Files Found in {} folder, cannot proceed.".format(start_dir))
        raise Exception("No DTM Files Found, cannot proceed.")

    for elev_type in elev_types:

        spatialRef_error[elev_type] = False
        start_dir = os.path.join(ProjectFolder.delivered.path, elev_type)
        arcpy.AddMessage('### START DIR {}'.format(start_dir))
        f_name = getFileProcessList(start_dir, elev_type, target_path, publish_path, return_first=True, check_sr=False)
        if f_name is None:
            arcpy.AddMessage("Trying DERIVED source. No {} rasters found to re-value in {}.".format(elev_type, start_dir))
            if elev_type == DSM:
                start_dir = os.path.join(ProjectFolder.derived.path, "ELEVATION", "FIRST")
            elif elev_type == DLM:
                start_dir = os.path.join(ProjectFolder.derived.path, "ELEVATION", "ALAST")
            elif elev_type == INT:
                start_dir = os.path.join(ProjectFolder.derived.path, "INTENSITY", "FIRST")
            f_name = getFileProcessList(start_dir, elev_type, target_path, publish_path, return_first=True, check_sr=False)

        if f_name is None:
            arcpy.AddWarning("No {} rasters found to re-value in {}".format(elev_type, start_dir))
        else:
            spatial_ref = validateRasterSpaitialRef(ProjectFolder, start_dir, elev_type, target_path, v_name, v_unit, h_name, h_unit, h_wkid)

            if spatial_ref is None:
                spatialRef_error[elev_type] = True
            else:
                spatialRef_error[elev_type] = False
                fileList = getFileProcessList(start_dir, elev_type, target_path, publish_path)
                processRastersInFolder(fileList, target_path, publish_path, elev_type, lasd_boundary, z_min, z_max, v_name, v_unit, h_name, h_unit, h_wkid, spatial_ref)
                raster_footprint, raster_boundary = A05_C_ConsolidateRasterInfo.createRasterBoundaryAndFootprints(fgdb_path, target_path, ProjectID, ProjectFolder.path, ProjectUID, elev_type)
                if raster_footprint is not None:
                    raster_footprints.append(raster_footprint)
                    arcpy.RepairGeometry_management(in_features=raster_footprint, delete_null="KEEP_NULL")
                if raster_boundary is not None:
                    raster_boundaries.append(raster_boundary)
                    arcpy.RepairGeometry_management(in_features=raster_boundary, delete_null="KEEP_NULL")

        a = doTime(a, 'COMPLETED: Finished processing {}\n---------------------------------------\n\n'.format(elev_type))

    aa = doTime(aa, 'COMPLETED: Finished processing all elevation types')

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
    arcpy.RepairGeometry_management(in_features=raster_footprint_main, delete_null="KEEP_NULL")


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

    arcpy.RepairGeometry_management(in_features=raster_boundary_main, delete_null="KEEP_NULL")

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

    errorMsg = []
    for elev_type in spatialRef_error.keys():
        if spatialRef_error[elev_type]:
            arcpy.AddError("Failed to process {} correctly".format(elev_type))
            errorMsg.append(elev_type)

    try:
        if fgdb_path is not None and os.path.exists(fgdb_path):
            arcpy.Compact_management(in_workspace=fgdb_path)
    except:
        pass

    # JWS 6/8/18 - Push Derived FGDB Metadata to CMDR Geodatabase
    try:
        A05_D_UpdateCMDRMetadata.copy_metadata(fgdb_path, SDE_CMDR_FILE_PATH)
    except Exception as e:
        arcpy.AddMessage('Exception Pushing FGDB Metadata: {}'.format(e))

    aa = doTime(aa, 'COMPLETED: Finished merging raster footprints and boundaries')
    doTime(aaa, 'COMPLETED: A05_A Completed')
    return errorMsg







# jobID = arcpy.GetParameterAsText(0)
# jobID = 16402

def RemoveDEMErrantValues(strJobId):
    aa = datetime.now()
    Utility.printArguments(["WMX Job ID"], [strJobId], "A05 RemoveDEMErrantValues")
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")

    ProjectJob, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable

    errorMsg = processJob(ProjectJob, project, strUID)

    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")
    doTime(aa, "Operation Complete: A05 Remove DEM Errant Values")
    if len(errorMsg) > 0:
        raise Exception("Failed to process {} raster data correctly".format(" ".join(errorMsg)))


if __name__ == '__main__':
    jobID = sys.argv[1]
    RemoveDEMErrantValues(jobID)

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
#
#    processJob(ProjectJob, project, UID)
