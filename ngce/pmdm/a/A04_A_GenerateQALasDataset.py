'''
Created on Dec 7, 2015

@author: eric5946
'''
import arcpy
from datetime import datetime
import os
import sys
import time
import traceback

import A04_B_CreateLASStats
from ngce import Utility
from ngce.Utility import doTime, deleteFileIfExists, isSrValueValid
from ngce.cmdr import CMDR
from ngce.cmdr.JobUtil import getProjectFromWMXJobID
from ngce.folders import ProjectFolders
from ngce.las import LAS
from ngce.pmdm import RunUtil
from ngce.pmdm.a import A04_C_ConsolidateLASInfo


PROCESS_DELAY = 1
PROCESS_CHUNKS = 6  # files per thread
PROCESS_SPARES = -3  # processors to leave as spares

arcpy.env.parallelProcessingFactor = "8"

arcpy.env.overwriteOutput = True


'''
------------------------------------------------------------
iterate through the list of .las files and generate individual file
statistics datasets for each
------------------------------------------------------------
'''
def createLasStatistics(fileList, target_path, spatial_reference=None, isClassified=True, createQARasters=False, createMissingRasters=False, overrideBorderPath=None, runAgain=True):
    a = datetime.now()
    path = os.path.join(RunUtil.TOOLS_PATH, "A04_B_CreateLASStats.py")
    Utility.printArguments(["fileList", "target_path", "spatial_reference", "isClassified", "createQARasters", "createMissingRasters", "overrideBorderPath"],
                           [fileList, target_path, spatial_reference, isClassified, createQARasters, createMissingRasters, overrideBorderPath], "createLasStatistics")

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
        arcpy.AddMessage("\tUsing {}/{} Processors to process {} files in groups of {}".format(procCount, (procCount + PROCESS_SPARES), total, grouping))
        processList = []

        indx = 0
        for f_paths in Utility.grouper(fileList, grouping):
            f_paths = [x for x in f_paths if x is not None]
            f_path = ",".join(f_paths)
            indx = indx + len(f_paths)

            arcpy.AddMessage('\t Working on {}/{}: {}'.format(indx, total, f_path))
            args = [f_path, target_path, spatial_reference, "{}".format(isClassified), "{}".format(createQARasters), "{}".format(createMissingRasters), overrideBorderPath]

            try:
                processList.append(RunUtil.runToolx64_async(path, args, "A04_B", target_path))
                # give time for things to wake up
                time.sleep(PROCESS_DELAY)
            except:
                tb = sys.exc_info()[2]
                tbinfo = traceback.format_tb(tb)[0]
                pymsg = " PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
                        str(sys.exc_type) + ": " + str(sys.exc_value) + "\n"
                arcpy.AddWarning(pymsg)
                msgs = "GP ERRORS:\n" + arcpy.GetMessages(2) + "\n"
                arcpy.AddWarning(msgs)
                sys.exit(1)

            waitForResults = True
            first = True
            while waitForResults:
                if not first:
                    time.sleep(1)
                first = False
                # arcpy.AddMessage('Looping LEN Process List = {} ProcCount = {} is greater = {}'.format(len(processList), procCount, (len(processList) >= procCount)))
                for i, [p, l] in enumerate(processList):
                    if p.poll() is not None:
                        # error log messages are handled in
                        retCode = RunUtil.endRun_async(path, p, l)
                        if retCode <> 0:
                            fileList_repeat.append(f_path)
                        del processList[i]


                waitForResults = (len(processList) >= int(procCount))

        # Wait for last subprocesses to complete
        arcpy.AddMessage("\tWaiting for process list to clear {} jobs".format(len(processList)))
        while len(processList) > 0:
            for  i, [p, l] in enumerate(processList):
                if p.poll() is not None:
                    retCode = RunUtil.endRun_async(path, p, l)
                    if retCode <> 0:
                        fileList_repeat.append(f_path)
                    del processList[i]
                    if len(processList) > 0:
                        arcpy.AddMessage("\tWaiting for process list to clear {} jobs".format(len(processList)))

                else:
                    # arcpy.AddMessage("Waiting for process list to clear {} jobs".format(len(processList)))
                    time.sleep(PROCESS_DELAY)

        if runAgain and len(fileList_repeat) > 0:
            # try to clean up any errors along the way
            createLasStatistics(fileList, target_path, spatial_reference, isClassified, createQARasters, createMissingRasters, overrideBorderPath, runAgain=False)
        elif not runAgain and len(fileList_repeat) > 0:
            arcpy.AddError("Error processing .las files.")
            raise Exception("Error processing .las files.")

        doTime(a, 'createLasStatistics: All jobs completed.')

def getProjectDEMStatistics(las_qainfo):

    stat_fc_path = os.path.join(las_qainfo.filegdb_path, "LASDatasetInfo")
    fields_LASD_summary = ["Pt_Cnt", "Pt_Spacing", "Z_Min", "Z_Max", "Class", "Percent", "Z_range", "Area"]

    where_clause = None

    if las_qainfo.isClassified:
        where_clause = "{} = '0'".format(arcpy.AddFieldDelimiters(stat_fc_path, "Class"))
    arcpy.AddMessage("getting LAS Point file information where {}".format(where_clause))

    for r in arcpy.da.SearchCursor(las_qainfo.las_summary_fc_path, fields_LASD_summary, where_clause=where_clause):  # @UndefinedVariable
        arcpy.AddMessage("LAS Point ALL POINTS File Information {}".format(r))
        las_qainfo.pt_count_dsm = r[0]
        las_qainfo.pt_spacing_dsm = r[1]
        las_qainfo.minZ_dsm = r[2]
        las_qainfo.maxZ_dsm = r[3]

        # reuse DSM stats for DTM non-classified data
        las_qainfo.pt_count_dtm = las_qainfo.pt_count_dsm
        las_qainfo.pt_spacing_dtm = las_qainfo.pt_spacing_dsm
        las_qainfo.minZ_dtm = las_qainfo.minZ_dsm
        las_qainfo.maxZ_dtm = las_qainfo.maxZ_dsm

    if las_qainfo.isClassified:
        # Get classified data model key for DTM, Can't easily get DTM stats for non-classified data
        where_clause = "{} = '8'".format(arcpy.AddFieldDelimiters(stat_fc_path, "Class"))
        arcpy.AddMessage("getting LAS Point file information where {}".format(where_clause))
        for r in arcpy.da.SearchCursor(las_qainfo.las_summary_fc_path, fields_LASD_summary, where_clause=where_clause):  # @UndefinedVariable
            arcpy.AddMessage("LAS Point MODEL KEY File Information {}".format(r))
            las_qainfo.pt_count_dtm = r[0]
            las_qainfo.pt_spacing_dtm = r[1]
            las_qainfo.minZ_dtm = r[2]
            las_qainfo.maxZ_dtm = r[3]

        where_clause = "{} = 2".format(arcpy.AddFieldDelimiters(stat_fc_path, "Class"))
        arcpy.AddMessage("getting LAS Point file information where {}".format(where_clause))
        for r in arcpy.da.SearchCursor(las_qainfo.las_summary_fc_path, fields_LASD_summary, where_clause=where_clause):  # @UndefinedVariable
            arcpy.AddMessage("LAS Point GROUND File Information {}".format(r))
            las_qainfo.pt_count_dtm = r[0]
            las_qainfo.pt_spacing_dtm = r[1]
            las_qainfo.minZ_dtm = r[2]
            las_qainfo.maxZ_dtm = r[3]

    return las_qainfo

def getLasQAInfo(ProjectFolder):
    las_qainfo = None
    foundLas = False
    for isClassified in [True, False]:
        if not(foundLas):
            las_qainfo = LAS.QALasInfo(ProjectFolder, isClassified)

            if os.path.exists(las_qainfo.las_directory):
                las_qainfo.num_las_files, las_qainfo.first_las_name = Utility.fileCounter(las_qainfo.las_directory, '.las')
                arcpy.AddMessage("{} las files in LasDirectory '{}'".format(las_qainfo.num_las_files, las_qainfo.las_directory))

                if(las_qainfo.num_las_files > 0):
                    foundLas = True

    return las_qainfo



def getLasFileProcessList(start_dir, target_path, createQARasters, isClassified, returnFirst=False):
    ext = ".las"
    fileList = []
    arcpy.AddMessage("getLasFileProcessList: Starting in dir {}".format(start_dir))
    for root, dirs, files in os.walk(start_dir):  # @UnusedVariable
        for f in files:
            if f.upper().endswith(ext.upper()):
                subdir = os.path.join(",".join(dirs))
                f_path = os.path.join(root, subdir, f)

                if returnFirst:
                    return f_path

                if A04_B_CreateLASStats.isProcessFile(f_path, target_path, createQARasters, isClassified):
                    fileList.append(f_path)

#     theFile = os.path.join(target_path, "filelist.txt")
#     try:
#         os.remove(theFile)
#     except:
#         pass
#     theFile = open(theFile, 'w')
#     for item in fileList:
#         theFile.write("%s\n" % item)
#     theFile.close()

    return fileList

# def getLasFileProcessListFile(target_path):
#
#     theFile = os.path.join(target_path, "filelist.txt")
#     theFile = open(theFile, 'rb')
#     fileList = [line.rstrip('\n') for line in file]
#     theFile.close()
#
#     try:
#         os.remove(theFile)
#     except:
#         pass
#
#     return fileList

'''
-------------------------------------------------------------------------
Generate the information about the .las files

Input:
    jobID = The projects WMX Job ID
-------------------------------------------------------------------------
'''
# @TODO: Move this to its own script
def updateCMDR(ProjectJob, project, las_qainfo, updatedBoundary):

    bound_XMin = updatedBoundary.extent.XMin
    bound_YMin = updatedBoundary.extent.YMin
    bound_XMax = updatedBoundary.extent.XMax
    bound_YMax = updatedBoundary.extent.YMax

#     extents = [[bound_XMin, bound_YMin], [bound_XMax, bound_YMax]]

    updatedBoundary_Area = updatedBoundary.getArea("PRESERVE_SHAPE", "SQUAREMETERS")


    arcpy.AddMessage("Getting DEM Statistics")
    las_qainfo = getProjectDEMStatistics(las_qainfo)
    arcpy.AddMessage("Getting SR Info")
    sr_horz_alias = las_qainfo.getSpatialReference().name
    sr_horz_unit = las_qainfo.getSpatialReference().linearUnitName
    sr_horz_wkid = las_qainfo.getSpatialReference().factoryCode
    arcpy.AddMessage("SR horizontal alias & unit: {} {}".format(sr_horz_alias, sr_horz_unit))
    sr_vert_alias, sr_vert_unit = Utility.getVertCSInfo(las_qainfo.getSpatialReference())
    arcpy.AddMessage("SR vertical alias & unit: {} {}".format(sr_vert_alias, sr_vert_unit))
    arcpy.AddMessage("Updating CMDR Deliver features")
    Deliver = CMDR.Deliver()
    deliver = list(Deliver.getDeliver(las_qainfo.ProjectID))
    Deliver.setCountLasFiles(deliver, las_qainfo.num_las_files)
    Deliver.setCountLasPointsDTM(deliver, las_qainfo.pt_count_dtm)
    Deliver.setCountLasPointsDSM(deliver, las_qainfo.pt_count_dsm)
    Deliver.setPointSpacingDTM(deliver, las_qainfo.pt_spacing_dtm)
    Deliver.setPointSpacingDSM(deliver, las_qainfo.pt_spacing_dsm)
    Deliver.setBoundXMin(deliver, bound_XMin)
    Deliver.setBoundYMin(deliver, bound_YMin)
    Deliver.setBoundXMax(deliver, bound_XMax)
    Deliver.setBoundYMax(deliver, bound_YMax)
    if las_qainfo.pt_spacing_dtm > 0:
        Deliver.setPointDensityDTM(deliver, pow((1.0 / las_qainfo.pt_spacing_dtm), 2))
    if las_qainfo.pt_spacing_dsm > 0:
        Deliver.setPointDensityDSM(deliver, pow((1.0 / las_qainfo.pt_spacing_dsm), 2))
    Deliver.setDeliverArea(deliver, updatedBoundary_Area)
    Deliver.setHorzSRName(deliver, sr_horz_alias)
    Deliver.setHorzUnit(deliver, sr_horz_unit)
    Deliver.setHorzSRWKID(deliver, sr_horz_wkid)
    Deliver.setVertSRName(deliver, sr_vert_alias)
    Deliver.setVertUnit(deliver, sr_vert_unit)
    las_qainfo.minZ_dsm, las_qainfo.maxZ_dsm = LAS.validateZRange(sr_vert_unit, las_qainfo.minZ_dsm, las_qainfo.maxZ_dsm)
    las_qainfo.minZ_dtm, las_qainfo.maxZ_dtm = LAS.validateZRange(sr_vert_unit, las_qainfo.minZ_dtm, las_qainfo.maxZ_dtm)
    Deliver.setValidZMax(deliver, las_qainfo.maxZ_dsm)
    if las_qainfo.maxZ_dsm >= LAS.maxValidElevation(sr_vert_unit):
        Deliver.setValidZMax(deliver, min(las_qainfo.maxZ_dtm, LAS.maxValidElevation(sr_vert_unit)))
    Deliver.setValidZMin(deliver, max(las_qainfo.minZ_dtm, LAS.minValidElevation(sr_vert_unit)))
    Deliver.setIsLASClassified(deliver, las_qainfo.isClassified)
    if updatedBoundary is not None:
        arcpy.AddMessage("Updating CMDR ProjectJob AOI")
        ProjectJob.updateJobAOI(project, updatedBoundary)
        arcpy.AddMessage("Updating CMDR QC AOI")
        QC = CMDR.QC()
        qc = QC.getQC(las_qainfo.ProjectID)
        QC.updateAOI(qc, updatedBoundary)
        arcpy.AddMessage("Updating CMDR Delivery AOI")
        Deliver.updateAOI(deliver, updatedBoundary)
        arcpy.AddMessage("Updating CMDR Contract AOI")
        Contract = CMDR.Contract()
        contract = Contract.getContract(las_qainfo.ProjectID)
        Contract.updateAOI(contract, updatedBoundary)

# @TODO: fix the publish
#           Publish = CMDR.Publish()
#           publish = Publish.getPublish(las_qainfo.ProjectID)
#           Publish.updateAOI(publish, updatedBoundary)





def checkSpatialOnLas(start_dir, target_path, createQARasters, isClassified):
    las_spatial_ref = None
    prj_spatial_ref = None

    las_f_path = getLasFileProcessList(start_dir, target_path, createQARasters, isClassified, returnFirst=True)
    lasd_f_path = "{}d".format(las_f_path)

    a = datetime.now()
    deleteFileIfExists(lasd_f_path, True)
    arcpy.AddMessage("{} Testing spatial reference on .las file: '{}' '{}'".format(datetime.now(),las_f_path, lasd_f_path))

#     arcpy.CreateLasDataset_management(input="E:/NGCE/RasterDatasets/OK_SugarCreek_2008/DELIVERED/LAS_CLASSIFIED/3409805_ne_A.las", out_las_dataset="E:/NGCE/RasterDatasets/OK_SugarCreek_2008/DELIVERED/LAS_CLASSIFIED/c3409805_ne_A_LasDataset.lasd", folder_recursion="NO_RECURSION", in_surface_constraints="", spatial_reference="", compute_stats="COMPUTE_STATS", relative_paths="RELATIVE_PATHS", create_las_prj="NO_FILES")

    arcpy.CreateLasDataset_management(input=las_f_path,
                                      spatial_reference=None,
                                      out_las_dataset=lasd_f_path,
                                      folder_recursion="NO_RECURSION",
                                      in_surface_constraints="",
                                      compute_stats="COMPUTE_STATS",
                                      relative_paths="RELATIVE_PATHS",
                                      create_las_prj="NO_FILES")

    doTime(a, "\t{} Created LASD {}".format(datetime.now(),lasd_f_path))

    desc = arcpy.Describe(lasd_f_path)
    if desc is not None:
        las_spatial_ref = desc.SpatialReference
        if las_spatial_ref is not None:
            try:
                arcpy.AddMessage("\tFound spatial reference in LAS: {}".format(las_spatial_ref.exportToString()))
            except:
                pass

    prj_Count, prj_File = Utility.fileCounter(start_dir, '.prj')
    arcpy.AddMessage("\tFound {} PRJ files, the first is: {}".format(prj_Count,prj_File))
    if prj_Count > 0 and prj_File is not None and len(str(prj_File)) > 0:
        prj_Path = os.path.join(start_dir, prj_File)
        arcpy.AddMessage("\tReading spatial reference from PRJ file: {}".format(prj_Path))
        
        prj_spatial_ref = arcpy.SpatialReference(prj_Path)
        arcpy.AddMessage("\tGot from PRJ file spatial reference: {}".format(prj_spatial_ref))
        if prj_spatial_ref is not None:
            try:
                arcpy.AddMessage("\tFound spatial reference in PRJ: {}".format(prj_spatial_ref.exportToString()))
            except:
                pass

    arcpy.AddMessage("Decoding LAS File Spatial Reference")
    las_horz_cs_name, las_horz_cs_unit_name, las_horz_cs_factory_code, las_vert_cs_name, las_vert_cs_unit_name = Utility.getSRValues(las_spatial_ref)

    prj_horz_cs_name, prj_horz_cs_unit_name, prj_horz_cs_factory_code, prj_vert_cs_name, prj_vert_cs_unit_name = None, None, None, None, None
    if prj_spatial_ref is not None:
        arcpy.AddMessage("Decoding PRJ File Spatial Reference")
        prj_horz_cs_name, prj_horz_cs_unit_name, prj_horz_cs_factory_code, prj_vert_cs_name, prj_vert_cs_unit_name = Utility.getSRValues(prj_spatial_ref)

    arcpy.AddMessage("LAS File Spatial Reference:\n\tH_Name: '{}'\n\tH_Unit: '{}'\n\tH_WKID: '{}'\n\tV_Name: '{}'\n\tV_Unit: '{}'".format(las_horz_cs_name, las_horz_cs_unit_name, las_horz_cs_factory_code, las_vert_cs_name, las_vert_cs_unit_name))
    arcpy.AddMessage("PRJ File Spatial Reference:\n\tH_Name: '{}'\n\tH_Unit: '{}'\n\tH_WKID: '{}'\n\tV_Name: '{}'\n\tV_Unit: '{}'".format(prj_horz_cs_name, prj_horz_cs_unit_name, prj_horz_cs_factory_code, prj_vert_cs_name, prj_vert_cs_unit_name))

    prj_horz_name_isValid = isSrValueValid(prj_horz_cs_name)
    prj_vert_name_isValid = isSrValueValid(prj_vert_cs_name)
    prj_horz_unit_isValid = isSrValueValid(prj_horz_cs_unit_name)
    prj_vert_unit_isValid = isSrValueValid(prj_vert_cs_unit_name)

    las_horz_name_isValid = isSrValueValid(las_horz_cs_name)
    las_vert_name_isValid = isSrValueValid(las_vert_cs_name)
    las_horz_unit_isValid = isSrValueValid(las_horz_cs_unit_name)
    las_vert_unit_isValid = isSrValueValid(las_vert_cs_unit_name)

    prj_isValid = prj_horz_name_isValid and prj_vert_name_isValid and prj_horz_unit_isValid and prj_vert_unit_isValid

    las_isValid = las_horz_name_isValid and las_vert_name_isValid and las_horz_unit_isValid and las_vert_unit_isValid

    sr_horz_name_isSame = prj_horz_name_isValid and las_horz_name_isValid and prj_horz_cs_name == las_horz_cs_name
    sr_horz_unit_isSame = prj_horz_unit_isValid and las_horz_unit_isValid and prj_horz_cs_unit_name == las_horz_cs_unit_name
    sr_vert_name_isSame = prj_vert_name_isValid and las_vert_name_isValid and prj_vert_cs_name == las_vert_cs_name
    sr_vert_unit_isSame = prj_vert_unit_isValid and las_vert_unit_isValid and prj_vert_cs_unit_name == las_vert_cs_unit_name

    sr_horz_isSame = sr_horz_name_isSame and sr_horz_unit_isSame
    sr_vert_isSame = sr_vert_name_isSame and sr_vert_unit_isSame

    sr_isSame = sr_horz_isSame and sr_vert_isSame

    if prj_isValid or las_isValid:
        if sr_horz_isSame:
            arcpy.AddMessage("         The LAS and PRJ horizontal spatial references MATCH".format(Utility.getSpatialReferenceInfo(prj_spatial_ref)))
        else:
            arcpy.AddWarning("WARNING: The LAS and PRJ horizontal spatial references DO NOT MATCH.")

        if sr_vert_isSame:
            arcpy.AddMessage("         The LAS and PRJ vertical spatial references MATCH".format(Utility.getSpatialReferenceInfo(prj_spatial_ref)))
        else:
            arcpy.AddWarning("WARNING: The LAS and PRJ vertical spatial references DO NOT MATCH.")

        if sr_isSame:
            arcpy.AddMessage("         The LAS and PRJ spatial references MATCH".format(Utility.getSpatialReferenceInfo(prj_spatial_ref)))
        else:
            arcpy.AddWarning("WARNING: The LAS and PRJ spatial references DO NOT MATCH.")

    result = None
    if prj_isValid:
        arcpy.AddMessage("         Found a valid spatial reference in a PRJ file. Using this as the spatial reference: {}".format(Utility.getSpatialReferenceInfo(prj_spatial_ref)))
        result = os.path.join(start_dir, prj_File)
    elif las_isValid:
        arcpy.AddMessage("         Found a valid spatial reference in a LAS file. Using this as the spatial reference: {}".format(Utility.getSpatialReferenceInfo(las_spatial_ref)))
        result = las_spatial_ref

    return result

def isLayerExist(mxd, df, lyr_name):
    for lyr in arcpy.mapping.ListLayers(mxd, "*", df):
        if lyr.name == lyr_name:
            return True
    return False

def createMXD(las_qainfo, target_path, project_ID):
    mxd = None
    try:
        las_footprint_path = A04_C_ConsolidateLASInfo.getLasFootprintPath(las_qainfo.filegdb_path)
        lasd_boundary_path = A04_C_ConsolidateLASInfo.getLasdBoundaryPath(las_qainfo.filegdb_path)
        lasd_path = las_qainfo.las_dataset_path
        out_map_file_path = os.path.join(target_path, "{}.mxd".format(project_ID))

        if os.path.exists(out_map_file_path):
            arcpy.AddMessage("MXD exists: {}".format(out_map_file_path))
        else:
            mxd = arcpy.mapping.MapDocument(os.path.join(os.path.dirname(os.path.realpath(__file__)), "blank.mxd"))
            mxd.saveACopy(out_map_file_path)
            arcpy.AddMessage("Created MXD {}".format(out_map_file_path))

        mxd = arcpy.mapping.MapDocument(out_map_file_path)
        mxd.relativePaths = True

        df = mxd.activeDataFrame

        if not isLayerExist(mxd, df, "LAS Footprints"):
            lyr_footprint = arcpy.MakeFeatureLayer_management(las_footprint_path, "LAS Footprints").getOutput(0)
            arcpy.mapping.AddLayer(df, lyr_footprint, 'BOTTOM')
            arcpy.AddMessage("Added layer to mxd: {}".format(lyr_footprint))

        if not isLayerExist(mxd, df, "LAS Boundary"):
            lyr_boundary = arcpy.MakeFeatureLayer_management(lasd_boundary_path, "LAS Boundary").getOutput(0)
            arcpy.mapping.AddLayer(df, lyr_boundary, 'BOTTOM')
            arcpy.AddMessage("Added layer to mxd: {}".format(lyr_boundary))

        if not isLayerExist(mxd, df, "LAS Dataset"):
            lyr_lasd = arcpy.MakeLasDatasetLayer_management(lasd_path, "LAS Dataset").getOutput(0)
            arcpy.mapping.AddLayer(df, lyr_lasd, 'TOP')
            arcpy.AddMessage("Added layer to mxd: {}".format(lyr_lasd))

        if not isLayerExist(mxd, df, "LAS Boundary Difference"):
            lasd_boundary_SD = "{}_SD".format(lasd_boundary_path)
            lyr_diff = arcpy.MakeFeatureLayer_management(lasd_boundary_SD, "LAS Boundary Difference").getOutput(0)
            arcpy.mapping.AddLayer(df, lyr_diff, 'TOP')
            arcpy.AddMessage("Added layer to mxd: {}".format(lyr_diff))

        mxd.save()
    except:
        arcpy.AddWarning("Failed to set up project MXD")

    return mxd

def processJob(ProjectJob, project, createQARasters=False, createMissingRasters=False, overrideBorderPath=None):
    aaa = datetime.now()
    a = aaa
    lasd_boundary = None

    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    ProjectID = ProjectJob.getProjectID(project)
    ProjectUID = ProjectJob.getUID(project)

    target_path = ProjectFolder.derived.path



    # Get the LAS QA Info to determine if it is classified or not
    las_qainfo = getLasQAInfo(ProjectFolder)
    if las_qainfo.num_las_files <= 0:
        arcpy.AddError("Project with Job ID {} has no .las files in DELIVERED LAS_CLASSIFIED or LAS_UNCLASSIFIED folders, CANNOT CONTINUE.".format(ProjectFolder.projectId))
    else:
        ProjectFolders.createAnalysisFolders(target_path, las_qainfo.isClassified)

        # Make the STAT folder if it doesn't already exist

        stat_out_folder = ProjectFolder.derived.stats_path
        if not os.path.exists(stat_out_folder):
            os.makedirs(stat_out_folder)
            arcpy.AddMessage("created Derived STAT folder '{}'".format(stat_out_folder))
        else:
            arcpy.AddMessage("STAT folder '{}'".format(stat_out_folder))

        # Make the scratch file GDB for the project
        if not os.path.exists(las_qainfo.filegdb_path):
            arcpy.CreateFileGDB_management(target_path, las_qainfo.filegdb_name)
            Utility.addToolMessages()
        else:
            arcpy.AddMessage("Derived fGDB sand box already exists. Using '{}'".format(las_qainfo.filegdb_path))

        las_qainfo.lasd_spatial_ref = checkSpatialOnLas(las_qainfo.las_directory, target_path, createQARasters, las_qainfo.isClassified)

        if las_qainfo.lasd_spatial_ref is None:
            arcpy.AddError("ERROR:   Neither spatial reference in PRJ or LAS files are valid CANNOT CONTINUE.")
            arcpy.AddError("ERROR:   Please create a projection file (.prj) in the LAS folder using the '3D Analyst Tools/Conversion/From File/Point File Information' tool.")

        elif not las_qainfo.isValidSpatialReference():
            las_qainfo.lasd_spatial_ref = None
            arcpy.AddError("ERROR: Spatial Reference for the las files is not standard (see above)")
            arcpy.AddError("ERROR: Please create a projection file (.prj) in the LAS folder using the '3D Analyst Tools/Conversion/From File/Point File Information' tool.")
            try:
                arcpy.AddError("ERROR: '{}'".format(Utility.getSpatialReferenceInfo(las_qainfo.lasd_spatial_ref)))
            except:
                pass

        elif las_qainfo.isUnknownSpatialReference():
            las_qainfo.lasd_spatial_ref = None
            arcpy.AddError("ERROR: Please provide a projection file (.prj) that provides a valid transformation in the LAS directory.")
            arcpy.AddError("ERROR:   Please create a projection file (.prj) in the LAS folder using the '3D Analyst Tools/Conversion/From File/Point File Information' tool.")
            arcpy.AddError("ERROR: Spatial Reference for the las files is not standard")
            try:
                arcpy.AddError("ERROR: '{}'".format(Utility.getSpatialReferenceInfo(las_qainfo.lasd_spatial_ref)))
            except:
                pass

        if las_qainfo.lasd_spatial_ref is None:
            raise Exception("Error: Spatial Reference is invalid, unknown, or not specified.")
        else:
    #         prj_Count, prj_File = Utility.fileCounter(las_qainfo.las_directory, '.prj')
    #         if prj_Count > 0 and prj_File is not None and len(str(prj_File)) > 0:
    #             prj_spatial_ref = os.path.join(las_qainfo.las_directory, prj_File)
    #
    #         if prj_Count > 0:
    #             las_qainfo.setProjectionFile(prj_File)
    #             las_spatial_ref = os.path.join(las_qainfo.las_directory, prj_File)
    #             arcpy.AddMessage("Found a projection file with the las files, OVERRIDE LAS SR (if set) '{}'".format(las_spatial_ref))
    #             arcpy.AddMessage(Utility.getSpatialReferenceInfo(las_qainfo.getSpatialReference()))
    #         else:
    #             arcpy.AddMessage("Using projection (coordinate system) from las files if available.")

            fileList = getLasFileProcessList(las_qainfo.las_directory, target_path, createQARasters, las_qainfo.isClassified)
            createLasStatistics(fileList, target_path, las_qainfo.lasd_spatial_ref, las_qainfo.isClassified, createQARasters, createMissingRasters, overrideBorderPath)

            # Create the project's las dataset. Don't do this before you validated that each .las file has a .lasx
            if os.path.exists(las_qainfo.las_dataset_path):
                arcpy.AddMessage("Using existing LAS Dataset {}".format(las_qainfo.las_dataset_path))
                # arcpy.AddMessage("Deleting existing LAS Dataset {}".format(las_qainfo.las_dataset_path))
                # arcpy.Delete_management(las_qainfo.las_dataset_path)
            else:
                a = datetime.now()
                # note: don't use method in A04_B because we don't want to compute statistics this time
                arcpy.CreateLasDataset_management(input=las_qainfo.las_directory,
                                              out_las_dataset=las_qainfo.las_dataset_path,
                                              folder_recursion="RECURSION",
                                              in_surface_constraints="",
                                                  spatial_reference=las_qainfo.lasd_spatial_ref,
                                              compute_stats="NO_COMPUTE_STATS",
                                              relative_paths="RELATIVE_PATHS",
                                              create_las_prj="FILES_MISSING_PROJECTION")
                Utility.addToolMessages()
                a = doTime(a, "Created LAS Dataset '{}'".format(las_qainfo.las_dataset_path))


            desc = arcpy.Describe(las_qainfo.las_dataset_path)

            # las_qainfo.lasd_spatial_ref = desc.SpatialReference
            las_qainfo.LASDatasetPointCount = desc.pointCount
            las_qainfo.LASDatasetFileCount = desc.fileCount
            arcpy.AddMessage("LASDatasetPointCount {} and LASDatasetFileCount {}".format(desc.pointCount, desc.fileCount))

            lasd_boundary, las_footprint = A04_C_ConsolidateLASInfo.createRasterBoundaryAndFootprints(las_qainfo.filegdb_path, target_path, ProjectID, ProjectFolder.path, ProjectUID)

            mxd = createMXD(las_qainfo, target_path, ProjectID)


            # if createQARasters:
            arcpy.AddMessage("Creating QA raster mosaics")
            mosaics = A04_C_ConsolidateLASInfo.createQARasterMosaics(las_qainfo.isClassified, las_qainfo.filegdb_path, las_qainfo.lasd_spatial_ref, target_path, mxd, las_footprint, lasd_boundary)
            if mxd is not None:
                a = datetime.now()
                try:
                    mxd_path = mxd.filePath
                    for [md_path, md_name] in mosaics:
                        arcpy.AddMessage("Adding QA raster mosaic {} to mxd {}".format(md_path, mxd_path))
                        try:
                            if not arcpy.Exists(md_path):
                                a = doTime(a, "\tMD doesn't exist {}. Can't add to MXD {}. Is it open?".format(md_path, mxd_path))
                            else:
                                df = mxd.activeDataFrame
                                if isLayerExist(mxd, df, md_name):
                                    a = doTime(a, "\t MD {} already exists in MXD {}".format(md_name, mxd_path))
                                else:
                                    if len(str(md_name)) > 0:
                                        try:
                                            lyr_md = arcpy.MakeMosaicLayer_management(in_mosaic_dataset=md_path, out_mosaic_layer=md_name).getOutput(0)
                                            df = mxd.activeDataFrame
                                            arcpy.mapping.AddLayer(df, lyr_md, 'BOTTOM')
                                            # lyr_md.visible = False
                                            mxd.save()
                                            a = doTime(a, "\tAdded MD {} to MXD {} as {}".format(md_name, mxd_path, lyr_md))
                                        except:
                                            a = doTime(a, "\tfailed to add MD {} to MXD {}. Is it open?".format(md_path, mxd_path))

                        except:
                            try:
                                a = doTime(a, "\tfailed to add MD to MXD {}. Is it open?".format(mxd_path))
                            except:
                                pass

                    mxd.save()
                except:
                    try:
                        a = doTime(a, "\tfailed to save MXD {}. Is it open?".format(mxd_path))
                    except:
                            pass




    bbb = datetime.now()
    td = (bbb - aaa).total_seconds()
    arcpy.AddMessage("Completed {} in {}".format(las_qainfo.las_dataset_path, td))

    return las_qainfo, lasd_boundary



def GenerateQALasDataset(strJobId, createQARasters=False, createMissingRasters=False, overrideBorderPath=None):
    Utility.printArguments(["WMXJobID", "createQARasters", "createMissingRasters", "overrideBorderPath"],
                           [strJobId, createQARasters, createMissingRasters, overrideBorderPath], "A04 GenerateQALasDataset")

    aa = datetime.now()
    arcpy.AddMessage("Checking out licenses")
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")

    ProjectJob, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable

    las_qainfo, lasd_boundary = processJob(ProjectJob, project, createQARasters, createMissingRasters, overrideBorderPath)
    try:
        if las_qainfo is not None and os.path.exists(las_qainfo.filegdb_path):
            arcpy.Compact_management(in_workspace=las_qainfo.filegdb_path)
    except:
        pass

        # @TODO: Move this to another standalone script
        # updateCMDR(ProjectJob, project, las_qainfo, updatedBoundary)

    arcpy.AddMessage("Checking in licenses")
    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")

    if las_qainfo.num_las_files <= 0:
        raise Exception("Project has no .las files in DELIVERED LAS_CLASSIFIED or LAS_UNCLASSIFIED folders, CANNOT CONTINUE.\nERROR: {}".format(project))

    doTime(aa, "Operation Complete: A04 Generate QA LASDataset")


if __name__ == '__main__':

    strJobID = sys.argv[1]
    createQARasters = False
    createMissingRasters = True
    overrideBorderPath = None

    if len(sys.argv) > 2:
        arcpy.AddMessage("CreateQARasters argv = '{}'".format(sys.argv[2]))
        createQARasters = (str(sys.argv[2]).upper() == "TRUE")
    if len(sys.argv) > 3:
        arcpy.AddMessage("createMissingRasters argv = '{}'".format(sys.argv[3]))
        createMissingRasters = (str(sys.argv[3]).upper() == "TRUE")
    if len(sys.argv) > 4:
        overrideBorderPath = str(sys.argv[4])
    Utility.printArguments(["WMXJobID", "createQARasters", "createMissingRasters", "overrideBorderPath"],
                           [strJobID, createQARasters, createMissingRasters, overrideBorderPath], "A04 GenerateQALasDataset")

    GenerateQALasDataset(strJobID, createQARasters, createMissingRasters, overrideBorderPath)

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
#
#     processJob(ProjectJob, project, True)
