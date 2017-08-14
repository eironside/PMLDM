'''
Created on Dec 7, 2015

@author: eric5946
'''
import arcpy
import datetime
from itertools import izip_longest
import os
import shutil
import sys
import time
import traceback

import A04_B_CreateLASStats
from ngce import Utility
from ngce.cmdr import CMDR
from ngce.cmdr.CMDR import ProjectJob
from ngce.folders import ProjectFolders
from ngce.las import LAS
from ngce.pmdm import RunUtil
from ngce.pmdm.a import A04_C_ConsolidateLASInfo
from ngce.pmdm.a.A04_B_CreateLASStats import STAT_FOLDER, doTime, \
    deleteFileIfExists


PROCESS_DELAY = 2
PROCESS_CHUNKS = 4  # files per thread
PROCESS_SPARES = 3  # processors to leave as spares

arcpy.env.overwriteOutput = True
# TOOLS_PATH = r"Q:\elevation\WorkflowManager\Tools\ngce\pmdm\a"
TOOLS_PATH = r"C:\Users\eric5946\workspaceEE\NGCE_PMDM\src-ngce\ngce\pmdm\a"



def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return izip_longest(*args, fillvalue=fillvalue)
    
'''
------------------------------------------------------------
Create a given folder and sub folders
------------------------------------------------------------
'''
def createFolder(target_path, parent, children, deleteIfExists=False):
    for parent in parent:
        for child in children:
            out_folder = os.path.join(target_path, parent, child)
            if deleteIfExists:
                shutil.rmtree(out_folder, True)
            if not os.path.exists(out_folder):
                os.makedirs(out_folder)
            

'''
------------------------------------------------------------
Create a standard set of analysis folders before the threads start
------------------------------------------------------------
'''
def createFolders(target_path):
    value_field = ["ELEVATION"]
    dataset_name = ["FIRST", "LAST", "ALAST"]
    createFolder(target_path, value_field, dataset_name)
    
    value_field = ["INTENSITY"]
    dataset_name = ["FIRST"]
    createFolder(target_path, value_field, dataset_name)
    
    value_field = ["PULSE_COUNT", "POINT_COUNT", "PREDOMINANT_LAST_RETURN", "PREDOMINANT_CLASS", "INTENSITY_RANGE", "Z_RANGE"]
    dataset_name = ['ALL', "FIRST", "LAST"]
    createFolder(target_path, value_field, dataset_name)
    
    value_field = ["STATS"]
    dataset_name = ['LAS', 'RASTER']
    createFolder(target_path, value_field, dataset_name)

'''
------------------------------------------------------------
iterate through the list of .las files and generate individual file
statistics datasets for each
------------------------------------------------------------
'''
def createLasStatistics(fileList, target_path, spatial_reference=None, isClassified=True, doRasters=False, runAgain=True):
    a = datetime.datetime.now()
    path = os.path.join(TOOLS_PATH, "A04_B_CreateLASStats.py")
    Utility.printArguments(["fileList", "target_path", "spatial_reference", "isClassified", "doRasters"],
                           [fileList, target_path, spatial_reference, isClassified, doRasters], "createLasStatistics")
    
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
        arcpy.AddMessage("createLasStatistics: Using {}/{} Processors to process {} files in groups of {}".format(procCount, (procCount + PROCESS_SPARES), total, grouping))
        processList = []
        
        indx = 0
        for f_paths in grouper(fileList, grouping):
            f_paths = [x for x in f_paths if x is not None]
            f_path = ",".join(f_paths)
            indx = indx + len(f_paths)
            
            arcpy.AddMessage('\tcreateLasStatistics: Working on {}/{}: {}'.format(indx, total, f_path))
            args = [f_path, target_path, spatial_reference, "{}".format(isClassified), "{}".format(doRasters)]
            
            try:
                processList.append(RunUtil.runToolx64_async(path, args, "A04_B", target_path))
                # give time for things to wake up
                time.sleep(PROCESS_DELAY)
            except:
                tb = sys.exc_info()[2]
                tbinfo = traceback.format_tb(tb)[0]
                pymsg = "createLasStatistics: PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
                        str(sys.exc_type) + ": " + str(sys.exc_value) + "\n"
                arcpy.AddWarning(pymsg)
                msgs = "createLasStatistics: GP ERRORS:\n" + arcpy.GetMessages(2) + "\n"
                arcpy.AddWarning(msgs)
                sys.exit(1)
    
            waitForResults = True
            first = True
            while waitForResults:
                if not first:
                    time.sleep(1)
                first = False   
                # arcpy.AddMessage('createLasStatistics: Looping LEN Process List = {} ProcCount = {} is greater = {}'.format(len(processList), procCount, (len(processList) >= procCount)))
                for i, [p, l] in enumerate(processList):
                    if p.poll() is not None:
                        # error log messages are handled in 
                        retCode = RunUtil.endRun_async(path, p, l)
                        if retCode <> 0:
                            fileList_repeat.append(f_path)
                        del processList[i]
                        
                
                waitForResults = (len(processList) >= int(procCount))
                        
        # Wait for last subprocesses to complete
        arcpy.AddMessage("\tcreateLasStatistics: Waiting for process list to clear {} jobs".format(len(processList)))
        while len(processList) > 0:
            for  i, [p, l] in enumerate(processList):
                if p.poll() is not None:
                    RunUtil.endRun_async(path, p, l)
                    del processList[i]
                    arcpy.AddMessage("\tcreateLasStatistics: Waiting for process list to clear {} jobs".format(len(processList)))
    
                else:
                    # arcpy.AddMessage("createLasStatistics: Waiting for process list to clear {} jobs".format(len(processList)))
                    time.sleep(PROCESS_DELAY)
        
        if runAgain and len(fileList_repeat) > 0:
            # try to clean up any errors along the way
            createLasStatistics(fileList, target_path, spatial_reference, isClassified, doRasters, runAgain=False)
            
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



def getLasFileProcessList(start_dir, target_path, doRasters, isClassified, returnFirst=False):
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
                
                if A04_B_CreateLASStats.isProcessFile(f_path, target_path, doRasters, isClassified):
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

def updateCMDR(ProjectJob, project, las_qainfo, updatedBoundary):
    
    bound_XMin = updatedBoundary.extent.XMin
    bound_YMin = updatedBoundary.extent.YMin
    bound_XMax = updatedBoundary.extent.XMax
    bound_YMax = updatedBoundary.extent.YMax 
    
#     extents = [[bound_XMin, bound_YMin], [bound_XMax, bound_YMax]]
    
    updatedBoundary_Area = updatedBoundary.getArea("PRESERVE_SHAPE", "SQUAREMETERS")

    
    arcpy.AddMessage("Getting DEM Statistics")
    getProjectDEMStatistics(las_qainfo)
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

# TODO fix the publish
#           Publish = CMDR.Publish()
#           publish = Publish.getPublish(las_qainfo.ProjectID)
#           Publish.updateAOI(publish, updatedBoundary)



def isSrValueValid(sr_value):
    result = True
    if sr_value is None or sr_value == 'UNKNOWN' or sr_value.upper() == 'NONE' or sr_value == '0':
        result = False
    return result

 
def checkSpatialOnLas(start_dir, target_path, doRasters, isClassified):
    las_spatial_ref = None
    prj_spatial_ref = None
    
    las_f_path = getLasFileProcessList(start_dir, target_path, doRasters, isClassified, returnFirst=True)
    lasd_f_path = "{}d".format(las_f_path)
    
    a = datetime.datetime.now()
    deleteFileIfExists(lasd_f_path, True)
    arcpy.AddMessage("Testing spatial reference on .las file: '{}' '{}'".format(las_f_path, lasd_f_path))
    
#     arcpy.CreateLasDataset_management(input="E:/NGCE/RasterDatasets/OK_SugarCreek_2008/DELIVERED/LAS_CLASSIFIED/3409805_ne_A.las", out_las_dataset="E:/NGCE/RasterDatasets/OK_SugarCreek_2008/DELIVERED/LAS_CLASSIFIED/c3409805_ne_A_LasDataset.lasd", folder_recursion="NO_RECURSION", in_surface_constraints="", spatial_reference="", compute_stats="COMPUTE_STATS", relative_paths="RELATIVE_PATHS", create_las_prj="NO_FILES")
    
    arcpy.CreateLasDataset_management(input=las_f_path,
                                      spatial_reference=None,
                                      out_las_dataset=lasd_f_path,
                                      folder_recursion="NO_RECURSION",
                                      in_surface_constraints="",
                                      compute_stats="COMPUTE_STATS",
                                      relative_paths="RELATIVE_PATHS",
                                      create_las_prj="NO_FILES")
    
    doTime(a, "\tCreated LASD {}".format(lasd_f_path))
    
    desc = arcpy.Describe(lasd_f_path)
    if desc is not None:
        las_spatial_ref = desc.SpatialReference
    
    prj_Count, prj_File = Utility.fileCounter(start_dir, '.prj')
    if prj_Count > 0 and prj_File is not None and len(str(prj_File)) > 0:
        prj_spatial_ref = os.path.join(start_dir, prj_File)
        
        prj_spatial_ref = arcpy.SpatialReference(prj_spatial_ref)
    
    las_horz_cs_name, las_horz_cs_unit_name, las_horz_cs_factory_code, las_vert_cs_name, las_vert_cs_unit_name = Utility.getSRValues(las_spatial_ref)
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
    
    
            

def processProject(ProjectJob, project, doRasters):
    aaa = datetime.datetime.now()
    updatedBoundary = None
    
    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    ProjectID = ProjectJob.getProjectID(project)
    ProjectUID = ProjectJob.getUID(project)
    
    target_path = ProjectFolder.derived.path
    
    createFolders(target_path)
    
    # Get the LAS QA Info to determine if it is classified or not
    las_qainfo = getLasQAInfo(ProjectFolder)
    if las_qainfo.num_las_files <= 0:
        arcpy.AddError("Project with Job ID {} has no .las files in DELIVERED LAS_CLASSIFIED or LAS_UNCLASSIFIED folders, CANNOT CONTINUE.".format(ProjectFolder.projectId))
    else:
        
        # Make the STAT folder if it doesn't already exist
        stat_out_folder = os.path.join(target_path, STAT_FOLDER)
        if not os.path.exists(stat_out_folder):
            os.makedirs(stat_out_folder)
            arcpy.AddMessage("created Derived STAT folder '{}'".format(stat_out_folder))
        else:
            arcpy.AddMessage("STAT folder already exists. Using '{}'".format(stat_out_folder))
        
        # Make the scratch file GDB for the project
        if not os.path.exists(las_qainfo.filegdb_path):
            arcpy.CreateFileGDB_management(target_path, las_qainfo.filegdb_name)
            Utility.addToolMessages()
        else:
            arcpy.AddMessage("Derived fGDB sand box already exists. Using '{}'".format(las_qainfo.filegdb_path))
        
        las_qainfo.lasd_spatial_ref = checkSpatialOnLas(las_qainfo.las_directory, target_path, doRasters, las_qainfo.isClassified)
        
        if las_qainfo.lasd_spatial_ref is None:
            arcpy.AddError("ERROR:   Neither spatial reference in PRJ or LAS files are valid CANNOT CONTINUE.")
            arcpy.AddError("ERROR:   Please create a projection file (.prj) in the LAS folder using the '3D Analyst Tools/Conversion/From File/Point File Information' tool.")
            
        elif not las_qainfo.isValidSpatialReference():
            las_qainfo.lasd_spatial_ref = None
            arcpy.AddError("ERROR: Spatial Reference for the las files is not standard: '{}'".format(Utility.getSpatialReferenceInfo(las_qainfo.lasd_spatial_ref)))
            arcpy.AddError("ERROR: Please create a projection file (.prj) in the LAS folder using the '3D Analyst Tools/Conversion/From File/Point File Information' tool.")
            
        elif las_qainfo.isUnknownSpatialReference():
            las_qainfo.lasd_spatial_ref = None
            arcpy.AddError("ERROR: Spatial Reference for the las files is not standard: '{}'".format(Utility.getSpatialReferenceInfo(las_qainfo.lasd_spatial_ref)))
            arcpy.AddError("ERROR: Please provide a projection file (.prj) that provides a valid transformation in the LAS directory.")
            arcpy.AddError("ERROR:   Please create a projection file (.prj) in the LAS folder using the '3D Analyst Tools/Conversion/From File/Point File Information' tool.")
            
        
        if las_qainfo.lasd_spatial_ref is not None:
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
        
        fileList = getLasFileProcessList(las_qainfo.las_directory, target_path, doRasters, las_qainfo.isClassified)
            createLasStatistics(fileList, target_path, las_qainfo.lasd_spatial_ref, las_qainfo.isClassified, doRasters)
        
        # Create the project's las dataset. Don't do this before you validated that each .las file has a .lasx
        if arcpy.Exists(las_qainfo.las_dataset_path):
            arcpy.AddMessage("Deleting existing LAS Dataset {}".format(las_qainfo.las_dataset_path))
            arcpy.Delete_management(las_qainfo.las_dataset_path)
        
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
                     
        # get the SR object from LAS Dataset
        desc = arcpy.Describe(las_qainfo.las_dataset_path)
        
            # las_qainfo.lasd_spatial_ref = desc.SpatialReference
        las_qainfo.LASDatasetPointCount = desc.pointCount
        las_qainfo.LASDatasetFileCount = desc.fileCount
        
#             if spatial_ref is None:
#                 las_spatial_ref = las_qainfo.lasd_spatial_ref
#                 try:
#                     arcpy.AddMessage("    Using coordinate system found in las files: {}".format(Utility.getSpatialReferenceInfo(las_spatial_ref)))
#                     if not las_qainfo.isValidSpatialReference():
#                         arcpy.AddWarning("Spatial Reference for the las files is not standard. It may not add to the Master correctly.")
#                 except:
#                     pass
        
        
        arcpy.AddMessage("LASDatasetPointCount {} and LASDatasetFileCount {}".format(desc.pointCount, desc.fileCount))
        
#             las_qainfo.isValidSpatialReference()
#             if las_qainfo.isUnknownSpatialReference():
#                 arcpy.AddMessage("Spatial Reference for the las files is 'Unknown'. If missing in the .las file, please provide a .prj file in your LAS folder containing the desired horizontal/vertical coordinate systems.")
#                 arcpy.AddError("Missing spatial reference, CANNOT CONTINUE.")
#                 sys.exit(1)
#             else:
            
            updatedBoundary = A04_C_ConsolidateLASInfo.createLasdBoundaryAndFootprints(las_qainfo.filegdb_path, target_path, ProjectID, ProjectFolder.path, ProjectUID)
    
    bbb = datetime.datetime.now()
    td = (bbb - aaa).total_seconds()
    arcpy.AddMessage("Completed {} in {}".format(las_qainfo.las_dataset_path, td))
    
    return las_qainfo, updatedBoundary



def GenerateQALasDataset(jobID, doRasters):
    Utility.printArguments(["WMXJobID"],
                           [jobID], "A04 GenerateQALasDataset")
    
    arcpy.AddMessage("Checking out licenses")
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")
    
    arcpy.AddMessage("Getting WMX Job Datastore")
    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)

    arcpy.AddMessage("Getting Job from CMDR")
    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)  # @UnusedVariable

    arcpy.AddMessage("Got job {}".format(project))
    
    if project is None:
        arcpy.AddError("Project with Job ID {} not found, CANNOT CONTINUE.".format(jobID)) 
    else:
#         ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
#         ProjectID = ProjectJob.getProjectID(project)
        
#         arcpy.AddMessage("Project '{}' folder '{}'".format(ProjectID, ProjectFolder))
        
        # las_qainfo, updatedBoundary = processProject(ProjectJob, project, doRasters)
        processProject(ProjectJob, project, doRasters)
        
        
        # updateCMDR(ProjectJob, project, las_qainfo, updatedBoundary)
                            
    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")
    arcpy.AddMessage("Operation complete")


if __name__ == '__main__':
    
#     projId = sys.argv[1]
#      
#     doRasters = False
#     if len(sys.argv) > 2:
#         doRasters = sys.argv[2]
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
                
    ProjectJob = ProjectJob()
    project = [
               UID,  # field_ProjectJob_UID
               wmx_job_id,  # field_ProjectJob_WMXJobID,
               project_Id,  # field_ProjectJob_ProjID,
               alias,  # field_ProjectJob_Alias
               alias_clean,  # field_ProjectJob_AliasClean
               state ,  # field_ProjectJob_State
               year ,  # field_ProjectJob_Year
               parent_dir,  # field_ProjectJob_ParentDir
               archive_dir,  # field_ProjectJob_ArchDir
               project_dir,  # field_ProjectJob_ProjDir
               project_AOI  # field_ProjectJob_SHAPE
               ]
     
     
    processProject(ProjectJob, project, True)
