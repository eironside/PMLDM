'''
Created on Dec 7, 2015

@author: eric5946
'''
import arcpy
import os
import shutil
from subprocess import PIPE
import subprocess
import sys
import time
import traceback

from ngce import Utility
from ngce.cmdr import CMDR, CMDRConfig
from ngce.folders import ProjectFolders
from ngce.las import LASConfig , LAS


arcpy.env.overwriteOutput = True
TOOLS_PATH = r"Q:\elevation\WorkflowManager\Tools\ngce\pmdm\a"

def createTargetPolygonFC(filegdb_path, fc_out_name, template_path, spatial_reference):
    Utility.printArguments(["filegdb_path", "fc_out_name", "template_path", "spatial_reference"],
                           [filegdb_path, fc_out_name, template_path, spatial_reference], "createTargetPolygonFC")
    fcOUT = arcpy.CreateFeatureclass_management(filegdb_path, fc_out_name, "POLYGON", "", "", "", spatial_reference)
    # acquire field list from the input table
    desc = arcpy.Describe(template_path)
    fieldListComplete = desc.fields
    # limit field list to all fields except OBJECT_ID
    fieldList = fieldListComplete[1:]
    # create fields in the output feature class
    origfieldnamelist = []
    newfieldnamelist = []
    for i in fieldList:
        arcpy.AddField_management(fcOUT, i.name, i.type, "", "", i.length)
        origfieldnamelist.append(i.name)
        newfieldnamelist.append(i.name)
    
    newfieldnamelist.append('SHAPE@')
    
    return fcOUT, [origfieldnamelist, newfieldnamelist]


def getUpdatedBoundary(las_boundary_path):
    Utility.printArguments(["las_boundary_path"],
                           [las_boundary_path], "getUpdatedBoundary")

    boundary_shape = None  # Utility.getExistingRecord(in_table=las_boundary_path, field_names=['OBJECTID', 'SHAPE@'], uidIndex=-1)[0][1]
    
    bound_XMin = None
    bound_YMin = None
    bound_XMax = None
    bound_YMax = None 
    scurs1 = arcpy.da.SearchCursor(in_table=las_boundary_path, field_names=['OBJECTID', 'SHAPE@'], spatial_reference=arcpy.SpatialReference(4326))  # @UndefinedVariable
    for row in scurs1:
        boundary_shape = row[1]
        
        bound_XMin = boundary_shape.extent.XMin
        bound_YMin = boundary_shape.extent.YMin
        bound_XMax = boundary_shape.extent.XMax
        bound_YMax = boundary_shape.extent.YMax 
        
        break
    
    del scurs1
    
    return boundary_shape, [[bound_XMin, bound_YMin], [bound_XMax, bound_YMax]]


def updateBoundaryShapes(boundary_shape, lasd_name, source_fc, target_fc, field_list_old, field_list_new):
    Utility.printArguments(["boundary_shape", "lasd_name", "source_fc", "target_fc", "field_list_old", "field_list_new"],
                           [boundary_shape, lasd_name, source_fc, target_fc, field_list_old, field_list_new], "updateBoundaryShapes")
    
    where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(source_fc, CMDRConfig.field_LASFileInfo_File_Name), lasd_name)
    cursor_i = arcpy.da.InsertCursor(target_fc, field_list_new)  # @UndefinedVariable
    for r in arcpy.da.SearchCursor(source_fc, field_list_old, where_clause=where_clause):  # @UndefinedVariable
        row = r + (boundary_shape,)
        cursor_i.insertRow(row)
        arcpy.AddMessage("Updating {} record: {}".format(source_fc, row))
    del cursor_i
    
def updateGeometries(las_boundary_path, las_footprint_path, las_fileinfo_path, las_stats_dataset_path, filegdb_path, spatial_reference, lasd_name, las_summary_path):
    Utility.printArguments(["las_boundary_path", "las_footprint_path", "las_fileinfo_path", "las_stats_dataset_path", "filegdb_path", "spatial_reference", "lasd_name", "las_summary_path"],
                           [las_boundary_path, las_footprint_path, las_fileinfo_path, las_stats_dataset_path, filegdb_path, spatial_reference, lasd_name, las_summary_path], "updateGeometries")

    lasdStatInfo_path = os.path.join(filegdb_path, CMDRConfig.fcName_LASDStatInfo) 
    lasFileSum_path = os.path.join(filegdb_path, CMDRConfig.fcName_LASFileSummary)
    if (arcpy.Exists(las_stats_dataset_path) and arcpy.Exists(las_summary_path)) or not arcpy.Exists(lasdStatInfo_path) or not arcpy.Exists(lasFileSum_path):
        if arcpy.Exists(lasdStatInfo_path):
            arcpy.Delete_management(lasdStatInfo_path)
            Utility.addToolMessages()
        
        if arcpy.Exists(lasFileSum_path):
            arcpy.Delete_management(lasFileSum_path)
            Utility.addToolMessages()
            
        lasdStatInfo_path, lasdStatInfo_field_lists = createTargetPolygonFC(filegdb_path, CMDRConfig.fcName_LASDStatInfo, las_stats_dataset_path, spatial_reference)
        lasFileSum_path, lasFileSum_field_lists = createTargetPolygonFC(filegdb_path, CMDRConfig.fcName_LASFileSummary, las_summary_path, spatial_reference)

        boundary_shape, extents = getUpdatedBoundary(las_qainfo.las_boundary_path)
        updateBoundaryShapes(boundary_shape, lasd_name, las_stats_dataset_path, lasdStatInfo_path, lasdStatInfo_field_lists[0], lasdStatInfo_field_lists[1])
        updateBoundaryShapes(boundary_shape, lasd_name, las_summary_path, lasFileSum_path, lasFileSum_field_lists[0], lasFileSum_field_lists[1])
        
        las_footprint_names = Utility.getFieldValues(in_table=las_footprint_path, field_name='Name')
        for name in las_footprint_names:
            where_clause = "{} = '{}'".format(arcpy.AddFieldDelimiters(las_footprint_path, "Name"), name)
            footprint_shape = Utility.getExistingRecord(in_table=las_footprint_path, field_names=['OBJECTID', 'SHAPE@'], where_clause=where_clause, uidIndex=-1)[0][1]
            
            where_clause = "{} = '{}.las'".format(arcpy.AddFieldDelimiters(las_fileinfo_path, "File_Name"), name)
            Utility.addOrUpdateRecord(in_table=las_fileinfo_path, field_names=["Shape@"], uidIndex=-1, rowValueList=[footprint_shape], where_clause=where_clause, editSession=False)
            
            where_clause = "{} = '{}.las'".format(arcpy.AddFieldDelimiters(las_stats_dataset_path, "File_Name"), name)
            edit = Utility.startEditingSession()
            cursor_i = arcpy.da.InsertCursor(lasdStatInfo_path, lasdStatInfo_field_lists[1])  # @UndefinedVariable
            for r in arcpy.da.SearchCursor(las_stats_dataset_path, lasdStatInfo_field_lists[0], where_clause=where_clause):  # @UndefinedVariable
                row = r + (footprint_shape,)
                cursor_i.insertRow(row)
                arcpy.AddMessage("Updating {} record: {}".format(las_stats_dataset_path, row))
            Utility.stopEditingSession(edit)
            del cursor_i
        
        if arcpy.Exists(las_stats_dataset_path):
            arcpy.Delete_management(las_stats_dataset_path)
            Utility.addToolMessages()    
        
        if arcpy.Exists(las_summary_path):
            arcpy.Delete_management(las_summary_path)
            Utility.addToolMessages()
    
    LASDStatInfo = CMDR.LASDStatInfo()
    arcpy.AddMessage("Updating CMDR LAS Stat Info {} with {}".format(lasdStatInfo_path, CMDRConfig.fields_LASDStatInfo))
    for r in arcpy.da.SearchCursor(lasdStatInfo_path, CMDRConfig.fields_LASDStatInfo):  # @UndefinedVariable
        LASDStatInfo.saveOrUpdate(r)
        
    LASFileInfo = CMDR.LASFileInfo()
    arcpy.AddMessage("Updating CMDR {} with {}".format(las_fileinfo_path, CMDRConfig.fields_LASFileInfo))
    for r in arcpy.da.SearchCursor(las_fileinfo_path, CMDRConfig.fields_LASFileInfo):  # @UndefinedVariable
        LASFileInfo.saveOrUpdate(r)
    
    LASFileSummary = CMDR.LASFileSummary()
    arcpy.AddMessage("Updating CMDR LAS File Info {} with {}".format(lasFileSum_path, CMDRConfig.fields_LASFileSum))
    for r in arcpy.da.SearchCursor(lasFileSum_path, CMDRConfig.fields_LASFileSum):  # @UndefinedVariable
        LASFileSummary.saveOrUpdate(r)
    
    
#     return boundary_shape, [[bound_XMin, bound_YMin], [bound_XMax, bound_YMax]]



def createLasStatistics(start_dir, spatial_reference):
    Utility.printArguments(["start_dir", "spatial_reference"],
                           [start_dir, spatial_reference], "createLasStatistics")
    
    ext = ".las"
    path = os.path.join(TOOLS_PATH, "A0401_CreateLASStats.py")
    
    env = os.environ.copy()
    env['PYTHONPATH'] = r'C:\Python27\ArcGISx6410.3\Lib\site-packages'
    env['PATH'] = r'C:\Python27\ArcGISx6410.3'
    exe = r'"C:\Python27\ArcGISx6410.3\pythonw.exe"'
    # eric exe = r'"C:\Python27\ArcGISx6410.4\pythonw.exe"'
    
    procCount = int(os.environ['NUMBER_OF_PROCESSORS'])
    if int(procCount)>4:
        procCount = int(procCount)-4
    arcpy.AddMessage("createLasStatistics: Using {} Processes".format(procCount))
    processList = []
    fileList = []

    arcpy.AddMessage("createLasStatistics: Starting in dir {}".format(start_dir))
    for root, dirs, files in os.walk(start_dir):  # @UnusedVariable
        
        for f in files:
            if f.upper().endswith(ext.upper()):
                subdir = os.path.join(",".join(dirs))
                f_path = os.path.join(root, subdir, f)
                
                # Check to see if stats exist
                s_path = "{}x".format(f_path)
# #                if not arcpy.Exists(s_path):
                if not os.path.isfile(s_path):
                    fileList.append(f_path)
    indx = 0
    total = len(fileList)
    for f_path in fileList:
        indx = indx+1
        s_path = "{}x".format(f_path)
        
        arcpy.AddMessage('createLasStatistics: Working on {}/{}: {}'.format(indx, total, f_path))
        args = [exe, '\"{}\"'.format(path), '\"{}\"'.format(f_path)]
        if spatial_reference is not None:
            args.append('"{}"'.format(spatial_reference.exportToString()))

        try:
##            arcpy.AddMessage('createLasStatistics: LEN Process List = {} Starting process with command: {}'.format(len(processList), args))
            process = subprocess.Popen(" ".join(args), env=env, shell=False, stdout=PIPE, stderr=PIPE)
            processList.append(process)
            time.sleep(10)
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
        first= True
        while waitForResults:   
##            arcpy.AddMessage('createLasStatistics: Looping LEN Process List = {} ProcCount = {} is greater = {}'.format(len(processList), procCount, (len(processList) >= procCount)))
            processRemoved = []
            for p in processList:
                if p.poll() is not None:
                    processRemoved.append(p)
                    out, err = p.communicate(None)
                    retCode = p.returncode
                    if out is not None and len(out) > 0:
                        arcpy.AddMessage("createLasStatistics: OUT {}".format(str(out).rstrip("\r").rstrip("\n").rstrip("\r")))
                    if err is not None and len(err) > 0:
                        arcpy.AddWarning("createLasStatistics: ERR {}".format(str(err).rstrip("\r").rstrip("\n").rstrip("\r")))
                    if retCode != 0:
                        arcpy.AddWarning("createLasStatistics: Failed with return code {}".format(retCode))
            if len(processRemoved) >0:
##                arcpy.AddMessage('createLasStatistics: Removing {} completed jobs'.format(len(processRemoved)))
                for p in processRemoved:
                    processList.remove(p)
            elif not first:
                time.sleep(10)
            first= False
            waitForResults = (len(processList) >= int(procCount))
            
                
        
                    
##                else:
##                    arcpy.AddMessage('createLasStatistics: os.path says lasx statistics file exists for {}'.format(s_path))
                    
    # Wait for last subprocesses to complete
    arcpy.AddMessage("createLasStatistics: Waiting for process list to clear {} jobs".format(len(processList)))
    while len(processList) > 0:
        for p in processList:
            if p.poll() is not None:
                out, err = p.communicate(None)
                retCode = p.returncode
                if out is not None and len(out) > 0:
                    arcpy.AddMessage("createLasStatistics: OUT {}".format(str(out).rstrip("\r").rstrip("\n").rstrip("\r")))
                if err is not None and len(err) > 0:
                    arcpy.AddWarning("createLasStatistics: ERR {}".format(str(err).rstrip("\r").rstrip("\n").rstrip("\r")))
                if retCode != 0:
                    arcpy.AddWarning("createLasStatistics: Failed with return code {}".format(retCode))
                processList.remove(p)
            else:
##                arcpy.AddMessage("createLasStatistics: Waiting for process list to clear {} jobs".format(len(processList)))
                time.sleep(2)
    
    arcpy.AddMessage('createLasStatistics: All jobs completed.')









def createLasStatRasters(las_qainfo):
    # #PULSE_COUNT ?The number of last return points.
    # #POINT_COUNT ?The number of points from all returns.
    # #PREDOMINANT_LAST_RETURN ?The most frequent last return value.
    # #PREDOMINANT_CLASS ?The most frequent class code.
    # #INTENSITY_RANGE ?The range of intensity values.
    # #Z_RANGE ?The range of elevation values.
    
    Utility.printArguments(["las_qainfo"],
                           [las_qainfo], "createLasStatRasters")

    
                        
    path = os.path.join(TOOLS_PATH, "A0402_CreateLASStatsRaster.py")
    
    env = os.environ.copy()
    env['PYTHONPATH'] = r'C:\Python27\ArcGISx6410.3\Lib\site-packages'
    env['PATH'] = r'C:\Python27\ArcGISx6410.3'
    exe = r'"C:\Python27\ArcGISx6410.3\pythonw.exe"'
    
    processList = []
    indx = 0
    
    lasd = las_qainfo.las_dataset_path
    sampl_type = "CELLSIZE"
    stats_methods = ["PULSE_COUNT", "POINT_COUNT", "PREDOMINANT_LAST_RETURN", "PREDOMINANT_CLASS", "INTENSITY_RANGE", "Z_RANGE"]
    cellSize = las_qainfo.LASQARaster_cellSize  # Do our best to get 10  Meters
    for method in stats_methods:    
        out_folder = os.path.join(las_qainfo.target_path, method)
##        arcpy.AddMessage("createLasStatRasters: LAS statistics folder {}".format(out_folder))
        if not os.path.exists(out_folder):
            # shutil.rmtree(out_folder)
            os.mkdir(out_folder)
##            arcpy.AddMessage("createLasStatRasters: LAS statistics folder created {}".format(out_folder))
        for name in ['_ALL', "_FIRST", "_LAST"]:
        
            out_raster = os.path.join(out_folder, "{}{}.tif".format(method, name))
            if not arcpy.Exists(out_raster):
                indx = indx+1
                arcpy.AddMessage("createLasStatRasters: Creating {}{} raster at {}".format(method, name, out_raster))
            
                if indx>1:
                    time.sleep(60)
                args = [exe, '\"{}\"'.format(path), '\"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\"'.format(method, out_raster, lasd, sampl_type, cellSize, out_folder, name)]
                process = subprocess.Popen(" ".join(args), env=env, shell=False, stdout=PIPE, stderr=PIPE)
                processList.append(process)
                
            else:
                arcpy.AddMessage('createLasStatRasters: Skipping {}{} raster. It already exists at {}'.format(method, name, out_raster))

    if len(processList) > 0:
        arcpy.AddMessage("createLasStatRasters: Waiting for {} jobs to complete".format(len(processList)))
    notFinished = None in [p.poll() for p in processList]
    while notFinished:
        time.sleep(10)
        notFinished = None in [p.poll() for p in processList]
    
    for p in processList:
        if p.poll() is not None:
            out, err = p.communicate(None)
            retCode = p.returncode
            if out is not None and len(out) > 0:
                arcpy.AddMessage("createLasStatRasters:  OUT {}".format(str(out).rstrip("\r").rstrip("\n").rstrip("\r")))
            if err is not None and len(err) > 0:
                arcpy.AddWarning("createLasStatRasters:  ERR {}".format(str(err).rstrip("\r").rstrip("\n").rstrip("\r")))
            if retCode != 0:
                arcpy.AddWarning("createLasStatRasters:  Failed with return code {}".format(retCode))
                    
    arcpy.AddMessage('createLasStatRasters: All jobs completed.')
    


def generatePointFileInfo(ProjectUID, ProjectID, isClassified, las_qainfo, las_spatial_ref):
    if not arcpy.Exists(las_qainfo.feature_dataset_path):
        arcpy.AddMessage("    Calculating point file information '{}'".format(las_qainfo.feature_dataset_path))
        arcpy.PointFileInformation_3d(input=las_qainfo.las_directory, out_feature_class=las_qainfo.feature_dataset_path, in_file_type="LAS", file_suffix=".las", input_coordinate_system=las_spatial_ref, folder_recursion="RECURSION", extrude_geometry="NO_EXTRUSION", decimal_separator="DECIMAL_POINT", summarize_by_class_code="NO_SUMMARIZE", improve_las_point_spacing="LAS_SPACING")
        Utility.addToolMessages()
        Utility.addAndCalcFieldLong(las_qainfo.feature_dataset_path, "Class")
        feature_dataset_path_temp = '{}1'.format(las_qainfo.feature_dataset_path)
        arcpy.AddMessage('    calculating point file information with class summary')
        arcpy.PointFileInformation_3d(input=las_qainfo.las_directory, out_feature_class=feature_dataset_path_temp, in_file_type="LAS", file_suffix=".las", input_coordinate_system=las_spatial_ref, folder_recursion="RECURSION", extrude_geometry="NO_EXTRUSION", decimal_separator="DECIMAL_POINT", summarize_by_class_code="SUMMARIZE", improve_las_point_spacing="NO_LAS_SPACING")
        Utility.addToolMessages()
        arcpy.Append_management(inputs=feature_dataset_path_temp, target=las_qainfo.feature_dataset_path, schema_type="TEST", field_mapping="", subtype="")
        Utility.addToolMessages()
        if arcpy.Exists(feature_dataset_path_temp):
            arcpy.Delete_management(in_data=feature_dataset_path_temp, data_type="FeatureClass")
            Utility.addToolMessages()
        # @TODO Sort these fields out into a config file
        Utility.addAndCalcFieldFloat(las_qainfo.feature_dataset_path, CMDRConfig.field_LASFileInfo_File_Z_Range, "!Z_max! - !Z_min!", CMDRConfig.field_LASFileInfo_File_Z_Range.replace("_", " "))
        Utility.addAndCalcFieldText(las_qainfo.feature_dataset_path, CMDRConfig.field_LASFileInfo_ProjID, "100", '"{}"'.format(ProjectID), CMDRConfig.field_LASFileInfo_ProjID.replace("_", " "))
        Utility.addAndCalcFieldText(las_qainfo.feature_dataset_path, CMDRConfig.field_LASFileInfo_File_Path, "500", """"\{}" + "/" + !FileName!""".format(las_qainfo.las_directory), CMDRConfig.field_LASFileInfo_File_Path.replace("_", " "))
        Utility.addAndCalcFieldGUID(las_qainfo.feature_dataset_path, CMDRConfig.field_LASFileInfo_UID, '"{}"'.format(ProjectUID), CMDRConfig.field_LASFileInfo_UID.replace("_", " "))
        # Utility.addAndCalcFieldText(las_qainfo.feature_dataset_path, CMDRConfig.field_LASFileInfo_File_Link_MapService, "900")
        Utility.addAndCalcFieldText(las_qainfo.feature_dataset_path, CMDRConfig.field_LASFileInfo_File_LAS_Classifed, "10", '"{}"'.format(isClassified), CMDRConfig.field_LASFileInfo_File_LAS_Classifed.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.feature_dataset_path, field="FileName", new_field_name=CMDRConfig.field_LASFileInfo_File_Name, new_field_alias=CMDRConfig.field_LASFileInfo_File_Name.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.feature_dataset_path, field="Pt_Spacing", new_field_name=CMDRConfig.field_LASFileInfo_File_PointSpacing, new_field_alias=CMDRConfig.field_LASFileInfo_File_PointSpacing.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.feature_dataset_path, field="Pt_Count", new_field_name=CMDRConfig.field_LASFileInfo_File_PointCount, new_field_alias=CMDRConfig.field_LASFileInfo_File_PointCount.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.feature_dataset_path, field="Z_Min", new_field_name=CMDRConfig.field_LASFileInfo_File_Z_Min, new_field_alias=CMDRConfig.field_LASFileInfo_File_Z_Min.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.feature_dataset_path, field="Z_Max", new_field_name=CMDRConfig.field_LASFileInfo_File_Z_Max, new_field_alias=CMDRConfig.field_LASFileInfo_File_Z_Max.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.feature_dataset_path, field="Class", new_field_name=CMDRConfig.field_LASFileInfo_File_LAS_Class, new_field_alias=CMDRConfig.field_LASFileInfo_File_LAS_Class.replace("_", " "))
        arcpy.Statistics_analysis(in_table=las_qainfo.feature_dataset_path, out_table=las_qainfo.las_summary_path, statistics_fields="{} SUM;{} MEAN;{} MIN;{} MAX".format(CMDRConfig.field_LASFileInfo_File_PointCount, CMDRConfig.field_LASFileInfo_File_PointSpacing, CMDRConfig.field_LASFileInfo_File_Z_Min, CMDRConfig.field_LASFileInfo_File_Z_Max), case_field=CMDRConfig.field_LASFileInfo_File_LAS_Class)
        Utility.addToolMessages()
        Utility.addAndCalcFieldFloat(las_qainfo.las_summary_path, CMDRConfig.field_LASFileInfo_File_Z_Range, "!MAX_{}! - !MIN_{}!".format(CMDRConfig.field_LASFileInfo_File_Z_Max, CMDRConfig.field_LASFileInfo_File_Z_Min), CMDRConfig.field_LASFileInfo_File_Z_Range.replace("_", " "))
        Utility.addAndCalcFieldText(las_qainfo.las_summary_path, CMDRConfig.field_LASFileInfo_ProjID, "100", '"{}"'.format(ProjectID), CMDRConfig.field_LASFileInfo_ProjID.replace("_", " "))
        Utility.addAndCalcFieldText(las_qainfo.las_summary_path, CMDRConfig.field_LASFileInfo_File_Path, "500", '"\{}"'.format(las_qainfo.las_dataset_path), CMDRConfig.field_LASFileInfo_File_Path.replace("_", " "))
        Utility.addAndCalcFieldText(las_qainfo.las_summary_path, CMDRConfig.field_LASFileInfo_File_Name, "100", '"{}"'.format(las_qainfo.las_dataset_name), CMDRConfig.field_LASFileInfo_File_Name.replace("_", " "))
        Utility.addAndCalcFieldGUID(las_qainfo.las_summary_path, CMDRConfig.field_LASFileInfo_UID, '"{}"'.format(ProjectUID), CMDRConfig.field_LASFileInfo_UID.replace("_", " "))
        Utility.addAndCalcFieldText(las_qainfo.las_summary_path, CMDRConfig.field_LASFileInfo_File_LAS_Classifed, "10", '"{}"'.format(isClassified), CMDRConfig.field_LASFileInfo_File_LAS_Classifed.replace("_", " "))
        # Utility.addAndCalcFieldText(las_qainfo.las_summary_path, CMDRConfig.field_LASFileInfo_File_Link_MapService, "900")
        arcpy.AlterField_management(in_table=las_qainfo.las_summary_path, field="MEAN_{}".format(CMDRConfig.field_LASFileInfo_File_PointSpacing), new_field_name=CMDRConfig.field_LASFileInfo_File_PointSpacing, new_field_alias=CMDRConfig.field_LASFileInfo_File_PointSpacing.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_summary_path, field="SUM_{}".format(CMDRConfig.field_LASFileInfo_File_PointCount), new_field_name=CMDRConfig.field_LASFileInfo_File_PointCount, new_field_alias=CMDRConfig.field_LASFileInfo_File_PointCount.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_summary_path, field="MIN_{}".format(CMDRConfig.field_LASFileInfo_File_Z_Min), new_field_name=CMDRConfig.field_LASFileInfo_File_Z_Min, new_field_alias=CMDRConfig.field_LASFileInfo_File_Z_Min.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_summary_path, field="MAX_{}".format(CMDRConfig.field_LASFileInfo_File_Z_Max), new_field_name=CMDRConfig.field_LASFileInfo_File_Z_Max, new_field_alias=CMDRConfig.field_LASFileInfo_File_Z_Max.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_summary_path, field="FREQUENCY", new_field_name=CMDRConfig.field_LASFileSummary_File_LAS_Count, new_field_alias=CMDRConfig.field_LASFileSummary_File_LAS_Count.replace("_", " "))
    else:
        arcpy.AddMessage("    Using existing LAS Point File Information {}".format(las_qainfo.feature_dataset_path))


def createLasdStatInfo(ProjectUID, ProjectID, isClassified, las_qainfo, lasd_all):
    # _________________________________________________________________________________________
    # arcpy.AddMessage("\tExporting LASD data set and file statistics")
    #                             if arcpy.Exists(las_qainfo.las_stats_dataset_file_path):
    #                                 arcpy.AddMessage("    Deleting LAS Dataset Statistics '{}'".format(las_qainfo.las_stats_dataset_file_path))
    #                                 arcpy.Delete_management(in_data=las_qainfo.las_stats_dataset_file_path)
    #                                 Utility.addToolMessages()
    if not arcpy.Exists(las_qainfo.las_stats_dataset_file_path):
        arcpy.AddMessage("    Calculating LAS Dataset Statistics '{}'".format(las_qainfo.las_stats_dataset_file_path))
        arcpy.LasDatasetStatistics_management(in_las_dataset=lasd_all, # las_qainfo.las_dataset_path,
            calculation_type="SKIP_EXISTING_STATS", # "OVERWRITE_EXISTING_STATS",
            out_file=las_qainfo.las_stats_dataset_file_path, summary_level="DATASET", delimiter="COMMA", decimal_separator="DECIMAL_POINT")
        Utility.addToolMessages()
        arcpy.AddMessage("    LAS Dataset Statistics '{}'".format(las_qainfo.las_stats_dataset_file_path))
        arcpy.TableToTable_conversion(in_rows=las_qainfo.las_stats_dataset_file_path, out_path=las_qainfo.filegdb_path, out_name=LASConfig.LASDataset_DatasetStats_Name, where_clause="", field_mapping="""Item "Item" true true false 8000 Text 0 0 ,First,#,{0},Item,-1,-1;Category "Category" true true false 8000 Text 0 0 ,First,#,{0},Category,-1,-1;Pt_Cnt "Pt_Cnt" true true false 8 Double 0 0 ,First,#,{0},Pt_Cnt,-1,-1;Percent "Percent" true true false 8 Double 0 0 ,First,#,{0},Percent,-1,-1;Z_Min "Z_Min" true true false 8 Double 0 0 ,First,#,{0},Z_Min,-1,-1;Z_Max "Z_Max" true true false 8 Double 0 0 ,First,#,{0},Z_Max,-1,-1;Intensity_Min "Intensity_Min" true true false 8 Double 0 0 ,First,#,{0},Intensity_Min,-1,-1;Intensity_Max "Intensity_Max" true true false 8 Double 0 0 ,First,#,{0},Intensity_Max,-1,-1;Synthetic_Pt_Cnt "Synthetic_Pt_Cnt" true true false 8 Double 0 0 ,First,#,{0},Synthetic_Pt_Cnt,-1,-1;Range_Min "Range_Min" true true false 8 Double 0 0 ,First,#,{0},Range_Min,-1,-1;Range_Max "Range_Max" true true false 8 Double 0 0 ,First,#,{0},Range_Max,-1,-1""".format(las_qainfo.las_stats_dataset_file_path), config_keyword="")
        Utility.addToolMessages()
        Utility.addAndCalcFieldText(las_qainfo.las_stats_dataset_path, "File_Name", "500", '"\{}"'.format(las_qainfo.las_dataset_path))
    else:
        arcpy.AddMessage("    Using existing LAS Dataset statistics file {}".format(las_qainfo.las_stats_dataset_file_path))
    
    if not arcpy.Exists(las_qainfo.las_stats_files_file_path):
        arcpy.AddMessage("    Calculating LAS File Statistics '{}'".format(las_qainfo.las_stats_files_file_path))
        arcpy.LasDatasetStatistics_management(in_las_dataset=lasd_all, # las_qainfo.las_dataset_path,
            calculation_type="SKIP_EXISTING_STATS", out_file=las_qainfo.las_stats_files_file_path, summary_level="LAS_FILES", delimiter="COMMA", decimal_separator="DECIMAL_POINT")
        Utility.addToolMessages()
        arcpy.AddMessage("    LAS File Statistics '{}'".format(las_qainfo.las_stats_files_file_path))
        arcpy.TableToTable_conversion(in_rows=las_qainfo.las_stats_files_file_path, out_path=las_qainfo.filegdb_path, out_name=LASConfig.LASDataset_FileStats_Name, where_clause="", field_mapping="""File_Name "File_Name" true true false 8000 Text 0 0 ,First,#,{0},File_Name,-1,-1;Item "Item" true true false 8000 Text 0 0 ,First,#,{0},Item,-1,-1;Category "Category" true true false 8000 Text 0 0 ,First,#,{0},Category,-1,-1;Pt_Cnt "Pt_Cnt" true true false 8 Double 0 0 ,First,#,{0},Pt_Cnt,-1,-1;Percent "Percent" true true false 8 Double 0 0 ,First,#,{0},Percent,-1,-1;Z_Min "Z_Min" true true false 8 Double 0 0 ,First,#,{0},Z_Min,-1,-1;Z_Max "Z_Max" true true false 8 Double 0 0 ,First,#,{0},Z_Max,-1,-1;Intensity_Min "Intensity_Min" true true false 8 Double 0 0 ,First,#,{0},Intensity_Min,-1,-1;Intensity_Max "Intensity_Max" true true false 8 Double 0 0 ,First,#,{0},Intensity_Max,-1,-1;Synthetic_Pt_Cnt "Synthetic_Pt_Cnt" true true false 8 Double 0 0 ,First,#,{0},Synthetic_Pt_Cnt,-1,-1;Range_Min "Range_Min" true true false 8 Double 0 0 ,First,#,{0},Range_Min,-1,-1;Range_Max "Range_Max" true true false 8 Double 0 0 ,First,#,{0},Range_Max,-1,-1""".format(las_qainfo.las_stats_files_file_path), config_keyword="")
        Utility.addToolMessages()
        arcpy.Append_management(inputs=las_qainfo.las_stats_files_path, target=las_qainfo.las_stats_dataset_path, schema_type="TEST", field_mapping="", subtype="")
        Utility.addToolMessages()
        if arcpy.Exists(las_qainfo.las_stats_files_path):
            arcpy.Delete_management(in_data=las_qainfo.las_stats_files_path, data_type="Table")
            Utility.addToolMessages()
        # @TODO sort these fields out into a config file
        # @TODO add file type "LASD" or "LAS" based on file extension, remove extension from File_Name
        arcpy.AlterField_management(in_table=las_qainfo.las_stats_dataset_path, field="File_Name", new_field_name=CMDRConfig.field_LASFileInfo_File_Path, new_field_alias=CMDRConfig.field_LASFileInfo_File_Path.replace("_", " "), field_type="TEXT", field_length="500", field_is_nullable="NULLABLE", clear_field_alias="false")
        Utility.addAndCalcFieldText(dataset_path=las_qainfo.las_stats_dataset_path, field_name=CMDRConfig.field_LASFileInfo_File_Name, field_length="100", field_value="trimField( !File_Path! )", field_alias=CMDRConfig.field_LASFileInfo_File_Name.replace("_", " "), code_block="def trimField(name):\n   import os\n   return os.path.split(name)[1]")
        Utility.addAndCalcFieldFloat(las_qainfo.las_stats_dataset_path, CMDRConfig.field_LASFileInfo_File_Z_Range, "!Z_Max! - !Z_Min!", CMDRConfig.field_LASFileInfo_File_Z_Range.replace("_", " "))
        Utility.addAndCalcFieldFloat(las_qainfo.las_stats_dataset_path, CMDRConfig.field_LASFileInfo_File_I_Range, "!Intensity_Max! - !Intensity_Min!", CMDRConfig.field_LASFileInfo_File_I_Range.replace("_", " "))
        Utility.addAndCalcFieldText(las_qainfo.las_stats_dataset_path, CMDRConfig.field_LASFileInfo_ProjID, "500", '"{}"'.format(ProjectID), CMDRConfig.field_LASFileInfo_ProjID.replace("_", " "))
        Utility.addAndCalcFieldGUID(las_qainfo.las_stats_dataset_path, CMDRConfig.field_LASFileInfo_UID, '"{}"'.format(ProjectUID), CMDRConfig.field_LASFileInfo_UID.replace("_", " "))
        Utility.addAndCalcFieldText(las_qainfo.las_stats_dataset_path, CMDRConfig.field_LASFileInfo_File_LAS_Classifed, "10", '"{}"'.format(isClassified), CMDRConfig.field_LASFileInfo_File_LAS_Classifed.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_stats_dataset_path, field="Pt_Cnt", new_field_name=CMDRConfig.field_LASFileInfo_File_PointCount, new_field_alias=CMDRConfig.field_LASFileInfo_File_PointCount.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_stats_dataset_path, field="Z_Min", new_field_name=CMDRConfig.field_LASFileInfo_File_Z_Min, new_field_alias=CMDRConfig.field_LASFileInfo_File_Z_Min.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_stats_dataset_path, field="Z_Max", new_field_name=CMDRConfig.field_LASFileInfo_File_Z_Max, new_field_alias=CMDRConfig.field_LASFileInfo_File_Z_Max.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_stats_dataset_path, field="Intensity_Min", new_field_name=CMDRConfig.field_LASFileInfo_File_I_Min, new_field_alias=CMDRConfig.field_LASFileInfo_File_I_Min.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_stats_dataset_path, field="Intensity_Max", new_field_name=CMDRConfig.field_LASFileInfo_File_I_Max, new_field_alias=CMDRConfig.field_LASFileInfo_File_I_Max.replace("_", " "))
        arcpy.AlterField_management(in_table=las_qainfo.las_stats_dataset_path, field=CMDRConfig.PERCENT, new_field_name=CMDRConfig.field_LASDStatInfo_Percent, new_field_alias=CMDRConfig.field_LASDStatInfo_Percent.replace("_", " "))
    else:
        arcpy.AddMessage("    Using existing LAS Dataset file statistics file {}".format(las_qainfo.las_stats_files_file_path))
    



def getProjectDEMStatistics(las_qainfo):
    
    
    fields_LASD_summary = [CMDRConfig.field_LASFileInfo_File_PointCount, CMDRConfig.field_LASFileInfo_File_PointSpacing, CMDRConfig.field_LASFileInfo_File_Z_Min, CMDRConfig.field_LASFileInfo_File_Z_Max]
    where_clause = "{} is NULL".format(arcpy.AddFieldDelimiters(las_qainfo.las_summary_fc_path, CMDRConfig.field_LASFileInfo_File_LAS_Class))
    arcpy.AddMessage("getting LAS Point file information where {}".format(where_clause))
    for r in arcpy.da.SearchCursor(las_qainfo.las_summary_fc_path, fields_LASD_summary, where_clause=where_clause):  # @UndefinedVariable
        arcpy.AddMessage("    LAS Point ALL POINTS File Information {}".format(r))
        las_qainfo.pt_count_dsm = r[0]
        las_qainfo.pt_spacing_dsm = r[1]
        las_qainfo.minZ_dsm = r[2]
        las_qainfo.maxZ_dsm = r[3]
          
        # reuse DSM stats for DTM non-classified data
        las_qainfo.pt_count_dtm = r[0]
        las_qainfo.pt_spacing_dtm = r[1]
        las_qainfo.minZ_dtm = r[2]
        las_qainfo.maxZ_dtm = r[3]
      
    if las_qainfo.isClassified:
        # Get classified data ground & model key for DTM, Can't easily get DTM stats for non-classified data
        where_clause = "{} = 8".format(arcpy.AddFieldDelimiters(las_qainfo.las_summary_path, CMDRConfig.field_LASFileInfo_File_LAS_Class))
        arcpy.AddMessage("getting LAS Point file information where {}".format(where_clause))
        for r in arcpy.da.SearchCursor(las_qainfo.las_summary_fc_path, fields_LASD_summary, where_clause=where_clause):  # @UndefinedVariable
            arcpy.AddMessage("    LAS Point MODEL KEY File Information {}".format(r))
            las_qainfo.pt_count_dtm = r[0]
            las_qainfo.pt_spacing_dtm = r[1]
            las_qainfo.minZ_dtm = r[2]
            las_qainfo.maxZ_dtm = r[3]
            
        where_clause = "{} = 2".format(arcpy.AddFieldDelimiters(las_qainfo.las_summary_path, CMDRConfig.field_LASFileInfo_File_LAS_Class))
        arcpy.AddMessage("getting LAS Point file information where {}".format(where_clause))
        for r in arcpy.da.SearchCursor(las_qainfo.las_summary_fc_path, fields_LASD_summary, where_clause=where_clause):  # @UndefinedVariable
            arcpy.AddMessage("    LAS Point GROUND File Information {}".format(r))
            las_qainfo.pt_count_dtm = r[0]
            las_qainfo.pt_spacing_dtm = r[1]
            las_qainfo.minZ_dtm = r[2]
            las_qainfo.maxZ_dtm = r[3]



def GenerateQALasDataset(jobID):
    Utility.printArguments(["WMXJobID"],
                           [jobID], "A04 GenerateQALasDataset")
    
    arcpy.AddMessage("Checking out licenses")
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")
    
    arcpy.AddMessage("Getting WMX Job Datastore")
    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)

    arcpy.AddMessage("Getting Job from CMDR")
    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)

    arcpy.AddMessage("Got job {}".format(project))
    if project is not None:
        ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
        ProjectID = ProjectJob.getProjectID(project)
    #     ProjectUID = ProjectJob.getUID(project)
        target_path = ProjectFolder.derived.path
        arcpy.AddMessage("Project '{}' folder '{}'".format(ProjectID, ProjectFolder))
        
        
        
        
        foundClassified = False
        for isClassified in [True, False]:
            if not(foundClassified):
                las_qainfo = LAS.QALasInfo(ProjectFolder, isClassified)
                
                # create file geodatabase if it does not exist
#                 if os.path.exists(las_qainfo.filegdb_path):
#                     arcpy.AddMessage("Deleting fGDB '{}'".format(las_qainfo.filegdb_path))
#                     arcpy.Delete_management(in_data=las_qainfo.filegdb_path)
#                     Utility.addToolMessages()
                if not os.path.exists(las_qainfo.filegdb_path):
                    arcpy.AddMessage("creating Derived fGDB sand box '{}'".format(las_qainfo.filegdb_path))
                    arcpy.CreateFileGDB_management(target_path, las_qainfo.filegdb_name)
                    Utility.addToolMessages()
                else:
                    arcpy.AddMessage("Derived fGDB sand box already exists. Using '{}'".format(las_qainfo.filegdb_path))
                    
                if os.path.exists(las_qainfo.las_directory):
                    
                    las_qainfo.num_las_files, las_qainfo.first_las_name = Utility.fileCounter(las_qainfo.las_directory, '.las')
                    arcpy.AddMessage("    {} las files in LasDirectory '{}'".format(las_qainfo.num_las_files, las_qainfo.las_directory))
                    
                    if(las_qainfo.num_las_files > 0):
                        foundClassified = True
                        
                        las_spatial_ref = None
                        prj_Count, prj_File = Utility.fileCounter(las_qainfo.las_directory, '.prj')
                        if prj_Count > 0:
                            las_spatial_ref = las_qainfo.setProjectionFile(prj_File)
                            arcpy.AddMessage("    Found a projection file with the las files, OVERRIDE LAS SR (if set) '{}'".format(las_qainfo.prj_file_name))
                            arcpy.AddMessage(Utility.getSpatialReferenceInfo(las_spatial_ref))
                        else:
                            arcpy.AddMessage("    Using projection (coordinate system) from las files if available.")

                        arcpy.AddMessage("    creating lasx files if they don't exist.")
                        createLasStatistics(las_qainfo.las_directory, las_spatial_ref)
                        arcpy.AddMessage("    created lasx files.")
                        
#                         if arcpy.Exists(las_qainfo.las_dataset_path):
#                             arcpy.AddMessage("    Deleting LAS Dataset '{}'".format(las_qainfo.las_dataset_path))
#                             arcpy.Delete_management(in_data=las_qainfo.las_dataset_path)
#                             Utility.addToolMessages()
                        if not arcpy.Exists(las_qainfo.las_dataset_path):
                            arcpy.AddMessage("    Creating LAS Dataset '{}'".format(las_qainfo.las_dataset_path))
                            arcpy.CreateLasDataset_management(input=las_qainfo.las_directory,
                                                              out_las_dataset=las_qainfo.las_dataset_path,
                                                              folder_recursion="RECURSION",
                                                              in_surface_constraints="",
                                                              spatial_reference=las_spatial_ref,
                                                              compute_stats="COMPUTE_STATS",
                                                              relative_paths="RELATIVE_PATHS")
                            
                            
                            # # Not sure why buty this tool messages call fails
                            Utility.addToolMessages()
                        else:
                            arcpy.AddMessage("    Using existing LAS Dataset {}".format(las_qainfo.las_dataset_path))
                            
                            
                         
                        lasd_all = arcpy.MakeLasDatasetLayer_management(in_las_dataset=las_qainfo.las_dataset_path, out_layer="{}_LasDataset_All".format(ProjectID), class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="'Last Return';'First of Many';'Last of Many';'Single Return';1;2;3;4;5", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                        lasd_first = arcpy.MakeLasDatasetLayer_management(in_las_dataset=las_qainfo.las_dataset_path, out_layer="{}_LasDataset_first".format(ProjectID), class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="1", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                        lasd_ground = arcpy.MakeLasDatasetLayer_management(in_las_dataset=las_qainfo.las_dataset_path, out_layer="{}_LasDataset_last".format(ProjectID), class_code="0;2;8;9;10;11;12", return_values="'Last Return'", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
                        arcpy.AddMessage("    Created LAS Dataset and views ")
                         
                        # get the SR object from LAS Dataset
                        desc = arcpy.Describe(las_qainfo.las_dataset_name)
                        las_qainfo.lasd_spatial_ref = desc.SpatialReference
                        if las_spatial_ref is None:
                            arcpy.AddMessage("    Using coordinate system found in las files.")
                            las_spatial_ref = las_qainfo.lasd_spatial_ref
                            arcpy.AddMessage(Utility.getSpatialReferenceInfo(las_spatial_ref))
                        las_qainfo.LASDatasetPointCount = desc.pointCount
                        las_qainfo.LASDatasetFileCount = desc.fileCount
                        las_qainfo.isValidSpatialReference() 
                        las_qainfo.isUnknownSpatialReference()
                        arcpy.AddMessage("    LASDatasetPointCount {} and LASDatasetFileCount {}".format(desc.pointCount, desc.fileCount))
                         
#                         if las_qainfo.isValidSpatialReference():

                        createLasdStatInfo(ProjectUID, ProjectID, isClassified, las_qainfo, lasd_all)
                            
                       
                        # _________________________________________________________________________________________
                        
                        # calculate point file information
                        
#                         if arcpy.Exists(las_qainfo.feature_dataset_path):
#                             arcpy.AddMessage("    Deleting point file information '{}'".format(las_qainfo.feature_dataset_path))
#                             arcpy.Delete_management(in_data=las_qainfo.feature_dataset_path)
#                             Utility.addToolMessages()
                        generatePointFileInfo(ProjectUID, ProjectID, isClassified, las_qainfo, las_spatial_ref)
                        
                        createLasStatRasters(las_qainfo)
                                    
                        updatedBoundary = None
                        extents = None
                        updatedBoundary_Area = None
                        bound_XMin = None
                        bound_YMin = None
                        bound_XMax = None
                        bound_YMax = None

                        arcpy.AddMessage("Creating LAS mosaic dataset {}".format(las_qainfo.LASMD_Name))
                        if not las_qainfo.isUnknownSpatialReference():
                            if arcpy.Exists(las_qainfo.LASMD_path):
                                arcpy.AddMessage("LAS Mosaic Dataset already found at {}".format(las_qainfo.LASMD_path))
                            else:
                                arcpy.AddMessage("Creating LAS Mosaic Dataset at {}".format(las_qainfo.LASMD_path))
                                # Assumes the raster type xml file is in the same directory as this python file
                                LAS_Raster_type = LAS.LAS_raster_type_20_all_bin_mean_idw 
                                arcpy.AddMessage("LAS Raster Type defined in '{}'".format(LAS_Raster_type))
                              
                                # Create a MD in same SR as LAS Dataset
                                arcpy.CreateMosaicDataset_management(las_qainfo.filegdb_path, las_qainfo.LASMD_Name,
                                                                     coordinate_system=las_qainfo.getSpatialReference(), num_bands="1", pixel_type="32_BIT_FLOAT",
                                                                     product_definition="NONE", product_band_definitions="#")
                                Utility.addToolMessages()
                              
                                # Add the LAS files to the Mosaic Dataset and don't update the boundary yet.
                                # The cell size of the Mosaic Dataset is determined by the art.xml file chosen by the user.
                                arcpy.AddRastersToMosaicDataset_management(las_qainfo.LASMD_path, LAS_Raster_type, las_qainfo.las_directory,
                                                                           update_cellsize_ranges="UPDATE_CELL_SIZES", update_boundary="NO_BOUNDARY", update_overviews="NO_OVERVIEWS",
                                                                           maximum_pyramid_levels="0", maximum_cell_size="0", minimum_dimension="100",
                                                                           spatial_reference=las_qainfo.getSpatialReference(),
                                                                           filter="*.las", sub_folder="SUBFOLDERS", duplicate_items_action="ALLOW_DUPLICATES",
                                                                           build_pyramids="NO_PYRAMIDS", calculate_statistics="NO_STATISTICS",
                                                                           build_thumbnails="NO_THUMBNAILS", operation_description="#",
                                                                           force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE")
                                Utility.addToolMessages()
                                
                                 # Get a count of the number of LAS ingested 
                                result = arcpy.GetCount_management(las_qainfo.LASMD_path)
                                countRowsWithLAS = int(result.getOutput(0))
                                if countRowsWithLAS > 0:
                                    arcpy.AddMessage("{0} has {1} LAS row(s).".format(las_qainfo.LASMD_path, countRowsWithLAS))
                              
                                    # Build Footprints using min_region_size="20" and approx_num_vertices="200". Update the Boundary using the new footprints.
                                    arcpy.BuildFootprints_management(las_qainfo.LASMD_path, where_clause="#", reset_footprint="RADIOMETRY", min_data_value=las_qainfo.getLASFootprintMinDataValue(),
                                                                     max_data_value=las_qainfo.getLASFootprintMaxDataValue(), approx_num_vertices=las_qainfo.LAS_Footprint_APPROX_NUM_VERTS, shrink_distance="0", maintain_edges="MAINTAIN_EDGES",
                                                                     skip_derived_images="SKIP_DERIVED_IMAGES", update_boundary="UPDATE_BOUNDARY", request_size="2000",
                                                                     min_region_size=las_qainfo.LAS_Footprint_MIN_REGION_SIZE, simplification_method="NONE", edge_tolerance="#", max_sliver_size="20",
                                                                     min_thinness_ratio="0.05")
                                  
                                    Utility.addToolMessages()
                                    
                                    LAS.updateMDLASBoundary(las_qainfo.filegdb_path, las_qainfo.LASMD_path, LASConfig.INTERNAL_PART_AREA, las_qainfo.boundary_interval)
                                    LAS.updateMDLASFootprints(las_qainfo.filegdb_path, las_qainfo.LASMD_path, LASConfig.INTERNAL_PART_AREA, las_qainfo.boundary_interval)
                                    
    #                                 updatedBoundary, extents = updateGeometries(las_qainfo.las_boundary_path, las_qainfo.las_footprint_path, las_qainfo.feature_dataset_path, las_qainfo.las_stats_dataset_path, las_qainfo.filegdb_path, las_qainfo.getSpatialReference(), las_qainfo.las_dataset_name, las_qainfo.las_summary_path)
                                    updateGeometries(las_qainfo.las_boundary_path, las_qainfo.las_footprint_path, las_qainfo.feature_dataset_path, las_qainfo.las_stats_dataset_path, las_qainfo.filegdb_path, las_qainfo.getSpatialReference(), las_qainfo.las_dataset_name, las_qainfo.las_summary_path)
                          
                            # Get a count of the number of LAS ingested 
                            result = arcpy.GetCount_management(las_qainfo.LASMD_path)
                            countRowsWithLAS = int(result.getOutput(0))
                            if countRowsWithLAS > 0:
                                arcpy.AddMessage("{0} has {1} LAS row(s).".format(las_qainfo.LASMD_path, countRowsWithLAS))
                                
                                updatedBoundary, extents = getUpdatedBoundary(las_qainfo.las_boundary_path)
    #                             updatedBoundary = Utility.getExistingRecord(in_table=las_qainfo.las_boundary_path, field_names=['OBJECTID', 'SHAPE@'], uidIndex=-1)[0][1]
                                arcpy.AddMessage("Updating boundary area")        
                                updatedBoundary_Area = updatedBoundary.getArea ("PRESERVE_SHAPE", "SQUAREMETERS")
                                
                                arcpy.AddMessage("Updating boundary extents")        
                                bound_XMin = extents[0][0]
                                bound_YMin = extents[0][1]
                                bound_XMax = extents[1][0]
                                bound_YMax = extents[1][1]

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
                            las_qainfo.minZ_dsm, las_qainfo.maxZ_dsm = LAS.validateZRange(las_qainfo.minZ_dsm, las_qainfo.maxZ_dsm)
                            las_qainfo.minZ_dtm, las_qainfo.maxZ_dtm = LAS.validateZRange(las_qainfo.minZ_dtm, las_qainfo.maxZ_dtm)
                            Deliver.setValidZMax(deliver, las_qainfo.maxZ_dsm)
                            if las_qainfo.maxZ_dsm >= LAS.MAX_VALID_ELEVATION:
                                Deliver.setValidZMax(deliver, min(las_qainfo.maxZ_dtm, LAS.MAX_VALID_ELEVATION))
                            Deliver.setValidZMin(deliver, max(las_qainfo.minZ_dtm, LAS.MIN_VALID_ELEVATION))
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
    #                             Publish = CMDR.Publish()
    #                             publish = Publish.getPublish(las_qainfo.ProjectID)
    #                             Publish.updateAOI(publish, updatedBoundary)
                                
                                    
                            
                            if not las_qainfo.isValidSpatialReference():
                                arcpy.AddMessage("Spatial Reference for the las files is not standard. It may not add to the Master correctly.")
                        else:
                            arcpy.AddMessage("Spatial Reference for the las files is 'Unknown'. If missing in the .las file, please provide a .prj file in your LAS folder containing the desired horizontal/vertical coordinate systems.")
                            arcpy.AddError("Missing spatial reference, CANNOT CONTINUE.")
                    else:
                        arcpy.AddMessage("Ignoring las directory. No Las Files Found '{}'".format(las_qainfo.las_directory))
                else:
                    arcpy.AddMessage("Ignoring las directory. It doesn't exist: '{}'".format(las_qainfo.las_directory))
            else:
                arcpy.AddMessage("Ignoring unclassifed las directory, already processed classified las")
    else:
        arcpy.AddError("Project with Job ID {} not found, CANNOT CONTINUE.".format(jobID)) 

    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")
    arcpy.AddMessage("Operation complete")


if __name__ == '__main__':
    projId = sys.argv[1]
    GenerateQALasDataset(projId)
    
