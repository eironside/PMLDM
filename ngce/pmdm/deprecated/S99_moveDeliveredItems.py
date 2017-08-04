import arcpy
import os
import shutil


ORIGINAL_DIR = "ORIGINAL"
DELIVERED_DIR = "DELIVERED"

BREAKS_DIR = "BREAKS"
CONTROL_DIR = "CONTROL"
DTM_DIR = "DTM"
DSM_DIR = "DSM"
INTENSITY_DIR = "INTENSITY"
LAS_CLASSIFIED_DIR = "LAS_CLASSIFIED"
LAS_INVALID_DIR = "LAS_INVALID"
LAS_UNCLASSIFIED_DIR = "LAS_UNCLASSIFIED"
QA_DIR = "QA"
TILE_INDEX_DIR = "TILE_INDEX"
TILES_DIR = "TILES"

def cleanInput(inputString):
    if inputString is not None and len(str(inputString).strip()):
        inputString = str(inputString).strip()
    else:
        inputString = None
    return inputString

def createPath(basePath, midDir, endDir):
    basePath = cleanInput(basePath)
    midDir = cleanInput(midDir)
    endDir = cleanInput(endDir)
    path = None
    if basePath is not None and endDir is not None:
        path = basePath
        if midDir is not None:
            basePath = os.path.join(basePath, midDir)
        path = os.path.join(basePath, endDir)
    
    return path

try:
    proj_directory = cleanInput(arcpy.GetParameterAsText(0))
    if proj_directory is not None:
        orig_BREAKS_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(1))
        orig_CONTROL_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(2))
        orig_DSM_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(3))
        orig_DTM_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(4))
        orig_INTENSITY_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(5))
        orig_LAS_CLASSIFIED_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(6))
        orig_LAS_INVALID_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(7))
        orig_LAS_UNCLASSIFIED_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(8))
        orig_QA_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(9))
        orig_TILE_INDEX_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(10))
        orig_TILE_path = createPath(proj_directory, ORIGINAL_DIR, arcpy.GetParameterAsText(11))
        
        
        arcpy.AddMessage("0. proj_path '{}'".format(proj_directory))
        arcpy.AddMessage("1. orig_BREAKS_path '{}'".format(orig_BREAKS_path))
        arcpy.AddMessage("2. orig_CONTROL_path '{}'".format(orig_CONTROL_path))
        arcpy.AddMessage("3. orig_DSM_path '{}'".format(orig_DSM_path))
        arcpy.AddMessage("4. orig_DTM_path '{}'".format(orig_DTM_path))
        arcpy.AddMessage("5. orig_INTENSITY_path '{}'".format(orig_INTENSITY_path))
        arcpy.AddMessage("6. orig_LAS_CLASSIFIED_path '{}'".format(orig_LAS_CLASSIFIED_path))
        arcpy.AddMessage("7. orig_LAS_INVALID_path '{}'".format(orig_LAS_INVALID_path))
        arcpy.AddMessage("8. orig_LAS_UNCLASSIFIED_path '{}'".format(orig_LAS_UNCLASSIFIED_path))
        arcpy.AddMessage("9. orig_QA_path '{}'".format(orig_QA_path))
        arcpy.AddMessage("10. orig_TILE_INDEX_path '{}'".format(orig_TILE_INDEX_path))
        arcpy.AddMessage("11. orig_TILE_path '{}'".format(orig_TILE_path))
        
        
        # # set standard directory names
        new_BREAKS_path = createPath(proj_directory, DELIVERED_DIR, BREAKS_DIR)
        new_CONTROL_path = createPath(proj_directory, DELIVERED_DIR, CONTROL_DIR)
        new_DTM_path = createPath(proj_directory, DELIVERED_DIR, DTM_DIR)
        new_DSM_path = createPath(proj_directory, DELIVERED_DIR, DSM_DIR)
        new_INTENSITY_path = createPath(proj_directory, DELIVERED_DIR, INTENSITY_DIR)
        new_LAS_CLASSIFIED_path = createPath(proj_directory, DELIVERED_DIR, LAS_CLASSIFIED_DIR)
        new_LAS_INVALID_path = createPath(proj_directory, DELIVERED_DIR, LAS_INVALID_DIR)
        new_LAS_UNCLASSIFIED_path = createPath(proj_directory, DELIVERED_DIR, LAS_UNCLASSIFIED_DIR)
        new_QA_path = createPath(proj_directory, DELIVERED_DIR, QA_DIR)
        new_TILE_INDEX_path = createPath(proj_directory, DELIVERED_DIR, TILE_INDEX_DIR)
        new_TILE_path = createPath(proj_directory, DELIVERED_DIR, TILES_DIR)
        
        
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_BREAKS_path, new_BREAKS_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_CONTROL_path, new_CONTROL_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_DSM_path, new_DSM_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_DTM_path, new_DTM_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_INTENSITY_path, new_INTENSITY_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_LAS_CLASSIFIED_path, new_LAS_CLASSIFIED_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_LAS_INVALID_path, new_LAS_INVALID_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_LAS_UNCLASSIFIED_path, new_LAS_UNCLASSIFIED_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_QA_path, new_QA_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_TILE_INDEX_path, new_TILE_INDEX_path))
        arcpy.AddMessage("'{}' ==> '{}'".format(orig_TILE_path, new_TILE_path))
        
        
        data_path_list = [
                          [orig_BREAKS_path, new_BREAKS_path]
                          , [orig_CONTROL_path, new_CONTROL_path]
                          , [orig_DTM_path, new_DTM_path]
                          , [orig_DSM_path, new_DSM_path]
                          , [orig_INTENSITY_path, new_INTENSITY_path]
                          , [orig_LAS_CLASSIFIED_path, new_LAS_CLASSIFIED_path]
                          , [orig_LAS_INVALID_path, new_LAS_INVALID_path]
                          , [orig_LAS_UNCLASSIFIED_path, new_LAS_UNCLASSIFIED_path]
                          , [orig_QA_path, new_QA_path]
                          , [orig_TILE_INDEX_path, new_TILE_INDEX_path]
                          , [orig_TILE_path, new_TILE_path]
                          ]
        
        for dataPathSet in data_path_list:
            source = dataPathSet[0]
            dest = dataPathSet[1]
            if source is not None:
                if not os.path.exists(source):
                    arcpy.AddWarning("Source path doesn't exist. Can't copy '{}' to '{}'".format(source, dest))
                elif os.path.exists(dest):
                    arcpy.AddWarning("Destination path exists, can't overwrite '{}' to '{}'".format(source, dest))
                else:    
                    arcpy.AddMessage("Moving files from '{}' to '{}'".format(source, dest))
                    os.path.join(source, "*")
                    shutil.move(source, dest)
            else:
                arcpy.AddMessage("Source not provided for '{}'. Ignoring empty dataset.".format(dest))
    else:
        arcpy.AddError("Invalid project directory '{}'.".format(proj_directory))
except Exception as e:
    arcpy.AddError(e.message)

arcpy.AddMessage("Operation complete")
