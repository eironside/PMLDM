#-------------------------------------------------------------------------------
# Name:        ProjectInfo
# Purpose:
#
# Author:      kiyo4645
#
# Created:     05/11/2015
# Description: Builds the standard project folder structure at the specified
#              location.
# Inputs:
# parent_path - the existing parent dire
#-------------------------------------------------------------------------------


import arcpy
from datetime import datetime
import sys  # @UnusedImport
from ngce import Utility
from ngce.Utility import doTime
from ngce.folders.ProjectFolders import Project


def CreateProjectFolders(parent_path=None,
                 project_id=None,
                 project_path=None 
                 ):
    a = datetime.now()
    Utility.printArguments(["parent_path", "project_id", "project_path"],
                   [parent_path, project_id, project_path], "A01 CreateProjectFolders")
    
    projectFolder = Project(parent_path=parent_path, projectId=project_id, path=project_path)
    
    arcpy.AddMessage("Working on project path {}".format(projectFolder.path))
    
    projectFolder.make()
    arcpy.AddMessage("Finished creating project '{}' directory structure".format(projectFolder.path))

    doTime(a, "Operation Complete: A01 Create Project Folders")


if __name__ == '__main__':

     parent_path = sys.argv[1]
     project_id = sys.argv[2]
     project_path = sys.argv[3]

    
#    parent_path = r"E:\NGCE\RasterDatasets"
#    project_id = "OK_TEST_2008"
#    project_path = None
    CreateProjectFolders(parent_path, project_id, project_path)
