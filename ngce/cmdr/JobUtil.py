'''
Created on Aug 25, 2017

@author: eric5946
'''
import arcpy

from ngce.Utility import setWMXJobDataAsEnvironmentWorkspace
from ngce.cmdr.CMDR import ProjectJob
from ngce.folders import ProjectFolders


def getLogFolderFromWMXJobID(strJobId):
    ProjectJob, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable
    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    return ProjectFolder.derived.log_path

def getProjectFromWMXJobID(strJobId):
    setWMXJobDataAsEnvironmentWorkspace(strJobId)
    
    ProjectJob = ProjectJob()
    project, strUID = ProjectJob.getProject(strJobId)  # @UnusedVariable
    
    if project is None:
        arcpy.AddError('Failed to retrieve project info: project with WMX Job ID {} not found'.format(strJobId))
    
    return ProjectJob, project, strUID
