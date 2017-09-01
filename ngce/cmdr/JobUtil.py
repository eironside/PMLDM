'''
Created on Aug 25, 2017

@author: eric5946
'''
import arcpy

from ngce.Utility import setWMXJobDataAsEnvironmentWorkspace
from ngce.cmdr.CMDR import ProjectJob
from ngce.folders import ProjectFolders


def getLogFolderFromWMXJobID(strJobId):
    project_job, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable
    project_folder = ProjectFolders.getProjectFolderFromDBRow(project_job, project)
    return project_folder.derived.log_path

def getProjectFromWMXJobID(strJobId):
    setWMXJobDataAsEnvironmentWorkspace(strJobId)
    
    project_job = ProjectJob()
    project, strUID = project_job.getProject(strJobId)  # @UnusedVariable
    
    if project is None:
        arcpy.AddError('Failed to retrieve project info: project with WMX Job ID {} not found'.format(strJobId))
    
    return project_job, project, strUID
