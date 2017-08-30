'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy

from ngce.cmdr.JobUtil import getLogFolderFromWMXJobID
from ngce.pmdm import RunUtil


# from ngce.pmdm.a import A08AddProjectToMaster
PATH = r'ngce\pmdm\a\A08AddProjectToMaster.py'


jobID = arcpy.GetParameterAsText(0)
MasterMDs_fgdb_path = arcpy.GetParameterAsText(1)
MasterMDName = arcpy.GetParameterAsText(2)
args = [jobID, MasterMDs_fgdb_path, MasterMDName]


# A08AddProjectToMaster.AddProjectToMaster(jobID, MasterMDs_fgdb_path, MasterMDName)
RunUtil.runTool(PATH, args, log_path=getLogFolderFromWMXJobID(jobID))
