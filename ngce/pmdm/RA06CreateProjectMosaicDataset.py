'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy

from ngce.cmdr.JobUtil import getLogFolderFromWMXJobID
from ngce.pmdm import RunUtil


# from ngce.pmdm.a import A06CreateProjectMosaicDataset
PATH = r'ngce\pmdm\a\A06_A_CreateProjectMosaicDataset.py'

jobID = arcpy.GetParameterAsText(0)
dateDeliver = arcpy.GetParameterAsText(1)
#@TODO: add the start and end dates

args = [jobID, dateDeliver]
arcpy.AddMessage(args)
# A06CreateProjectMosaicDataset.CreateProjectMosaicDataset(jobID)

RunUtil.runTool(PATH, args, log_path=getLogFolderFromWMXJobID(jobID))
