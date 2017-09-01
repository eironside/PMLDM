'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy

from ngce.cmdr.JobUtil import getLogFolderFromWMXJobID
from ngce.pmdm import RunUtil


# from ngce.pmdm.a import A07PublishProjectMosaicDataset
PATH = r'ngce\pmdm\a\A07_A_PublishProjectMosaicDataset.py'

jobID = arcpy.GetParameterAsText(0)
serverConnectionFile = arcpy.GetParameterAsText(1)
serverFunctionPath = arcpy.GetParameterAsText(2)

args = [jobID, serverConnectionFile, serverFunctionPath]

# A07PublishProjectMosaicDataset.PublishMosaicDataset(jobID, serverConnectionFile, serverFunctionPath)

RunUtil.runTool(PATH, args, log_path=getLogFolderFromWMXJobID(jobID))
