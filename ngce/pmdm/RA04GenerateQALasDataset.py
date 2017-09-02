'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy

from ngce.cmdr.JobUtil import getLogFolderFromWMXJobID
from ngce.pmdm import RunUtil


# from ngce.pmdm.a import A04GenerateQALasDataset
PATH = r'ngce\pmdm\a\A04_A_GenerateQALasDataset.py'

jobID = arcpy.GetParameterAsText(0)
createQARasters = arcpy.GetParameterAsText(1)
createMissingRasters = arcpy.GetParameterAsText(2)
overrideBorderPath = None
try:
    overrideBorderPath = arcpy.GetParameterAsText(3)
except:
    pass

args = [jobID,createQARasters,createMissingRasters,overrideBorderPath]
arcpy.AddMessage(args)

# A04GenerateQALasDataset.GenerateQALasDataset(jobID)

RunUtil.runTool(PATH, args, log_path=getLogFolderFromWMXJobID(jobID))
