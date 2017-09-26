'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy

from ngce.cmdr.JobUtil import getLogFolderFromWMXJobID
from ngce.pmdm import RunUtil


# from ngce.pmdm.c import C02PrepareContoursForPublishing
PATH = r'ngce\pmdm\c\C02_A_CreateContourAnnotations.py'

jobID = arcpy.GetParameterAsText(0)
args = [jobID]
arcpy.AddMessage(PATH)
# C02PrepareContoursForPublishing.PrepareContoursForPublishing(jobID)
RunUtil.runTool(PATH, args, log_path=getLogFolderFromWMXJobID(jobID))
