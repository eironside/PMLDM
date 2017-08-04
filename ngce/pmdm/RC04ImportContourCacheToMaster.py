'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy
from ngce.pmdm import RunUtil

#from ngce.pmdm.c import C04ImportContourCacheToMaster
PATH = r'ngce\pmdm\c\C04ImportContourCacheToMaster.py'


jobID = arcpy.GetParameterAsText(0)
serverConnectionFilePath = arcpy.GetParameterAsText(1)
masterServiceName = arcpy.GetParameterAsText(2)
args = [jobID,serverConnectionFilePath,masterServiceName]

#C04ImportContourCacheToMaster.ImportContourCacheToMaster(jobID, serverConnectionFilePath, masterServiceName)

RunUtil.runTool(PATH, args)
