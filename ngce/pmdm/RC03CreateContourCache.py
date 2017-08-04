'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy
from ngce.pmdm import RunUtil

#from ngce.pmdm.c import C03CreateContourCache
PATH = r'ngce\pmdm\c\C03CreateContourCache.py'


jobID = arcpy.GetParameterAsText(0)
serverConnectionFile = arcpy.GetParameterAsText(1)
args = [jobID,serverConnectionFile]

#C03CreateContourCache.CreateContourCache(jobID, serverConnectionFile)
RunUtil.runTool(PATH, args)
