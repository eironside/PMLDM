'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy
from ngce.pmdm import RunUtil

#from ngce.pmdm.c import C01ProcessContoursFromMDParallel
PATH = r'ngce\pmdm\c\C01ProcessContoursFromMDParallel.py'


jobID = arcpy.GetParameterAsText(0)
args = [jobID]

#C01ProcessContoursFromMDParallel.PrepareContoursForPublishing(jobID)
RunUtil.runTool(PATH, args, log_path=RunUtil.getLogFolderFromWMXJobID(jobID))
