'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy
from ngce.pmdm import RunUtil

#from ngce.pmdm.a import A05RemoveDEMErrantValues
PATH = r'ngce\pmdm\a\A05RemoveDEMErrantValues.py'

jobID = arcpy.GetParameterAsText(0)
args = [jobID]

#A05RemoveDEMErrantValues.RemoveDEMErrantValues(jobID)

RunUtil.runTool(PATH, args, log_path=RunUtil.getLogFolderFromWMXJobID(jobID))
