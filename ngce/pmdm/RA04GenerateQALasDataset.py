'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy
from ngce.pmdm import RunUtil

#from ngce.pmdm.a import A04GenerateQALasDataset
PATH = r'ngce\pmdm\a\A04GenerateQALasDataset.py'

jobID = arcpy.GetParameterAsText(0)
args = [jobID]

#A04GenerateQALasDataset.GenerateQALasDataset(jobID)

RunUtil.runTool(PATH, args, log_path=RunUtil.getLogFolderFromWMXJobID(jobID))
