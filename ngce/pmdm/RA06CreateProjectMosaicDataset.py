'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy
from ngce.pmdm import RunUtil

#from ngce.pmdm.a import A06CreateProjectMosaicDataset

PATH = r'ngce\pmdm\a\A06CreateProjectMosaicDataset.py'

jobID = arcpy.GetParameterAsText(0)
args = [jobID]

#A06CreateProjectMosaicDataset.CreateProjectMosaicDataset(jobID)

RunUtil.runTool(PATH, args, log_path=RunUtil.getLogFolderFromWMXJobID(jobID))
