'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy
from ngce.pmdm import RunUtil

#from ngce.pmdm.a import A03ProjectZipArchive

PATH = r'ngce\pmdm\a\A03ProjectZipArchive.py'

jobID = arcpy.GetParameterAsText(0)
args = [jobID]

#A03ProjectZipArchive.ProjectZipArchive(jobID)

RunUtil.runTool(PATH, args)
