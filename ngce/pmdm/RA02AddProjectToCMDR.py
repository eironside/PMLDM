'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy
from ngce.pmdm import RunUtil

#from ngce.pmdm.a import A02AddProjectToCMDR

PATH = r'ngce\pmdm\a\A02AddProjectToCMDR.py'

strProjID = arcpy.GetParameterAsText(0)   
strAlias = arcpy.GetParameterAsText(1)     
strState = arcpy.GetParameterAsText(2)
strYear = arcpy.GetParameterAsText(3)
strJobId = arcpy.GetParameterAsText(4)
strParentDir = arcpy.GetParameterAsText(5)
strArchiveDir = arcpy.GetParameterAsText(6)

args = [strProjID, strAlias, strState, strYear, strJobId, strParentDir, strArchiveDir]

#A02AddProjectToCMDR.AddPrjectToCMDR(strProjID, strAlias, strState, strYear, strJobId, strParentDir, strArchiveDir)

RunUtil.runTool(PATH, args)
