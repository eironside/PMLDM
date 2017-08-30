'''
Created on Feb 12, 2016

@author: eric5946
'''

import arcpy
from ngce.pmdm import RunUtil

PATH = r'ngce\pmdm\a\A01CreateProjectFolders.py'

parent_path = arcpy.GetParameterAsText(0)
project_id = arcpy.GetParameterAsText(1)
project_path = arcpy.GetParameterAsText(2)

args = [parent_path, project_id, project_path]

RunUtil.runTool(PATH, args, log_path=project_path)

