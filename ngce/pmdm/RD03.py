import arcpy

from ngce.pmdm import RunUtil


PATH = r'ngce\pmdm\d\D03.py'
# PATH = r'C:\Users\jeff8977\Desktop\NGCE_GitHub\ngce\pmdm\d\D03.py'

jobID = arcpy.GetParameterAsText(0)
# jobID = '808'

RunUtil.runTool(PATH, [jobID], log_path=RunUtil.getLogFolderFromWMXJobID(jobID))