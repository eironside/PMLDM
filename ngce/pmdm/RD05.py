# import arcpy
from ngce.pmdm import RunUtil

PATH = r'C:\Users\jeff8977\Desktop\NGCE_GitHub\ngce\pmdm\d\D05.py'

# jobID = arcpy.GetParameterAsText(0)
jobID = '808'

RunUtil.runTool(PATH, [jobID])