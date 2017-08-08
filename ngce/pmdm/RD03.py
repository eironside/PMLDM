# import arcpy
from ngce.pmdm import RunUtil

PATH = r'C:\Users\jeff8977\Desktop\USDA\ngce\pmdm\d\D03.py'

# jobID = arcpy.GetParameterAsText(0)
jobID = '808'

RunUtil.runTool(PATH, [jobID])