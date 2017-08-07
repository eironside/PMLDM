# import arcpy
from pmdm import RunUtil

PATH = r'C:\Users\jeff8977\Desktop\USDA\ngce\pmdm\d\D01.py'

# jobID = arcpy.GetParameterAsText(0)
jobID = '807'

RunUtil.runTool(PATH, [jobID])