'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy

from ngce.pmdm.b import B01AddMDMasterToCMDR

# wmxWorkspace= arcpy.GetParameterAsText(0)
wmxJobId= arcpy.GetParameterAsText(0)
masterParentDir= arcpy.GetParameterAsText(1)
masterName= arcpy.GetParameterAsText(2)
masterCellSize_m= arcpy.GetParameterAsText(3)
masterServerConnectionFilePath= arcpy.GetParameterAsText(4)
# masterServiceFolder= arcpy.GetParameterAsText(6)

B01AddMDMasterToCMDR.AddMDMasterToCMDR(wmxJobId, masterParentDir, masterName, masterCellSize_m, masterServerConnectionFilePath)