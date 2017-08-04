'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy

from ngce.pmdm.b.B01 import 1CreateMasterMosaicDataset

parent_path = arcpy.GetParameterAsText(0)
MasterGDBName = arcpy.GetParameterAsText(1) 
MasterMDName = arcpy.GetParameterAsText(2)

1CreateMasterMosaicDataset.CreateMasterMosaicDatasets(parent_path, MasterGDBName, MasterMDName)