'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy

from ngce.pmdm.b import B03PublishMDMosaicDataset


jobID = arcpy.GetParameterAsText(0)
serverFunctionPath = arcpy.GetParameterAsText(1)

B03PublishMDMosaicDataset.PublishMDMasterMosaicDataset(jobID, serverFunctionPath)
