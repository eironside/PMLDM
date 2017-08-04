'''
Created on Feb 12, 2016

@author: eric5946
'''
import arcpy

from ngce.pmdm.b import B02CreateMDMaster


wmxJobID = arcpy.GetParameterAsText(0)

B02CreateMDMaster.CreateMasterMosaicDatasets(wmxJobID)