'''
Created on Dec 2, 2015

@author: eric5946
'''
import arcpy
import os
import sys

from ngce import Utility
from ngce.cmdr import CMDR, CMDRConfig


# def updateMasterDomain(masterPath, workspace):
#     domain_name = 'Code_Master_Path'
#     table = r"in_memory/{}".format(domain_name)
#     # Create a table in memory to hold the codes and their descriptions for a particular domain. 
#     fields = ['codeField', 'descriptionField']
#     cvdTable = arcpy.DomainToTable_management(workspace, domain_name, table, fields[0], fields[1])
#     found = False
#     # Create a cursor to loop through the table holding the domain and code info 
#     rows = arcpy.SearchCursor(cvdTable)  
#     # Loop through each row in the table. 
#     for row in rows:   
#         # For each row in the table populate the key with the code field and the value from the description field   
#         if row.codeField == masterPath:
#             found = True
#             arcpy.AddMessage("Master MD Path already in domain {}".format(masterPath))
#             break
#         del row 
#     del rows  
#     
#     if not found:
#         arcpy.AddMessage("Adding Master MD Path to domain {}".format(masterPath))
#         cursor_i = arcpy.InsertCursor(cvdTable, fields)
#         cursor_i.insertRow([masterPath, masterPath])
#         del cursor_i
#     
#         arcpy.DeleteDomain_management(workspace, domain_name)
#         arcpy.TableToDomain_management (cvdTable, code_field=fields[0], description_field=fields[1], in_workspace=workspace, domain_name=domain_name, domain_description="The Master Mosaic Dataset Paths", update_option='APPEND') 
def AddMDMasterToCMDR(wmxJobId, masterParentDir, masterName, masterCellSize_m, masterServerConnectionFilePath):
    masterServiceFolder = None
    index = masterName.find("/")
    if index < 0:
        index = masterName.find("\\")
    
    if index >= 0:
        masterServiceFolder = masterName[0:index]
        masterName = masterName[index+1:]
    
    
    Utility.printArguments(["wmxJobId", "masterParentDir", "masterName", "masterCellSize_m", "masterServerConnectionFilePath", "masterServiceFolder"],
                           [wmxJobId, masterParentDir, masterName, masterCellSize_m, masterServerConnectionFilePath, masterServiceFolder], "B01 AddMDMasterToCMDR")
    
    # build attributes from parameters
    if wmxJobId is not None and int(wmxJobId) > 0:
        if masterParentDir is not None:
            if masterName is None:
                masterName = CMDRConfig.DEFAULT_MDMASTER_NAME
            
            
            
            # get CMDR from job data workspace and set as current workspace
            Utility.setWMXJobDataAsEnvironmentWorkspace(wmxJobId)
            # get job AOI geometry
            master_AOI = Utility.getJobAoi(wmxJobId)
            
            # NOTE: Edit session handled in Utility
            
            Master = CMDR.MDMaster()
            Master.addOrUpdate(wmx_job_ID=wmxJobId,
                               parent_dir=masterParentDir,
                               master_name=masterName,
                               masterServerConnectionFilePath=masterServerConnectionFilePath,
                               masterCellSize_m=masterCellSize_m,
                               masterServiceFolder=masterServiceFolder,
                                master_AOI=master_AOI)
            
            
#             mdMaster_row = list(Master.getExistingMDRow(wmxJobId))
            
#             updateMasterDomain(Master.getMDPath(mdMaster_row), wmxWorkspace)
        else:
            arcpy.AddError("Master parent directory not set. cannot continue.")
    else:
        arcpy.AddError("Master wmx Job ID is not set. cannot continue.")    
          

    arcpy.AddMessage("Operation complete")


if __name__ == '__main__':
    wmxWorkspace = "PMDM_WMX"
    wmxJobId = 6401
    masterParentDir = u"\\Ngcedev\DAS1\RasterData\Elevation\LiDar\MASTER"
    masterName = u"KUNGFOO\\ELEVATION_1M"
    masterCellSize_m = 1.0
    masterServerConnectionFilePath = u"\\NGCEDEV.esri.com\ArcGIS\arcgis on localhost_6080 (publisher).ags"
#     masterServiceFolder = "MASTER"
    AddMDMasterToCMDR(wmxJobId, masterParentDir, masterName, masterCellSize_m, masterServerConnectionFilePath)
