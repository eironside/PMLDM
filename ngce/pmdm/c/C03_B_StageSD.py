'''
Created on Jun 15, 2017

@author: eric5946
'''
import arcpy
from datetime import datetime
import sys
from ngce.Utility import doTime,addToolMessages,printArguments




if __name__ == '__main__':
    startupType = "STARTED"
    
    # time parameters to gauge how much time things are taking
    aaa = datetime.now()
    
    sddraftPath = sys.argv[1]
    sdPath = sys.argv[2]
    serverConnectionFile = sys.argv[3]
    if len(sys.argv) > 4:
        startupType = sys.argv[4]
    printArguments(["sddraftPath","sdPath","serverConnectionFile","startupType"], [sddraftPath,sdPath,serverConnectionFile, startupType], "A07_B_StageSD")


    arcpy.AddMessage("Staging draft {} to service definition {}".format(sddraftPath, sdPath))
    arcpy.StageService_server(sddraftPath, sdPath)
    addToolMessages()
    
    arcpy.AddMessage("Uploading service definition {} to server {}".format(sdPath, serverConnectionFile))
    arcpy.UploadServiceDefinition_server(in_sd_file=sdPath, in_server=serverConnectionFile, in_startupType=startupType, in_my_contents="NO_SHARE_ONLINE", in_public="PRIVATE", in_organization="NO_SHARE_ORGANIZATION")    
    addToolMessages()
    
    doTime(aaa, "Service publish completed {}".format(sdPath))
        
        
        
        

