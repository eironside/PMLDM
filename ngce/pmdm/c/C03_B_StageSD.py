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

    sddraftPath = sys.argv[1] #.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN
    arcpy.AddMessage('sddraftPath = ' + sddraftPath)
    sdPath = sys.argv[2] #.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN
    arcpy.AddMessage('sdPath = ' + sdPath)
    serverConnectionFile = sys.argv[3] #.replace('aiotxftw6na01data', 'aiotxftw6na01') #Added replace method 22 Mar 2019 BJN
    arcpy.AddMessage('serverConnectionFile = ' + serverConnectionFile)
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





