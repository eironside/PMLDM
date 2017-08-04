import arcpy
import os
import sys

def setWMXJobDataAsEnvironmentWorkspace(jobId):
    arcpy.AddMessage(str(arcpy.GetJobDataWorkspace_wmx(jobId,os.path.join(os.path.dirname(os.path.abspath(__file__)),'WMXAdmin.jtc'))))  # @UndefinedVariable

## Doesn't work, need to export to WKT format
##def getJobAoi(project_jobId):
##    arcpy.AddMessage(arcpy.CopyFeatures_management(arcpy.GetJobAOI_wmx(project_jobId), arcpy.Geometry())[0])  # @UndefinedVariable


if __name__ == '__main__':
    
    arcpy.CheckOutExtension("JTX")
    
    func = sys.argv[1]
    jobId = sys.argv[2]

    if func == 'setWMXJobDataAsEnvironmentWorkspace':
        setWMXJobDataAsEnvironmentWorkspace(jobId)
##    elif func == 'getJobAoi':
##        getJobAoi(jobId)
    
