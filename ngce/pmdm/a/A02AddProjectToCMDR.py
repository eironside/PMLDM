'''
Created on Dec 2, 2015

@author: eric5946
'''
import arcpy
from datetime import datetime
import os
import sys

from ngce import Utility
from ngce.Utility import doTime
from ngce.cmdr import CMDR


def AddPrjectToCMDR(strProjID, strAlias, strState, strYear, strJobId, strParentDir, strArchiveDir):
    Utility.printArguments(["ProjID", "Alias", "State", "Year", "JobID", "ParentDir", "ArchiveDir"],
                           [strProjID, strAlias, strState, strYear, strJobId, strParentDir, strArchiveDir], "A02 AddPrjectToCMDR")
    a = datetime.now()
    aa = a
    # build attributes from parameters
    if strProjID is None:
        arcpy.AddError("Project ID is empty. Please supply a project ID.")
    else:
        
        # alias has spaces and invalid characters, Name clean is just the alias without invalid chars
        strAliasClean = Utility.cleanString(strAlias)
        
        
        # Create project directory path
        strProjDir = os.path.join(strParentDir, strProjID)
        
        Utility.printArguments(["ProjDir", "AliasClean"],
                               [strProjDir, strAliasClean], "A02 AddPrjectToCMDR")
        
        # get CMDR from job data workspace and set as current workspace
        Utility.setWMXJobDataAsEnvironmentWorkspace(strJobId)
        # get job AOI geometry
        project_AOI = Utility.getJobAoi(strJobId)
        
        # NOTE: Edit session handled in Utility
        
        Contract = CMDR.Contract()
        contract_row = Contract.addOrUpdateProject(project_ID=strProjID,
                                                   project_UID=None,
                                                   project_AOI=project_AOI)
        strUID = Contract.getProjectUID(contract_row)
        a = doTime(a, "Added project '" + strProjID + "' to CMDR Contract with UID '" + strUID + "'")
        
        ProjectJob = CMDR.ProjectJob()
        ProjectJob.addOrUpdateProject(wmx_job_id=strJobId,
                                      project_Id=strProjID,
                                      alias=strAlias,
                                      alias_clean=strAliasClean,
                                      state=strState,
                                      year=strYear,
                                      parent_dir=strParentDir,
                                      archive_dir=strArchiveDir,
                                      project_dir=strProjDir,
                                      UID=strUID,
                                      project_AOI=project_AOI)
        a = doTime(a, "Added project '" + strProjID + "' to CMDR Project Job")
        
        Deliver = CMDR.Deliver()
        Deliver.addOrUpdateProject(project_Id=strProjID,
                                   UID=strUID,
                                   project_AOI=project_AOI)
        a = doTime(a, "Added project '" + strProjID + "' to CMDR Deliver")
        
        QC = CMDR.QC()
        QC.addOrUpdateProject(project_Id=strProjID,
                                   UID=strUID,
                                   project_AOI=project_AOI)
        a = doTime(a, "Added project '" + strProjID + "' to CMDR QAQC")
        
        Publish = CMDR.Publish()
        Publish.addOrUpdateProject(project_Id=strProjID,
                                   UID=strUID,
                                   project_AOI=project_AOI)
        a = doTime(a, "Added project '" + strProjID + "' to CMDR Publish")
    
    
          

    doTime(aa, "Operation Complete: A02 Add Project to CMDR")


if __name__ == '__main__':
##Debug_____
##    strProjID = "OK_SugarCreek_2008"
##    strAlias = "Sugar Creek"
##    strState = "OK"
##    strYear = "2008"
##    strJobId = "16402"
##    strParentDir = u"E:\\NGCE\\RasterDatasets"
##    strArchiveDir = u"E:\\NGCE\\RasterDatasets"

    strProjID = sys.argv[1]
    strAlias = sys.argv[2]
    strState = sys.argv[3]
    strYear = sys.argv[4]
    strJobId = sys.argv[5]
    strParentDir = sys.argv[6]
    strArchiveDir = sys.argv[7]

    AddPrjectToCMDR(strProjID, strAlias, strState, strYear, strJobId, strParentDir, strArchiveDir)
