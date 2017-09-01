import arcpy
from datetime import datetime
import os
import shutil
import sys
import zipfile

from ngce import Utility
from ngce.Utility import doTime
from ngce.cmdr.JobUtil import getProjectFromWMXJobID


def processJob(ProjectJob, project, strUID):
    a = datetime.now()
    aa = a

    archive_dir = ProjectJob.getArchiveDir(project)
    basedir = ProjectJob.getProjectDir(project)
    archive_name = ProjectJob.getProjectID(project)
    Utility.printArguments(["ArchiveDir", "BaseDir", "ArchiveName"], [archive_dir, basedir, archive_name], "A03 ProjectZipArchive")
    
    if archive_dir is None or basedir is None or archive_name is None:    
        arcpy.AddError('Failed to retrieve project info: archive_dir={} base_dir={} archive_name={}'.format(archive_dir, basedir, archive_name))
    else:
        # ## Currently archiving entire project directory. 
        
        # ## Uncomment following to just archive the ORIGINAL folder
        # basedir = os.path.join(basedir, FoldersConfig.original_dir)
        
        # ## Uncomment following to just archive the DELIVEREDfolder
        # basedir = os.path.join(basedir, FoldersConfig.delivered_dir)
        cwd = os.getcwd()
        arcpy.AddMessage('Changeing working directory from {} to {}'.format(cwd, basedir))
        os.chdir(basedir)
        arcpy.AddMessage('Current working directory is {}'.format(os.getcwd()))
        
        
        # archive contents of folder basedir
        arcpy.AddMessage('archiving contents of directory {} to {}.zip'.format(basedir, archive_name))
        # shutil.make_archive(archive_name, 'zip', basedir)

        with zipfile.ZipFile(archive_name + '.zip', "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for root, _, filenames in os.walk(basedir):
                for name in filenames:
                    name = os.path.join(root, name)
                    name = os.path.normpath(name)
                    a = doTime('adding to archive {} file {}'.format(archive_name, name))
                    zf.write(name, name)

        
        # move the file to the archive directory
        a = doTime('moving zip file to archive directory {}'.format(archive_dir))
        shutil.move("{}.zip".format(archive_name), archive_dir)
        a = doTime('Moved archive {} file to {}'.format(archive_name, archive_dir))

    doTime(aa, "Operation Complete: A03 Zip project and move to archive folder")
    
def ProjectZipArchive(strJobId):
    Utility.printArguments(["WMXJobID"], [strJobId], "A03 ProjectZipArchive")
    
    ProjectJob, project, strUID = getProjectFromWMXJobID(strJobId)
    
    processJob(ProjectJob, project, strUID)
    


if __name__ == '__main__':
    
    strJobId = sys.argv[1]
    ProjectZipArchive(strJobId)
    
#     ### Debug settings
#     UID = None  # field_ProjectJob_UID
#     wmx_job_id = 1
#     project_Id = "OK_SugarCreek_2008"
#     alias = "Sugar Creek"
#     alias_clean = "SugarCreek"
#     state = "OK"
#     year = 2008
#     parent_dir = r"E:\NGCE\RasterDatasets"
#     archive_dir = r"E:\NGCE\RasterDatasets"
#     project_dir = r"E:\NGCE\RasterDatasets\OK_SugarCreek_2008"
#     project_AOI = None
#                    
#     ProjectJob = ProjectJob()
#     project = [
#                UID,  # field_ProjectJob_UID
#                wmx_job_id,  # field_ProjectJob_WMXJobID,
#                project_Id,  # field_ProjectJob_ProjID,
#                alias,  # field_ProjectJob_Alias
#                alias_clean,  # field_ProjectJob_AliasClean
#                state ,  # field_ProjectJob_State
#                year ,  # field_ProjectJob_Year
#                parent_dir,  # field_ProjectJob_ParentDir
#                archive_dir,  # field_ProjectJob_ArchDir
#                project_dir,  # field_ProjectJob_ProjDir
#                project_AOI  # field_ProjectJob_SHAPE
#                ]
#     
#     processJob(ProjectJob, project, UID)
    
    
