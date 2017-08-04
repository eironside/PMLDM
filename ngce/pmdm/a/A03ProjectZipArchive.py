import arcpy
import shutil
import os
import zipfile

from ngce import Utility
from ngce.cmdr import CMDR


def ProjectZipArchive(strJobId):
    Utility.printArguments(["WMXJobID"], [strJobId], "A03 ProjectZipArchive")
    
    Utility.setWMXJobDataAsEnvironmentWorkspace(strJobId)
    
    ProjectJob = CMDR.ProjectJob()
    project, strUID = ProjectJob.getProject(strJobId)
    
    if project is not None:
        archive_dir = ProjectJob.getArchiveDir(project)
        basedir = ProjectJob.getProjectDir(project)
        archive_name = ProjectJob.getProjectID(project)
        Utility.printArguments(["ArchiveDir", "BaseDir", "ArchiveName"], [archive_dir, basedir, archive_name], "A03 ProjectZipArchive")
        
        if archive_dir is not None and basedir is not None and archive_name is not None:    
            # Currently archiving entire project directory. Uncomment following to just archive the ORIGINAL folder
            # basedir = os.path.join(basedir, FoldersConfig.original_dir)
            cwd = os.getcwd()
            arcpy.AddMessage('Changeing working directory from {} to {}'.format(cwd, basedir))
            os.chdir(basedir)
            arcpy.AddMessage('Current working directory is {}'.format(os.getcwd()))
            
            
            # archive contents of folder basedir
            arcpy.AddMessage('archiving contents of directory {} to {}.zip'.format(basedir, archive_name))
            #shutil.make_archive(archive_name, 'zip', basedir)

            with zipfile.ZipFile(archive_name + '.zip', "w",zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
                for root, _, filenames in os.walk(basedir):
                    for name in filenames:
                        name = os.path.join(root, name)
                        name = os.path.normpath(name)
                        arcpy.AddMessage('adding to archive {} file {}'.format(archive_name,name))
                        zf.write(name, name)

            
            # move the file to the archive directory
            arcpy.AddMessage('moving zip file to archive directory {}'.format(archive_dir))
            shutil.move("{}.zip".format(archive_name), archive_dir)
        
        else:
            arcpy.AddError('Failed to retrieve project info: archive_dir={} base_dir={} archive_name={}'.format(archive_dir, basedir, archive_name))
    else:
        arcpy.AddError('Failed to retrieve project info: project with WMX Job ID {} not found'.format(strJobId))

    arcpy.AddMessage("Operation complete")


if __name__ == '__main__':
    ProjectZipArchive(16402)
