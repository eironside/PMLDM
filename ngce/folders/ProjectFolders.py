'''
Created on Dec 21, 2015

@author: eric5946
'''
import arcpy
import os
import shutil

from ngce.cmdr.CMDRConfig import DTM
from ngce.folders.FoldersConfig import delivered_dir, breaks_dir, control_dir, demFirst_dir, demLast_dir, \
    intensity_dir, lasClassified_dir, lasUnclassified_dir, tileIndex_dir, boundary_dir, qa_dir, derived_dir, \
    stats_dir, published_dir, las_dir, metadata_dir, original_dir, contour_dir, elev_dir, pulse_count_dir, \
    point_count_dir, predominant_last_return_dir, predominant_class_dir, intensity_range_dir, z_range_dir, \
    demLAll_dir, demHeight_dir, FIRST, LAST, ALL, ALAST, lasd_dir, LAS, RASTER, DLM, DHM, DSM, INT, SCRATCH, ELEVATION


class Delivered(object):
    '''
    Delivered project directory
    
    '''
    parent = ""
    parent_dir = ""
    parent_path = "" 
    
    path = delivered_dir
    dirList = [          breaks_dir,
                         control_dir,
                         contour_dir,
                         demFirst_dir,
                         demLast_dir,
#                          demHeight_dir,
                         intensity_dir,
#                         intensityFirst_dir,
#                         intensityLast_dir,
                         lasClassified_dir,
                         lasUnclassified_dir,
#                         lasInvalid_dir,
                         tileIndex_dir,
#                         tiles_dir,
                         boundary_dir,
                         qa_dir
                         ]
    
    breaks_path = os.path.join(path, breaks_dir)
    control_path = os.path.join(path, control_dir)
    contour_path = os.path.join(path, contour_dir)
    
    demFirst_path = os.path.join(path, demFirst_dir)
    demLast_path = os.path.join(path, demLast_dir)
#     demHeight_path = os.path.join(path, demHeight_dir)
    
    intensity_path = os.path.join(path, intensity_dir)
#    intensityFirst_path = os.path.join(path, intensityFirst_dir)
#    intensityLast_path = os.path.join(path, intensityLast_dir)
    
    lasClassified_path = os.path.join(path, lasClassified_dir)
    lasUnclassified_path = os.path.join(path, lasUnclassified_dir)
#    lasInvalid_path = os.path.join(path, lasInvalid_dir)
    
    tileIndex_path = os.path.join(path, tileIndex_dir)
#    tiles_path = os.path.join(path, tiles_dir)
    boundary_path = os.path.join(path, boundary_dir)
    
    qa_path = os.path.join(path, qa_dir)
    
    
    pathList = [         breaks_path,
                         control_path,
                         contour_path,
                         demFirst_path,
                         demLast_path,
#                          demHeight_path,
                         intensity_path,
#                         intensityFirst_path,
#                         intensityLast_path,
                         lasClassified_path,
                         lasUnclassified_path,
#                         lasInvalid_path,
                         tileIndex_path,
#                         tiles_path,
                         boundary_path,
                         qa_path
                         ]
    
    pathDict = {}
    
    def __init__(self, parent, parent_dir, parent_path):
        '''
        parent - Parent directory class
        parent_dir - Parent project directory
        parent_path - Parent project folder path
        '''
        self.parent = parent
        self.parent_dir = parent_dir
        self.parent_path = parent_path
        
        self.path = os.path.join(self.parent_path, self.path)
        
        self.breaks_path = os.path.join(self.path, breaks_dir)
        self.control_path = os.path.join(self.path, control_dir)
        self.contour_path = os.path.join(self.path, contour_dir)
        
        self.demFirst_path = os.path.join(self.path, demFirst_dir)
        self.demLast_path = os.path.join(self.path, demLast_dir)
#         self.demHeight_path = os.path.join(self.path, demHeight_dir)
        
        self.intensity_path = os.path.join(self.path, intensity_dir)
#        self.intensityFirst_path = os.path.join(self.path, intensityFirst_dir)
#        self.intensityLast_path = os.path.join(self.path, intensityLast_dir)
        
        self.lasClassified_path = os.path.join(self.path, lasClassified_dir)
        self.lasUnclassified_path = os.path.join(self.path, lasUnclassified_dir)
#        self.lasInvalid_path = os.path.join(self.path, lasInvalid_dir)
        
        self.tileIndex_path = os.path.join(self.path, tileIndex_dir)
#        self.tiles_path = os.path.join(self.path, tiles_dir)
        self.boundary_path = os.path.join(self.path, boundary_dir)
        
        self.qa_path = os.path.join(self.path, qa_dir)
        
        self.pathList = [self.breaks_path,
                         self.control_path,
                         self.contour_path,
                         self.demFirst_path,
                         self.demLast_path,
#                          self.demHeight_path,
                         self.intensity_path,
#                         self.intensityFirst_path,
#                         self.intensityLast_path,
                         self.lasClassified_path,
                         self.lasUnclassified_path,
#                         self.lasInvalid_path,
                         self.tileIndex_path,
#                         self.tiles_path,
                         self.boundary_path,
                         self.qa_path
                         ]
        
        
        for i, item in enumerate(self.dirList):
            self.pathDict[item] = self.pathList[i]
        
    def make(self, messages=[], errors=[], warnings=[]):
        if not(os.path.exists(self.path)):
            os.mkdir(self.path)
            messages.append("Created project directory '{}".format(self.path))
        else:
            warnings.append("Directory already exists '{}'".format(self.path))
        
        for item in self.pathList:
            if not(os.path.exists(item)):
                os.mkdir(item)
                messages.append("Created project directory '{}".format(item))
            else:
                warnings.append("Directory already exists '{}'".format(item))
        
        return messages, errors, warnings



class Derived(object):
    '''
    Derived project directory
    
    '''
    parent = ""
    parent_dir = ""
    parent_path = "" 
    
    path = derived_dir
    dirList = [          demFirst_dir,
                         demLast_dir,
                         demLAll_dir,
                         demHeight_dir,
                         elev_dir,
                        intensity_dir,
                         
                         lasClassified_dir,
                         lasUnclassified_dir,
                         
                        pulse_count_dir,
                        point_count_dir,
                        predominant_last_return_dir,
                        predominant_class_dir,
                        intensity_range_dir,
                        z_range_dir,
                         
                           stats_dir,
                           contour_dir
                         ]
    
    demFirst_path = os.path.join(path, demFirst_dir)
    demLast_path = os.path.join(path, demLast_dir)
    demHeight_path = os.path.join(path, demHeight_dir)
    demLAll_path = os.path.join(path, demLAll_dir)
    
    intensity_path = os.path.join(path, intensity_dir)
    elev_path = os.path.join(path, elev_dir)
    
    lasClassified_path = os.path.join(path, lasClassified_dir)
    lasUnclassified_path = os.path.join(path, lasUnclassified_dir)
    # LAS is a shortcut not a real directory, don't include in path list
    las_path = os.path.join(path, las_dir)
    
    pulse_count_path = os.path.join(path, pulse_count_dir)
    point_count_path = os.path.join(path, point_count_dir)
    predominant_last_return_path = os.path.join(path, predominant_last_return_dir)
    predominant_class_path = os.path.join(path, predominant_class_dir)
    intensity_range_path = os.path.join(path, intensity_range_dir)
    z_range_path = os.path.join(path, z_range_dir)
    
    stats_path = os.path.join(path, stats_dir)
    contour_path = os.path.join(path, contour_dir)
    
    lasd_name = ".lasd"
    fgdb_name = ".gdb"
    lasd_path = os.path.join(path, lasd_name)
    fgdb_path = os.path.join(path, fgdb_name)
    
    pathList = [         demFirst_path,
                         demLast_path,
                        demHeight_path,
                         demLAll_path,
                        intensity_path,
                         lasClassified_path,
                         lasUnclassified_path,
                         
                        pulse_count_path,
                        point_count_path,
                        predominant_last_return_path,
                        predominant_class_path,
                        intensity_range_path,
                        z_range_path,
                         
                        stats_path,
                        contour_path
                         ]
    
    pathDict = {}
    
    def __init__(self, parent, parent_dir, parent_path):
        '''
        parent - Parent directory class
        parent_dir - Parent project directory
        parent_path - Parent project folder path
        '''
        self.parent = parent
        self.parent_dir = parent_dir
        self.parent_path = parent_path
        
        self.path = os.path.join(self.parent_path, self.path)
        
        self.demFirst_path = os.path.join(self.path, demFirst_dir)
        self.demLast_path = os.path.join(self.path, demLast_dir)
        self.demHeight_path = os.path.join(self.path, demHeight_dir)
        self.demLAll_path = os.path.join(self.path, demLAll_dir)
        
        self.intensity_path = os.path.join(self.path, intensity_dir)
        self.intensity_first_path = os.path.join(self.path, intensity_dir, FIRST)
        
        self.elev_path = os.path.join(self.path, elev_dir)
        self.elev_first_path = os.path.join(self.path, elev_dir, FIRST)
        self.elev_last_path = os.path.join(self.path, elev_dir, LAST)
        self.elev_all_path = os.path.join(self.path, elev_dir, ALAST)
        
        self.lasClassified_path = os.path.join(self.path, lasClassified_dir)
        self.lasClassified_lasd_path = os.path.join(self.path, lasClassified_dir, lasd_dir)
        
        self.lasUnclassified_path = os.path.join(self.path, lasUnclassified_dir)
        self.lasUnclassified_lasd_path = os.path.join(self.path, lasUnclassified_dir, lasd_dir)
        
        # LAS is a shortcut not a real directory, don't include in path list
        self.las_path = os.path.join(self.path, las_dir)
        
        self.log_path = os.path.join(self.path, "logs")
        
        self.pulse_count_path = os.path.join(self.path, pulse_count_dir)
        self.pulse_count_all_path = os.path.join(self.path, pulse_count_dir, ALL)
        self.pulse_count_first_path = os.path.join(self.path, pulse_count_dir, FIRST)
        self.pulse_count_last_path = os.path.join(self.path, pulse_count_dir, LAST)
        
        self.point_count_path = os.path.join(self.path, point_count_dir)
        self.point_count_all_path = os.path.join(self.path, point_count_dir, ALL)
        self.point_count_first_path = os.path.join(self.path, point_count_dir, FIRST)
        self.point_count_last_path = os.path.join(self.path, point_count_dir, LAST)
        
        self.predominant_last_return_path = os.path.join(self.path, predominant_last_return_dir)
        self.predominant_last_return_all_path = os.path.join(self.path, predominant_last_return_dir, LAST)
        self.predominant_last_return_first_path = os.path.join(self.path, predominant_last_return_dir, FIRST)
        self.predominant_last_return_last_path = os.path.join(self.path, predominant_last_return_dir, LAST)
        
        self.predominant_class_path = os.path.join(self.path, predominant_class_dir)
        self.predominant_class_all_path = os.path.join(self.path, predominant_class_dir, ALL)
        self.predominant_class_first_path = os.path.join(self.path, predominant_class_dir, FIRST)
        self.predominant_class_last_path = os.path.join(self.path, predominant_class_dir, LAST)
        
        self.intensity_range_path = os.path.join(self.path, intensity_range_dir)
        self.intensity_range_all_path = os.path.join(self.path, intensity_range_dir, ALL)
        self.intensity_range_first_path = os.path.join(self.path, intensity_range_dir, FIRST)
        self.intensity_range_last_path = os.path.join(self.path, intensity_range_dir, LAST)
        
        self.z_range_path = os.path.join(self.path, z_range_dir)
        self.z_range_all_path = os.path.join(self.path, z_range_dir, ALL)
        self.z_range_first_path = os.path.join(self.path, z_range_dir, FIRST)
        self.z_range_last_path = os.path.join(self.path, z_range_dir, LAST)
        
        
        self.stats_path = os.path.join(self.path, stats_dir)
        self.stats_las_path = os.path.join(self.path, stats_dir, LAS)
        
        self.stats_raster_path = os.path.join(self.path, stats_dir, RASTER)
        
        self.stats_raster_dlm_path = os.path.join(self.path, stats_dir, RASTER, DLM)
        self.stats_raster_dlm_derived_path = os.path.join(self.path, stats_dir, RASTER, DLM, derived_dir)
        self.stats_raster_dlm_original_path = os.path.join(self.path, stats_dir, RASTER, DLM, original_dir)
        self.stats_raster_dlm_published_path = os.path.join(self.path, stats_dir, RASTER, DLM, published_dir)
        
        self.stats_raster_dtm_path = os.path.join(self.path, stats_dir, RASTER, DTM)
        self.stats_raster_dtm_derived_path = os.path.join(self.path, stats_dir, RASTER, DTM, derived_dir)
        self.stats_raster_dtm_original_path = os.path.join(self.path, stats_dir, RASTER, DTM, original_dir)
        self.stats_raster_dtm_published_path = os.path.join(self.path, stats_dir, RASTER, DTM, published_dir)
        
        self.stats_raster_dsm_path = os.path.join(self.path, stats_dir, RASTER, DSM)
        self.stats_raster_dsm_derived_path = os.path.join(self.path, stats_dir, RASTER, DSM, derived_dir)
        self.stats_raster_dsm_original_path = os.path.join(self.path, stats_dir, RASTER, DSM, original_dir)
        self.stats_raster_dsm_published_path = os.path.join(self.path, stats_dir, RASTER, DSM, published_dir)
        
        self.stats_raster_intensity_path = os.path.join(self.path, stats_dir, RASTER, INT)
        self.stats_raster_intensity_derived_path = os.path.join(self.path, stats_dir, RASTER, INT, derived_dir)
        self.stats_raster_intensity_original_path = os.path.join(self.path, stats_dir, RASTER, INT, original_dir)
        self.stats_raster_intensity_published_path = os.path.join(self.path, stats_dir, RASTER, INT, published_dir)
        
        self.contour_path = os.path.join(self.path, contour_dir)
        self.contour_scratch_path = os.path.join(self.path, contour_dir, SCRATCH)
        
        if self.parent is not None:
            self.lasd_name = "{}.lasd".format(self.parent.projectId)
            self.lasd_path = os.path.join(self.path, self.lasd_name)
            
            self.fgdb_name = "{}.gdb".format(self.parent.projectId)
            self.fgdb_path = os.path.join(self.path, self.fgdb_name)
        
        self.pathList = [
                         self.demFirst_path,
                         self.demLast_path,
        self.demHeight_path,
        self.demLAll_path,
        
        self.intensity_path,
        self.intensity_first_path,
        
        self.elev_path,
        self.elev_first_path,
        self.elev_last_path,
        self.elev_all_path,
        
        self.lasClassified_path,
        self.lasClassified_lasd_path,
        
        self.lasUnclassified_path,
        self.lasUnclassified_lasd_path,
                         
                         
        self.pulse_count_path,
        self.pulse_count_all_path,
        self.pulse_count_first_path,
        self.pulse_count_last_path,
        
        self.point_count_path,
        self.point_count_all_path,
        self.point_count_first_path,
        self.point_count_last_path,
        
        self.predominant_last_return_path,
        self.predominant_last_return_all_path,
        self.predominant_last_return_first_path,
        self.predominant_last_return_last_path,
        
        self.predominant_class_path,
        self.predominant_class_all_path,
        self.predominant_class_first_path,
        self.predominant_class_last_path,
        
        self.intensity_range_path,
        self.intensity_range_all_path,
        self.intensity_range_first_path,
        self.intensity_range_last_path,
        
        self.z_range_path,
        self.z_range_all_path,
        self.z_range_first_path,
        self.z_range_last_path,
                         
                        self.stats_path,
        self.stats_las_path,
        
        self.stats_raster_path,
        
        self.stats_raster_dlm_path,
        self.stats_raster_dlm_derived_path,
        self.stats_raster_dlm_original_path,
        self.stats_raster_dlm_published_path,
        
        self.stats_raster_dtm_path,
        self.stats_raster_dtm_derived_path,
        self.stats_raster_dtm_original_path,
        self.stats_raster_dtm_published_path,
        
        self.stats_raster_dsm_path,
        self.stats_raster_dsm_derived_path,
        self.stats_raster_dsm_original_path,
        self.stats_raster_dsm_published_path,
        
        self.stats_raster_intensity_path,
        self.stats_raster_intensity_derived_path,
        self.stats_raster_intensity_original_path,
        self.stats_raster_intensity_published_path,
        
        self.contour_path,
        self.contour_scratch_path,
        self.log_path
                         ]
        
        
        for i, item in enumerate(self.dirList):
            self.pathDict[item] = self.pathList[i]
        
        
        
        
    def make(self, messages=[], errors=[], warnings=[]):
        if not(os.path.exists(self.path)):
            os.makedirs(self.path)
            messages.append("Created project directory '{}".format(self.path))
        else:
            warnings.append("Directory already exists '{}'".format(self.path))
        
        for item in self.pathList:
            if not(os.path.exists(item)):
                os.makedirs(item)
                messages.append("Created project directory '{}".format(item))
            else:
                warnings.append("Directory already exists '{}'".format(item))
        
        return messages, errors, warnings
















class Published(object):
    '''
    Published project directory
    
    '''
    parent = ""
    parent_dir = ""
    parent_path = "" 
    
    path = published_dir
    dirList = [          demFirst_dir,
                         demLast_dir
#                          demHeightTiff_dir
                         
                         ]
    
    demFirstTiff_path = os.path.join(path, demFirst_dir)
    demLastTiff_path = os.path.join(path, demLast_dir)
#     demHeightTiff_path = os.path.join(path, demHeightTiff_dir)
    
    las_path = os.path.join(path, las_dir)
    
    lasd_name = ".lasd"
    fgdb_name = ".gdb"
    lasd_path = os.path.join(path, lasd_name)
    fgdb_path = os.path.join(path, fgdb_name)
    
    
    pathList = [         demFirstTiff_path,
                         demLastTiff_path
#                          demHeightTiff_path
                         
                         ]
    
    pathDict = {}
    
    def __init__(self, parent, parent_dir, parent_path):
        '''
        parent - Parent directory class
        parent_dir - Parent project directory
        parent_path - Parent project folder path
        '''
        self.parent = parent
        self.parent_dir = parent_dir
        self.parent_path = parent_path
        
        self.path = os.path.join(self.parent_path, self.path)
        
        self.demFirstTiff_path = os.path.join(self.path, demFirst_dir)
        self.demLastTiff_path = os.path.join(self.path, demLast_dir)
#         self.demHeightTiff_path = os.path.join(self.path, demHeightTiff_dir)
        
        self.las_path = os.path.join(self.path, las_dir)
        
        if self.parent is not None:
            self.lasd_name = "{}.lasd".format(self.parent.projectId)
            self.lasd_path = os.path.join(self.path, self.lasd_name)
            
            self.fgdb_name = "{}.gdb".format(self.parent.projectId)
            self.fgdb_path = os.path.join(self.path, self.fgdb_name)
            
        
        self.pathList = [self.demFirstTiff_path,
                         self.demLastTiff_path
#                          self.demHeightTiff_path,
                         
                         ]
        
        
        for i, item in enumerate(self.dirList):
            self.pathDict[item] = self.pathList[i]
        
        
        
    def make(self, messages=[], errors=[], warnings=[]):
        if not(os.path.exists(self.path)):
            os.mkdir(self.path)
            messages.append("Created project directory '{}".format(self.path))
        else:
            warnings.append("Directory already exists '{}'".format(self.path))
        
        for item in self.pathList:
            if not(os.path.exists(item)):
                os.mkdir(item)
                messages.append("Created project directory '{}".format(item))
            else:
                warnings.append("Directory already exists '{}'".format(item))
        
        return messages, errors, warnings
    


class Metadata(object):
    '''
    Metadata project directory
    
    '''
    parent = ""
    parent_dir = ""
    parent_path = "" 
    
    path = metadata_dir
    dirList = []
    
    pathList = []
    
    pathDict = {}
    
    def __init__(self, parent, parent_dir, parent_path):
        '''
        parent - Parent directory class
        parent_dir - Parent project directory
        parent_path - Parent project folder path
        '''
        self.parent = parent
        self.parent_dir = parent_dir
        self.parent_path = parent_path
        
        self.path = os.path.join(self.parent_path, self.path)
        
    def make(self, messages=[], errors=[], warnings=[]):
        if not(os.path.exists(self.path)):
            os.mkdir(self.path)
            messages.append("Created project directory '{}".format(self.path))
        else:
            warnings.append("Directory already exists '{}'".format(self.path))
        
        for item in self.pathList:
            if not(os.path.exists(item)):
                os.mkdir(item)
                messages.append("Created project directory '{}".format(item))
            else:
                warnings.append("Directory already exists '{}'".format(item))
        
        return messages, errors, warnings

        



class Original(object):
    '''
    Original project directory
    
    '''
    parent = ""
    parent_dir = ""
    parent_path = "" 
    
    path = original_dir
    dirList = []
    
    pathList = []
    
    pathDict = {}
    
    def __init__(self, parent, parent_dir, parent_path):
        '''
        parent - Parent directory class
        parent_dir - Parent project directory
        parent_path - Parent project folder path
        '''
        self.parent = parent
        self.parent_dir = parent_dir
        self.parent_path = parent_path
        
        self.path = os.path.join(self.parent_path, self.path)
        
    def make(self, messages=[], errors=[], warnings=[]):
        if not(os.path.exists(self.path)):
            os.mkdir(self.path)
            messages.append("Created project directory '{}".format(self.path))
        else:
            warnings.append("Directory already exists '{}'".format(self.path))
        
        for item in self.pathList:
            if not(os.path.exists(item)):
                os.mkdir(item)
                messages.append("Created project directory '{}".format(item))
            else:
                warnings.append("Directory already exists '{}'".format(item))
        
        return messages, errors, warnings


class QA(object):
    '''
    QA project directory
    
    '''
    parent = ""
    parent_dir = ""
    parent_path = "" 
    
    path = qa_dir
    dirList = []
    
    pathList = []
    
    pathDict = {}
    
    def __init__(self, parent, parent_dir, parent_path):
        '''
        parent - Parent directory class
        parent_dir - Parent project directory
        parent_path - Parent project folder path
        '''
        self.parent = parent
        self.parent_dir = parent_dir
        self.parent_path = parent_path
        
        self.path = os.path.join(self.parent_path, self.path)
        
    def make(self, messages=[], errors=[], warnings=[]):
        if not(os.path.exists(self.path)):
            os.mkdir(self.path)
            messages.append("Created project directory '{}".format(self.path))
        else:
            warnings.append("Directory already exists '{}'".format(self.path))
        
        for item in self.pathList:
            if not(os.path.exists(item)):
                os.mkdir(item)
                messages.append("Created project directory '{}".format(item))
            else:
                warnings.append("Directory already exists '{}'".format(item))
        
        return messages, errors, warnings

class Project(object):
    '''
    Project directory
    
    '''
    parent_dir = ""
    parent_path = ""
    
    path = ""  
    projectId = None
    
    jobId = None
    archive_dir = ""
    archive_path = ""
    
    alias = None
    aliasClean = None
    UID = None
    
    dirList = [          delivered_dir,
                         derived_dir,
                         metadata_dir,
                         original_dir,
                         qa_dir,
                         published_dir
                         ]
    
    delivered = Delivered(parent=None, parent_path=parent_path, parent_dir=parent_dir)
    derived = Derived(parent=None, parent_path=parent_path, parent_dir=parent_dir)
    metadata = Metadata(parent=None, parent_path=parent_path, parent_dir=parent_dir)
    original = Original(parent=None, parent_path=parent_path, parent_dir=parent_dir)
    qa = QA(parent=None, parent_path=parent_path, parent_dir=parent_dir)
    published = Published(parent=None, parent_path=parent_path, parent_dir=parent_dir)
    
    pathList = [
                    delivered,
                    derived,
                    metadata,
                    original,
                    qa,
                    published
                ]
    
    def __init__(self,
                 parent_path=None,
                 projectId=None,
                 path=None,
                 archive_path=None,
                 jobId=None,
                 alias=None,
                 aliasClean=None,
                 UID=None):
        '''
        Either the project path or the parent_path and projectId are required
        
        parent_path = Parent folder path
        projectId = id of the project (st_aliasclean_year)
        path = folder path of the project
        
        Optional arguments 
        
        archive_path - Archive folder path
        jobId = the WMX job id of the project
        alias = the alias name of the project
        aliasClean = the clean alias name of the project
        '''
        
        self.parent_path = parent_path
        self.projectId = projectId
        self.path = path
        
        self.archive_path = archive_path
        self.jobId = jobId
        self.alias = alias
        self.aliasClean = aliasClean
        self.UID = UID
        
        if self.parent_path is None or len(self.parent_path) <= 0:
            if self.path is not None and len(self.path) > 0:
                self.parent_path = os.path.split(self.path)[0]
        
        if self.projectId is None or len(self.projectId) <= 0:
            if self.path is not None and len(self.path) > 0:
                self.projectId = os.path.split(self.path)[1]
        
        if self.path is None or len(self.path) <= 0:
            if self.parent_path is not None and len(self.parent_path) > 0:
                if self.projectId is not None and len(self.projectId) > 0:
                    self.path = os.path.join(self.parent_path, self.projectId)
        
        if self.parent_path is None or len(self.parent_path) <= 0:
            raise ValueError("Parent Path, Project ID, and Project Path can't all be None")
        
        if self.projectId is None or len(self.projectId) <= 0:
            raise ValueError("Parent Path, Project ID, and Project Path can't all be None") 
        
        if self.path is None or len(self.path) <= 0:
            raise ValueError("Parent Path, Project ID, and Project Path can't all be None")
    
        self.delivered = Delivered(parent=self, parent_path=self.path, parent_dir=self.projectId)
        self.derived = Derived(parent=self, parent_path=self.path, parent_dir=self.projectId)
        self.metadata = Metadata(parent=self, parent_path=self.path, parent_dir=self.projectId)
        self.original = Original(parent=self, parent_path=self.path, parent_dir=self.projectId)
        self.qa = QA(parent=self, parent_path=self.path, parent_dir=self.projectId)
        self.published = Published(parent=self, parent_path=self.path, parent_dir=self.projectId)
        
        
        self.pathList = [
                    self.delivered,
                    self.derived,
                    self.metadata,
                    self.original,
                    self.qa,
                    self.published
                ]
        
    def make(self):
        messages = [];
        errors = [];
        warnings = [];
        if os.path.exists(self.parent_path):
            
            if not(os.path.exists(self.path)):
                os.mkdir(self.path)
                messages.append("Created project directory '{}".format(self.path))
            else:
                warnings.append("Directory already exists '{}'".format(self.path))
            
            for item in self.pathList:
                messages, errors, warnings = item.make(messages, errors, warnings)
        else:
            errors.append("Parent directory does NOT exists '{}'".format(self.parent_path))
        
#         if len(errors) > 0:
#             raise Exception(errors)
        return messages, errors, warnings

    def getUncRoot(self):
        parent_path = self.parent_path
        root_path = os.path.splitunc(parent_path)[0]
        unc_path = os.path.split(root_path)[0] 
        while unc_path.rfind('\\') > 4:
            unc_path = unc_path[:unc_path.rfind('\\')]
        return unc_path
        

def getProjectFolderFromDBRow(ProjectJob, project_job_row):
    return Project(parent_path=ProjectJob.getParentDir(project_job_row),
                   projectId=ProjectJob.getProjectID(project_job_row),
                   path=ProjectJob.getProjectDir(project_job_row),
                   archive_path=ProjectJob.getArchiveDir(project_job_row),
                   jobId=ProjectJob.getWMXJobID(project_job_row),
                   alias=ProjectJob.getAlias(project_job_row),
                   aliasClean=ProjectJob.getAliasClean(project_job_row),
                   UID=ProjectJob.getUID(project_job_row))
    

'''
------------------------------------------------------------
Create a given folder and sub folders
------------------------------------------------------------
'''
def createFolder(target_path, parent, children, deleteIfExists=False):
    for parent in parent:
        for child in children:
            out_folder = os.path.join(target_path, parent)
            if len(str(child)) > 0:
                out_folder = os.path.join(target_path, parent, child)
            
            if deleteIfExists:
                shutil.rmtree(out_folder, True)
            if not os.path.exists(out_folder):
                os.makedirs(out_folder)
            

'''
------------------------------------------------------------
Create a standard set of analysis folders before the threads start
------------------------------------------------------------
'''
def createAnalysisFolders(target_path, isClassified=None):
    value_field = [ELEVATION]
    dataset_name = [FIRST, LAST, ALAST]
    createFolder(target_path, value_field, dataset_name)
    
    value_field = [INT]
    dataset_name = [FIRST]
    createFolder(target_path, value_field, dataset_name)
    
    value_field = [pulse_count_dir, point_count_dir, predominant_last_return_dir, predominant_class_dir, intensity_range_dir, z_range_dir]
    dataset_name = [FIRST, LAST, ALL]
    createFolder(target_path, value_field, dataset_name)
    
    value_field = [stats_dir]
    dataset_name = [LAS, RASTER]
    createFolder(target_path, value_field, dataset_name)
    
    if isClassified is not None:
        value_field = [lasClassified_dir]
        if isClassified:
            value_field = [lasUnclassified_dir]
        dataset_name = [lasd_dir]
        createFolder(target_path, value_field, dataset_name)
    
    value_field = [DLM, DHM, DTM, DSM]
    dataset_name = ["TEMP"]
    createFolder(target_path, value_field, dataset_name)

def createPublishFolders(publish_path):
    value_field = [DTM, DSM, DLM, INT]
    dataset_name = ['']
    createFolder(publish_path, value_field, dataset_name)
