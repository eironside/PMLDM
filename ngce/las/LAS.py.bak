'''
Created on Jan 5, 2016

@author: eric5946
'''
import arcpy
import os

from ngce import Utility
from ngce.cmdr import CMDRConfig
from ngce.las import LASConfig


MIN_VALID_ELEVATION = -430.0  # Meters
MAX_VALID_ELEVATION = 8850.0  # Meters

# Used for QA LAS Boundary/footprints
LAS_raster_type_20_all_bin_mean_idw = os.path.join(os.path.dirname(os.path.realpath(__file__)), LASConfig.LAS_RASTER_TYPE_20_All_Bin_Mean_IDW)
# Used to add LAS to the project MD
LAS_raster_type_1_all_bin_mean_idw = os.path.join(os.path.dirname(os.path.realpath(__file__)), LASConfig.LAS_RASTER_TYPE_1_All_Bin_Mean_IDW) 

def validateZRange(minZ, maxZ):
    arcpy.AddMessage("Checking Min Z value {}".format(minZ))
    if minZ is None or minZ < MIN_VALID_ELEVATION or minZ > MAX_VALID_ELEVATION:
        arcpy.AddWarning("Min Z is not valid, resetting to default {}".format(MIN_VALID_ELEVATION))
        minZ = MIN_VALID_ELEVATION
    
    arcpy.AddMessage("Checking Max Z value {}".format(maxZ))    
    if maxZ is None or maxZ < MIN_VALID_ELEVATION or maxZ > MAX_VALID_ELEVATION:
        arcpy.AddWarning("Max Z is not valid, resetting to default {}".format(MAX_VALID_ELEVATION))
        maxZ = MAX_VALID_ELEVATION
    
    if minZ > maxZ:
        arcpy.AddWarning("Min Z > Max Z, resetting to defaults {} -> {}".format(MIN_VALID_ELEVATION, MAX_VALID_ELEVATION))
        minZ = MIN_VALID_ELEVATION
        maxZ = MAX_VALID_ELEVATION 
    
    return minZ, maxZ



def updateMDLASFootprints(filegdb_path, md_path, area_percent, point_interval):
    updateMDLASGeometry("FOOTPRINT", filegdb_path, md_path, area_percent, point_interval)
    
def updateMDLASBoundary(filegdb_path, md_path, area_percent, point_interval):
    updateMDLASGeometry("BOUNDARY", filegdb_path, md_path, area_percent, point_interval)
        
def updateMDLASGeometry(geometry_type, filegdb_path, md_path, area_percent, point_interval):
    '''
    geometry_type = ["BOUNDARY", "FOOTPRINT"]
    '''
    geometry_name = "LAS"
    # Create an in-memory feature class to hold the geometry
    geometry_export = os.path.join(filegdb_path, "{}_{}_Export".format(geometry_type, geometry_name))
    if arcpy.Exists(geometry_export):
        arcpy.Delete_management(geometry_export)
        Utility.addToolMessages()
    
    # Export the geometry to the in-memory feature class  
    arcpy.ExportMosaicDatasetGeometry_management(md_path, geometry_export, where_clause="#", geometry_type=geometry_type)
    Utility.addToolMessages()
    
    # Remove the holes and save to a feature class in the file geodatabase
    geometry_no_holes = os.path.join(filegdb_path, "{}_{}_NoHoles".format(geometry_type, geometry_name))
    if arcpy.Exists(geometry_no_holes):
        arcpy.Delete_management(geometry_no_holes)
        Utility.addToolMessages()
    
    arcpy.EliminatePolygonPart_management(geometry_export, geometry_no_holes, condition="PERCENT", part_area="0 SquareMeters", part_area_percent=area_percent, part_option="CONTAINED_ONLY")
    Utility.addToolMessages()

    # Smooth the polygons
    geometry_smooth = os.path.join(filegdb_path, "{}_{}".format(geometry_type, geometry_name))
    if arcpy.Exists(geometry_smooth):
        arcpy.Delete_management(geometry_smooth)
        Utility.addToolMessages()
    
    arcpy.SmoothPolygon_cartography(geometry_no_holes, geometry_smooth, "PAEK", point_interval, "FIXED_ENDPOINT", "NO_CHECK")
    Utility.addToolMessages()
      
    # Clean up
    if arcpy.Exists(geometry_export):
        arcpy.Delete_management(geometry_export)
        Utility.addToolMessages()
    if arcpy.Exists(geometry_no_holes):
        arcpy.Delete_management(geometry_no_holes)
        Utility.addToolMessages()
    
    # import simplified Footprints/boundary
    arcpy.ImportMosaicDatasetGeometry_management(md_path, target_featureclass_type=geometry_type, target_join_field="OBJECTID",
                                                 input_featureclass=geometry_smooth, input_join_field="OBJECTID")
    Utility.addToolMessages()
    

class QALasInfo(object):
    # Meters are converted to other units in methods below
    LASQARaster_cellSize = 10  # Meters (default, will be converted to map units)
    boundary_interval = "{} Meters".format(LASConfig.SIMPLIFY_INTERVAL)  # Length to simplify the boundary shapes
    
    # Build Footprints using min_region_size="20" and approx_num_vertices="200"
    # MIN & MAX data values are used for calculating footprints, not valid Z values
    LAS_Footprint_MIN_REGION_SIZE = 20  # Pixels
    LAS_Footprint_APPROX_NUM_VERTS = 200  # 200 vertices per footprint/boundary
    LAS_Footprint_MIN_DATA_VALUE = 1  # Meters, ignore values (for boundary calc) less than 1
    LAS_Footprint_MAX_DATA_VALUE = 6200  # Meters, ignore values (for boundary calc) greater than 6200 
    
    project_folder = None
    ProjectID = None
    target_path = None
    target_stats_path = None
    filegdb_name = None
    filegdb_path = None
    
    las_directory = None
    
    feature_dataset_id = None
    feature_dataset_name = CMDRConfig.fcName_LASFileInfo
    feature_dataset_path = None
    
    las_dataset_name = None 
    las_dataset_path = None
    las_summary_name = LASConfig.LASDataset_FileSummaryTable_Name
    las_summary_path = None
    las_summary_fc_name = CMDRConfig.fcName_LASFileSummary
    las_summary_fc_path = None
    las_stats_dataset_file_path = None
    las_stats_files_file_path = None
    las_stats_dataset_path = None
    las_stats_files_path = None
    
    prj_file_name = None
    
    num_las_files = None
    first_las_name = None
    
    prj_spatial_ref = None
    lasd_spatial_ref = None
    
    
    
    LASMD_Name = None
    LASMD_path = None
    
    LASDatasetFileCount = 0
            
    pt_count_dsm = 0
    pt_spacing_dsm = 0
    minZ_dsm = 0
    maxZ_dsm = 0
    
    pt_count_dtm = 0
    pt_spacing_dtm = 0
    minZ_dtm = 0
    maxZ_dtm = 0
    
    output_values = []
    
    def __init__(self, project_folder, isClassified=True):
        self.project_folder = project_folder
        self.isClassified = isClassified
        
        self.ProjectID = project_folder.projectId
        self.target_path = project_folder.derived.path
        self.target_stats_path = project_folder.derived.stats_path
        
        self.las_directory = project_folder.delivered.lasClassified_path
        if not(self.isClassified):
            self.las_directory = project_folder.delivered.lasUnclassified_path
        
        self.filegdb_name = "{}.gdb".format(self.ProjectID)
        self.filegdb_path = os.path.join(self.target_path, self.filegdb_name)
            
        self.feature_dataset_id
        
        self.feature_dataset_path = os.path.join(self.filegdb_path, self.feature_dataset_name)
        self.las_summary_path = os.path.join(self.filegdb_path, self.las_summary_name)
        self.las_summary_fc_path = os.path.join(self.filegdb_path, self.las_summary_fc_name)
        
        self.las_dataset_name = "{}.lasd".format(self.ProjectID) 
        self.las_dataset_path = os.path.join(self.target_path, self.las_dataset_name)
        
        self.las_stats_dataset_file_path = os.path.join(self.target_stats_path, "{}_statistics.txt".format(self.las_dataset_name))
        self.las_stats_files_file_path = os.path.join(self.target_stats_path, "{}_file_statistics.txt".format(self.las_dataset_name))
        
        self.las_stats_dataset_path = os.path.join(self.filegdb_path, LASConfig.LASDataset_DatasetStats_Name)
        self.las_stats_files_path = os.path.join(self.filegdb_path, LASConfig.LASDataset_FileStats_Name)        
        
        self.LASMD_Name = "LAS".format(self.ProjectID)
        self.LASMD_path = os.path.join(self.filegdb_path, self.LASMD_Name)
        
        self.las_boundary_path = os.path.join(self.filegdb_path, "BOUNDARY_LAS")
        self.las_footprint_path = os.path.join(self.filegdb_path, "FOOTPRINT_LAS")
        
        self.LASDatasetPointCount = 0 
        self.LASDatasetFileCount = 0
                
        self.pt_count_dsm = 0
        self.pt_spacing_dsm = 0
        self.minZ_dsm = 0
        self.maxZ_dsm = 0
        
        self.pt_count_dtm = 0
        self.pt_spacing_dtm = 0
        self.minZ_dtm = 0
        self.maxZ_dtm = 0
                                              
    def setProjectionFile(self, projection_file_name):
        self.prj_file_name = projection_file_name
        return self.getPRJSpatialReference()
    
    def getPRJSpatialReference(self):
        if self.prj_spatial_ref is None:
            if self.prj_file_name is not None:
                self.prj_spatial_ref = arcpy.SpatialReference(os.path.join(self.las_directory, self.prj_file_name))
        
        return self.prj_spatial_ref

    def getSpatialReference(self):
        result = None
        try:
            if self.lasd_spatial_ref is not None:
                result = self.lasd_spatial_ref
            elif self.prj_spatial_ref is not None:
                result = self.prj_spatial_ref
        except:
            pass
        return result
    
    def getGCSSpatialReference(self):
        result = None
        try:
            if self.prj_spatial_ref is not None and self.prj_spatial_ref.PCSCode is not None and self.prj_spatial_ref.PCSCode > 0 and self.prj_spatial_ref.GCS is not None:
                result = self.prj_spatial_ref.GCS
            elif self.lasd_spatial_ref is not None and self.lasd_spatial_ref.PCSCode is not None and self.lasd_spatial_ref.PCSCode > 0 and self.lasd_spatial_ref.GCS is not None:
                result = self.lasd_spatial_ref.GCS
        except:
            pass
        return result
    
    def isValidSpatialReference(self):
        sr = self.getSpatialReference()
        result = False
        if sr is not None:
            result = (sr.factoryCode is not None and sr.factoryCode > 0)
            if sr.factoryCode is None:
                arcpy.AddMessage("Spatial reference exists, but WKID (EPSG Code) is None. The custom projection for this project may not be able to be converted when published to the master.")
            elif sr.factoryCode <= 0:
                arcpy.AddMessage("Spatial reference exists, but WKID (EPSG Code) is {}. The custom projection for this project may not be able to be converted when published to the master.".format(sr.factoryCode))
            else:
                arcpy.AddMessage("Spatial reference exists and WKID (EPSG Code) is {}. This is a valid spatial reference.".format(sr.factoryCode))
        else:
            arcpy.AddMessage("Spatial reference is None, please add a .prj file to the las directory or spatial reference information to the las files.")
        arcpy.AddMessage(sr.exportToString())
        return result

    def isUnknownSpatialReference(self):
        sr = self.getSpatialReference()
        result = False
        if sr is not None:
            result = sr.name is None or sr.name.lower() == 'unknown'
            if result:
                arcpy.AddMessage("Spatial reference is UNKNOWN, please add a .prj file to the las directory or spatial reference information to the las files.")
        else:
            result = True
        return result

    def getLASQARasterCellFactor(self):
        factor = 1
#         if self.isValidSpatialReference():
        sr = self.getSpatialReference()
        if sr is not None:
            unitCode = sr.linearUnitCode
            
            if unitCode == 9002:  #    International foot
                factor = 0.3048
            elif unitCode == 9003:  #     US survey foot
                factor = 0.3048006096
            
        return factor
            
    def getLASQARasterCellSize(self):
        return (self.LASQARaster_cellSize / self.getLASQARasterCellFactor())
    
    
    
    def getLASFootprintMinDataValue(self):
        return (self.LAS_Footprint_MIN_DATA_VALUE / self.getLASQARasterCellFactor())
    
    def getLASFootprintMaxDataValue(self):
        return (self.LAS_Footprint_MAX_DATA_VALUE / self.getLASQARasterCellFactor())
