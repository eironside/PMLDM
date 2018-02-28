'''
Created on Aug 1, 2017

@author: eric5946
---------------------------------------------
B_ shapes are external boundary files
C_ shapes are footprint files

Merge B_ shapes into an overall boundary, then clip all the C_ shapes
to make individual footprints. Do this because B_ shapes
have an accurate external shape (concave), but invalid internal shape (missing pixels at the boundary).
While C_ shapes are accurate at the internal boundaries (where tiles meet) but a 
convex hull around the external portions (so concave portions are inaccurate).

This creates an overall boundary out of B_ shapes
Clips all of the C_ shapes as footprints
Clips all of the derived ELE & INT images for each las file
migrates all the fields on the footprints and summarizes them in the boundary 
---------------------------------------------
'''


import arcpy
import datetime
import os

from ngce.Utility import deleteFileIfExists, doTime, alterFields
from ngce.cmdr import CMDRConfig
from ngce.raster.RasterConfig import FIELD_INFO, ELEV_TYPE, PATH, NAME, V_NAME, \
    V_UNIT, H_NAME, H_UNIT, H_WKID, NODATA_VALUE, AREA, MAX, MEAN, MIN, RANGE, \
    STAND_DEV, XMIN, YMIN, XMAX, YMAX, WIDTH, HEIGHT, MEAN_CELL_WIDTH, \
    MEAN_CELL_HEIGHT, BAND_COUNT, FORMAT, HAS_RAT, IS_INT, IS_TEMP, PIXEL_TYPE, \
    UNCOMP_SIZE, STAT_RASTER_FOLDER


def getRasterBoundaryPath(fgdb_path, elev_type=None):
    result = os.path.join(fgdb_path, "BoundaryRaster") 
    if elev_type is not None:
        result = "{}_{}".format(result, elev_type)
    return result

def getRasterFootprintPath(fgdb_path, elev_type=None):
    result = os.path.join(fgdb_path, "FootprintRaster") 
    if elev_type is not None:
        result = "{}_{}".format(result, elev_type)
    return result

'''
---------------------------------------------
Takes a set of footprints and merges them into a boundary
Optionally summarizes statistics
---------------------------------------------
'''
def createBoundaryFeatureClass(raster_footprint, target_raster_boundary, statistics_fields="", alter_field_infos=None):
    a = datetime.datetime.now()
    aa = a
    raster_boundary_1 = "{}1".format(target_raster_boundary)
    deleteFileIfExists(raster_boundary_1, True)
    arcpy.AddMessage("\tDissolving with statistics: {}".format(statistics_fields))
    arcpy.Dissolve_management(in_features=raster_footprint, out_feature_class=raster_boundary_1, dissolve_field=FIELD_INFO[ELEV_TYPE][0], statistics_fields=statistics_fields)
    a = doTime(a, "\tDissolved to {}".format(raster_boundary_1))
    
    alterFields(alter_field_infos, raster_boundary_1)
    
    raster_boundary_2 = "{}2".format(target_raster_boundary)
    deleteFileIfExists(raster_boundary_2, True)
    arcpy.Buffer_analysis(in_features=raster_boundary_1, out_feature_class=raster_boundary_2, buffer_distance_or_field="10 Meters", line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
    
    
    raster_boundary_3 = "{}3".format(target_raster_boundary)
    deleteFileIfExists(raster_boundary_3, True)
    arcpy.EliminatePolygonPart_management(in_features=raster_boundary_2, out_feature_class=raster_boundary_3, condition="AREA", part_area="10000 SquareMiles", part_area_percent="0", part_option="CONTAINED_ONLY")
    deleteFileIfExists(raster_boundary_2, True)
    
    raster_boundary_4 = "{}4".format(target_raster_boundary)
    deleteFileIfExists(raster_boundary_4, True)
    arcpy.SimplifyPolygon_cartography(in_features=raster_boundary_3, out_feature_class=raster_boundary_4, algorithm="BEND_SIMPLIFY", tolerance="20 Meters", minimum_area="0 Unknown", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP", in_barriers="")
    try:
        arcpy.DeleteField_management(in_table=raster_boundary_4, drop_field="Id;ORIG_FID;InPoly_FID;SimPgnFlag;MaxSimpTol;MinSimpTol")
    except:
        pass
    deleteFileIfExists(raster_boundary_3, True)
    
    deleteFileIfExists(target_raster_boundary, True)
    arcpy.Buffer_analysis(in_features=raster_boundary_4, out_feature_class=target_raster_boundary, buffer_distance_or_field="-10 Meters", line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
    deleteFileIfExists(raster_boundary_4, True)
    
    if alter_field_infos is not None and len(alter_field_infos) > 0:
        fields = ";".join([field[1] for field in alter_field_infos])
        arcpy.JoinField_management(in_data=target_raster_boundary, in_field="OBJECTID", join_table=raster_boundary_1, join_field="OBJECTID", fields=fields)
        # Utility.addToolMessages()
        
    deleteFileIfExists(raster_boundary_1, True)
    
    a = doTime(aa, "Dissolved raster footprints to dataset boundary {} ".format(target_raster_boundary))
    

def addFieldIfMissing(feature_class, fieldnames, field_info):
    field_name = field_info[0]
    if (False if field_name in fieldnames else True):
        arcpy.AddField_management(in_table=feature_class, field_name=field_info[0], field_alias=field_info[1], field_type=field_info[2], field_length=field_info[3], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
        # arcpy.CalculateField_management(in_table=feature_class, field=field_info[0], expression=0, expression_type="PYTHON_9.3")



'''
---------------------------------------------
creates a list of all of the standard statistics fields we are interested in
and the summary info we want to get from them
---------------------------------------------
'''
def getStatsFields(feature_class=None):
    a = datetime.datetime.now()
    base_fields = [
                   [FIELD_INFO[PATH], "FIRST"],
                   [FIELD_INFO[NAME], "COUNT"],
                   [FIELD_INFO[V_NAME], "FIRST"],
                   [FIELD_INFO[V_UNIT], "FIRST"],
                   [FIELD_INFO[H_NAME], "FIRST"],
                   [FIELD_INFO[H_UNIT], "FIRST"],
                   [FIELD_INFO[H_WKID], "FIRST"],
                   [FIELD_INFO[NODATA_VALUE], "FIRST"],
                  [FIELD_INFO[AREA], "SUM"],
                  [FIELD_INFO[ELEV_TYPE], "FIRST"],
                  [FIELD_INFO[MAX], "MAX"],
                  [FIELD_INFO[MEAN], "MEAN"],
                  [FIELD_INFO[MIN], "MIN"],
                  [FIELD_INFO[RANGE], "MAX"],
                  [FIELD_INFO[STAND_DEV], "MEAN"],
                  [FIELD_INFO[XMIN], "MIN"],
                  [FIELD_INFO[YMIN], "MIN"],
                  [FIELD_INFO[XMAX], "MAX"],
                  [FIELD_INFO[YMAX], "MAX"],
                  [FIELD_INFO[WIDTH], "MEAN"],
                  [FIELD_INFO[HEIGHT], "MEAN"],
                  [FIELD_INFO[MEAN_CELL_WIDTH], "MEAN"],
                  [FIELD_INFO[MEAN_CELL_HEIGHT], "MEAN"],
                  [FIELD_INFO[BAND_COUNT], "MAX"],
                  [FIELD_INFO[FORMAT], "FIRST"],
                  [FIELD_INFO[HAS_RAT], "FIRST"],
                  [FIELD_INFO[IS_INT], "FIRST"],
                  [FIELD_INFO[IS_TEMP], "FIRST"],
                  [FIELD_INFO[PIXEL_TYPE], "FIRST"],
                  [FIELD_INFO[UNCOMP_SIZE], "MEAN"]
                  ]
    
    # @TODO: Delete fields that don't exist in a given feature class?
    
    field_alter = []
    for base_field in base_fields:
        field_name = "{}_{}".format(base_field[1], base_field[0][0])
        new_field_name = base_field[0][0]
        new_field_alias = base_field[0][1]
        new_field = [field_name, new_field_name, new_field_alias]
        
        if new_field[0] == "COUNT_name":
            new_field = [field_name, field_name, "Number of Raster Files"]
        field_alter.append(new_field)
        # arcpy.AddMessage("Alter Field Name: '{}'".format(new_field))
    
    existing_fieldnames = None
    if feature_class is not None:
        existing_fieldnames = [field.name for field in arcpy.ListFields(feature_class)]
        
    summary_fields = []
    for base_field in base_fields:
        base_field_info = base_field[0]
        if existing_fieldnames is not None and (False if base_field_info[0] in existing_fieldnames else True):
            arcpy.AddMessage("Adding field {} to {}".format(base_field_info, feature_class))
            addFieldIfMissing(feature_class, existing_fieldnames, base_field_info)
        
        base_field_op = base_field[1]
        summary_field = "{} {}".format(base_field_info[0], base_field_op)
        summary_fields.append(summary_field)
        # arcpy.AddMessage("Summary Field Name: '{}'".format(summary_field))
        
    summary_string = ";".join(summary_fields)
    
    a = doTime(a, "Summary String")
    return summary_string, field_alter
        


def addProjectInfo(raster_footprint, raster_boundary, project_ID, project_path, project_UID):
    if len(project_path) > 999:
        project_path = project_path[0:999]
    for table in [raster_footprint, raster_boundary]:
        for field in [[CMDRConfig.PROJECT_ID, "TEXT", "50", project_ID],
                      [CMDRConfig.PROJECT_DIR, "TEXT", "1000", project_path],
                      [CMDRConfig.PROJECT_GUID, "GUID", "", project_UID]
                      ]:
            
            arcpy.AddField_management(in_table=table, field_name=field[0], field_alias=field[0], field_type=field[1], field_length=field[2], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
            arcpy.CalculateField_management(in_table=table, field=field[0], expression='"{}"'.format(field[3]), expression_type="PYTHON_9.3")

'''
---------------------------------------------
Generate the las dataset boundary and the footprints
and migrate, summarize all of the statistics that go 
along with this set of las files
---------------------------------------------
'''
def createRasterBoundaryAndFootprints(fgdb_path, target_path, project_ID, project_path, project_UID, elev_type):
    a = datetime.datetime.now()
    raster_footprint = None
    raster_boundary = None
        
    stat_out_folder = os.path.join(target_path, STAT_RASTER_FOLDER, elev_type)
    if not os.path.exists(stat_out_folder):
        arcpy.AddMessage("Raster statistics for elevation type don't exist: {}".format(stat_out_folder))
    else:
                    
        b_file_list = []
        for f_name in [f for f in os.listdir(stat_out_folder) if (f.startswith('B_') and f.endswith('.shp'))]:
            b_path = os.path.join(stat_out_folder, f_name)
                
            try:
                if not os.path.exists(b_path):
                    arcpy.AddWarning("Failed to find B boundary file {}".format(b_path))
                else:
                    b_file_list.append(b_path)
            except:
                pass
            
        a = doTime(a, "Found boundaries {}".format(len(b_file_list)))
        
        
        raster_footprint = getRasterFootprintPath(fgdb_path, elev_type)
        raster_boundary = getRasterBoundaryPath(fgdb_path, elev_type)
        if arcpy.Exists(raster_footprint):
            arcpy.AddMessage("Raster Footprints exist: {}".format(raster_footprint))
        else:
            
            deleteFileIfExists(raster_footprint, True)
            arcpy.Merge_management(inputs=b_file_list, output=raster_footprint)
            
            field_alter = []
            for base_field in FIELD_INFO:
                field_name = base_field[0]
                new_field_name = base_field[0]
                new_field_alias = base_field[1]
                new_field = [field_name, new_field_name, new_field_alias]
                
                field_alter.append(new_field)
            alterFields(field_alter, raster_footprint)
            
            a = doTime(a, "Merged raster footprints {}".format(raster_footprint))
            
        if arcpy.Exists(raster_boundary):
            arcpy.AddMessage("Raster Boundary exist: {}".format(raster_boundary))
        else:
            summary_string, field_alter = getStatsFields(raster_footprint)
            createBoundaryFeatureClass(raster_footprint, raster_boundary,    summary_string, field_alter)
            
            addProjectInfo(raster_footprint, raster_boundary, project_ID, project_path, project_UID)
            
    return raster_footprint, raster_boundary
            

if __name__ == '__main__':
    fgdb_path = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DERIVED\OK_SugarCreek_2008.gdb'
#     spatial_reference = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DELIVERED\LAS_CLASSIFIED\CR_NAD83UTM14N_NAVD88Meters.prj'
    target_path = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DERIVED'
    project_ID = "OK_SugarCreek_2008"
#     isClassified = True
    project_UID = None
    project_path = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008'
    elev_types = ['DTM']
     
    for elev_type in elev_types:
        createRasterBoundaryAndFootprints(fgdb_path, target_path, project_ID, project_path, project_UID, elev_type)
    # appendLasdStats(fgdb_path, spatial_reference, target_path, ProjectID, isClassified, ProjectUID)
    
