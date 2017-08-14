'''
Created on Aug 1, 2017

@author: eric5946
'''
'''
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
'''
---------------------------------------------
calculates the time difference between a and now and prints msg

returns the current time
---------------------------------------------
'''

import arcpy
import datetime
import os

from ngce.cmdr import CMDRConfig
from ngce.pmdm.a.A05_B_RevalueRaster import  MEAN, MAX, MIN, STAND_DEV, XMIN, XMAX, YMIN, YMAX, V_NAME, V_UNIT, H_NAME, H_UNIT, H_WKID, FIELD_INFO, \
    AREA, IS_CLASSIFIED, RANGE, FIRST_RETURNS, SECOND_RETURNS, THIRD_RETURNS, \
    FOURTH_RETURNS, SINGLE_RETURNS, FIRST_OF_MANY_RETURNS, LAST_OF_MANY_RETURNS, \
    ALL_RETURNS, POINT_COUNT, POINT_SPACING, NAME
from ngce.raster import RasterConfig


STAT_FOLDER = os.path.join("STATS", "LAS")

def getLasdBoundaryPath(fgdb_path):
    return os.path.join(fgdb_path, "LASDatasetBoundary")

def getLasFootprintPath(fgdb_path):
    return os.path.join(fgdb_path, "LASFileFootprint")

def doTime(a, msg):
    b = datetime.datetime.now()
    td = (b - a).total_seconds()
    arcpy.AddMessage("{} in {}".format(msg, td))

    return datetime.datetime.now()


def deleteFileIfExists(f_path, useArcpy=False):
    try:
        if useArcpy:
            if arcpy.Exists(f_path):
                arcpy.Delete_management(f_path)
        else:
            if os.path.exists(f_path):
                os.remove(f_path)
    except:
        pass
    
    

'''
---------------------------------------------
Clips a derived raster to a given footprint.
Used to clip convex hull to concave boundaries
---------------------------------------------
'''
def clipDerivedRaster(out_raster_path, vector_bound_path):
    #a = datetime.datetime.now()
    raster_name = os.path.split(out_raster_path)[1]
    if os.path.exists(out_raster_path) and not str(raster_name).startswith("C_"):
        raster_path, raster_name = os.path.split(out_raster_path) 
        clip_raster_path = os.path.join(raster_path, "C_{}".format(raster_name))
        #arcpy.AddMessage("Clipping raster {} to {}".format(out_raster_path, clip_raster_path))
        if os.path.exists(clip_raster_path):
            deleteFileIfExists(clip_raster_path, True)
        arcpy.Rename_management(out_raster_path, clip_raster_path)
        
        try:
            arcpy.Clip_management(in_raster=clip_raster_path, out_raster=out_raster_path, in_template_dataset=vector_bound_path, nodata_value=RasterConfig.NODATA_DEFAULT, clipping_geometry="ClippingGeometry", maintain_clipping_extent="NO_MAINTAIN_EXTENT")
        except:
            try:
                # NO Data value may not be applicable to this raster type
                arcpy.Clip_management(in_raster=clip_raster_path, out_raster=out_raster_path, in_template_dataset=vector_bound_path, nodata_value="", clipping_geometry="ClippingGeometry", maintain_clipping_extent="NO_MAINTAIN_EXTENT")
            except:
                pass
        deleteFileIfExists(out_raster_path, True)
    
        #doTime(a, "\tClip raster {}".format(clip_raster_path)) 
        return clip_raster_path

'''
---------------------------------------------
Clip a directory of rasters to the boundary
---------------------------------------------
'''
def clipRastersToBoundary(start_dir, boundary_path):
    a = datetime.datetime.now()
    for root, dirs, files in os.walk(start_dir):  # @UnusedVariable
        for f in files:
            if f.upper().endswith(".TIF"):
                raster_path = os.path.join(root, f)
                clipDerivedRaster(raster_path, boundary_path)
                
    doTime(a, "\tClip rasters {}".format(start_dir))

'''
---------------------------------------------
fix the field name
---------------------------------------------
'''
def alterField(in_table, field, new_field_name, new_field_alias):
    try:
        arcpy.AlterField_management(in_table=in_table, field=field, new_field_name=new_field_name, new_field_alias=new_field_alias)
    except:
        pass

'''
---------------------------------------------
Takes a set of footprints and merges them into a boundary
Optionally summarizes statistics
---------------------------------------------
'''
def createBoundaryFeatureClass(las_footprint, target_lasd_boundary, statistics_fields="", alter_field_infos=None):
    a = datetime.datetime.now()
    aa = a
    lasd_boundary_1 = "{}1".format(target_lasd_boundary)
    deleteFileIfExists(lasd_boundary_1, True)
    arcpy.AddMessage("\tDissolving with statistics: {}".format(statistics_fields))
    arcpy.Dissolve_management(in_features=las_footprint, out_feature_class=lasd_boundary_1, statistics_fields=statistics_fields)
    a = doTime(a, "\tDissolved to {}".format(lasd_boundary_1))
    
    if alter_field_infos is not None:
        for alter_field_info in alter_field_infos:
            try:
                alterField(lasd_boundary_1, alter_field_info[0], alter_field_info[1], alter_field_info[2])                 
            except:
                pass
    
        a = doTime(a, "\tRenamed summary fields")
    
    lasd_boundary_2 = "{}2".format(target_lasd_boundary)
    deleteFileIfExists(lasd_boundary_2, True)
    arcpy.Buffer_analysis(in_features=lasd_boundary_1, out_feature_class=lasd_boundary_2, buffer_distance_or_field="10 Meters", line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
    
    
    lasd_boundary_3 = "{}3".format(target_lasd_boundary)
    deleteFileIfExists(lasd_boundary_3, True)
    arcpy.EliminatePolygonPart_management(in_features=lasd_boundary_2, out_feature_class=lasd_boundary_3, condition="AREA", part_area="10000 SquareMiles", part_area_percent="0", part_option="CONTAINED_ONLY")
    deleteFileIfExists(lasd_boundary_2, True)
    
    lasd_boundary_4 = "{}4".format(target_lasd_boundary)
    deleteFileIfExists(lasd_boundary_4, True)
    arcpy.SimplifyPolygon_cartography(in_features=lasd_boundary_3, out_feature_class=lasd_boundary_4, algorithm="BEND_SIMPLIFY", tolerance="20 Meters", minimum_area="0 Unknown", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP", in_barriers="")
    deleteFileIfExists(lasd_boundary_3, True)
    
    deleteFileIfExists(target_lasd_boundary, True)
    arcpy.Buffer_analysis(in_features=lasd_boundary_4, out_feature_class=target_lasd_boundary, buffer_distance_or_field="-10 Meters", line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
    deleteFileIfExists(lasd_boundary_4, True)
    
    if alter_field_infos is not None and len(alter_field_infos) > 0:
        fields = ";".join([field[1] for field in alter_field_infos])
        arcpy.JoinField_management(in_data=target_lasd_boundary, in_field="OBJECTID", join_table=lasd_boundary_1, join_field="OBJECTID", fields=fields)
        # Utility.addToolMessages()
        
    deleteFileIfExists(lasd_boundary_1, True)
    
    a = doTime(aa, "Dissolved las footprints to dataset boundary {} ".format(target_lasd_boundary))
    

'''
---------------------------------------------
creates a list of all of the standard statistics fields we are interested in
and the summary info we want to get from them
---------------------------------------------
'''
def getStatsFields():
    a = datetime.datetime.now()
    base_fields = [
                   [FIELD_INFO[NAME], "COUNT"],
                   [FIELD_INFO[IS_CLASSIFIED], "FIRST"],
                   [FIELD_INFO[V_NAME], "FIRST"],
                   [FIELD_INFO[V_UNIT], "FIRST"],
                   [FIELD_INFO[H_NAME], "FIRST"],
                   [FIELD_INFO[H_UNIT], "FIRST"],
                   [FIELD_INFO[H_WKID], "FIRST"],
                  [FIELD_INFO[AREA], "SUM"],
                  [FIELD_INFO[MAX], "MAX"],
                  [FIELD_INFO[MEAN], "MEAN"],
                  [FIELD_INFO[MIN], "MIN"],
                  [FIELD_INFO[RANGE], "MAX"],
                  [FIELD_INFO[STAND_DEV], "MEAN"],
                  [FIELD_INFO[XMIN], "MIN"],
                  [FIELD_INFO[YMIN], "MIN"],
                  [FIELD_INFO[XMAX], "MAX"],
                  [FIELD_INFO[YMAX], "MAX"]
                  ]
    
    class_fields = [
                    FIELD_INFO[FIRST_RETURNS],
                    FIELD_INFO[SECOND_RETURNS],
                    FIELD_INFO[THIRD_RETURNS],
                    FIELD_INFO[FOURTH_RETURNS],
                    FIELD_INFO[SINGLE_RETURNS],
                    FIELD_INFO[FIRST_OF_MANY_RETURNS],
                    FIELD_INFO[LAST_OF_MANY_RETURNS],
                    FIELD_INFO[ALL_RETURNS]
                    ]
    value_fields = [
                  [FIELD_INFO[POINT_COUNT], "SUM"],
                  [FIELD_INFO[MAX], "MAX"],
                  [FIELD_INFO[MIN], "MIN"],
                  [FIELD_INFO[RANGE], "MAX"]
                  ]
    class_value_fields = [
                  [FIELD_INFO[POINT_COUNT], "SUM"],
                  [FIELD_INFO[POINT_SPACING], "MEAN"],
                  [FIELD_INFO[MAX], "MAX"],
                  [FIELD_INFO[MIN], "MIN"],
                  [FIELD_INFO[RANGE], "MAX"]
                  ]
    for clazz in range(0, 18):
        if clazz <> 7 and clazz <> 18:
            value = "c{}".format(("0{}".format(clazz))[-2:])
            class_fields.append([value, "Class {}".format(value), "", ""])
    for class_field_info in class_fields:
        class_field = class_field_info[0]
        field_values = value_fields
        if class_field.startswith("c"):
            field_values = class_value_fields
        for value_field_record in field_values:
            value_field_info = value_field_record[0]
            value_field_summary = value_field_record[1]
            base_fields.append([["{}_{}".format(class_field_info[0], value_field_info[0]), "{} {}".format(class_field_info[1], value_field_info[1]) , value_field_info[2], value_field_info[3]], value_field_summary])
    
    # @TODO: Delete fields that don't exist in a given feature class?
    
    field_alter = []
    for base_field in base_fields:
        field_name = "{}_{}".format(base_field[1], base_field[0][0])
        new_field_name = base_field[0][0]
        new_field_alias = base_field[0][1]
        new_field = [field_name, new_field_name, new_field_alias]
        
        if new_field[0] == "COUNT_name":
            new_field = [field_name, field_name, "Number of LAS Files"]
        field_alter.append(new_field)
        # arcpy.AddMessage("Alter Field Name: '{}'".format(new_field))
    
    summary_fields = []
    for base_field in base_fields:
        base_field_info = base_field[0]
        base_field_op = base_field[1]
        summary_field = "{} {}".format(base_field_info[0], base_field_op)
        summary_fields.append(summary_field)
        # arcpy.AddMessage("Summary Field Name: '{}'".format(summary_field))
        
    summary_string = ";".join(summary_fields)
    
    a = doTime(a, "Summary String")
    return summary_string, field_alter
        

'''
---------------------------------------------
Shape files don't allow NULL (@#!$%^&!) so we have to re-cacluate
anything that is 0 for most numeric fields (but not all) to null. 
---------------------------------------------
'''
def checkNullFields(las_footprint):
    a = datetime.datetime.now()
    base_fields = [
                   FIELD_INFO[AREA][0],
              FIELD_INFO[MAX][0],
              FIELD_INFO[MEAN][0],
              FIELD_INFO[MIN][0],
              FIELD_INFO[RANGE][0],
              FIELD_INFO[STAND_DEV][0],
              FIELD_INFO[XMIN][0],
              FIELD_INFO[YMIN][0],
              FIELD_INFO[XMAX][0],
              FIELD_INFO[YMAX][0]
              ]
    
    class_fields = [
                    FIELD_INFO[FIRST_RETURNS][0],
                    FIELD_INFO[SECOND_RETURNS][0],
                    FIELD_INFO[THIRD_RETURNS][0],
                    FIELD_INFO[FOURTH_RETURNS][0],
                    FIELD_INFO[SINGLE_RETURNS][0],
                    FIELD_INFO[FIRST_OF_MANY_RETURNS][0],
                    FIELD_INFO[LAST_OF_MANY_RETURNS][0],
                    FIELD_INFO[ALL_RETURNS][0]
                    ]
    value_fields = [
                  FIELD_INFO[POINT_SPACING][0],
                  FIELD_INFO[MAX][0],
                  FIELD_INFO[MIN][0],
                  FIELD_INFO[RANGE][0]
                  ]
    for clazz in range(0, 18):
        if clazz <> 7 and clazz <> 18:
            value = "c{}".format(("0{}".format(clazz))[-2:])
            class_fields.append(value)
    for class_field in class_fields:
        for value_field in value_fields:
            base_fields.append("{}_{}".format(class_field, value_field))
    
    
    for field in base_fields:
        # arcpy.AddMessage("Nulling field '{}'".format(field))
        try:
            arcpy.CalculateField_management(in_table=las_footprint, field=field, expression="checkNull( !{}! )".format(field), expression_type="PYTHON_9.3", code_block="def checkNull( value ):\n   if value == 0:\n      value = None\n   return value")
        except:
            pass
    a = doTime(a, "Checked for nulls on las footprints {}".format(las_footprint))


def addProjectInfo(las_footprint, lasd_boundary, project_ID, project_path, project_UID):
    if len(project_path) > 999:
        project_path = project_path[0:999]
    for table in [las_footprint, lasd_boundary]:
        for field in [[CMDRConfig.PROJECT_ID, "TEXT", "50", project_ID],
                      [CMDRConfig.PROJECT_DIR, "TEXT", "1000", project_path],
                      [CMDRConfig.PROJECT_GUID, "GUID", "", project_UID]
                      ]:
            
            arcpy.AddField_management(in_table=table, field_name=field[0], field_alias=field[0], field_type=field[1], field_length=field[2], field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED")
            arcpy.CalculateField_management(in_table=table, field=CMDRConfig.PROJECT_ID, expression='"{}"'.format(field[3]), expression_type="PYTHON_9.3")

'''
---------------------------------------------
Generate the las dataset boundary and the footprints
and migrate, summarize all of the statistics that go 
along with this set of las files
---------------------------------------------
'''
def createLasdBoundaryAndFootprints(fgdb_path, target_path, project_ID, project_path, project_UID):
    a = datetime.datetime.now()
    
    stat_out_folder = os.path.join(target_path, STAT_FOLDER)
    
                    
    b_file_list = []
    c_file_list = []
    for f_name in [f for f in os.listdir(stat_out_folder) if (f.startswith('B_') and f.endswith('.shp'))]:
        b_path = os.path.join(stat_out_folder, f_name)
        c_path = os.path.join(stat_out_folder, "C{}".format(f_name[1:]))
            
        try:
            if not os.path.exists(b_path):
                arcpy.AddWarning("Failed to find B boundary file {}".format(b_path))
            else:
                b_file_list.append(b_path)
        except:
            pass
        try:
            if not os.path.exists(c_path):
                arcpy.AddWarning("Failed to find C boundary file {}".format(c_path))
            else:
                c_file_list.append(c_path)
        except:
            pass
    
    a = doTime(a, "Found boundaries {}".format(len(b_file_list)))
    
    
    las_footprint = getLasFootprintPath(fgdb_path)
    las_footprint_1 = os.path.join(fgdb_path, "{}1".format(las_footprint))
    deleteFileIfExists(las_footprint_1, True)
    arcpy.Merge_management(inputs=b_file_list, output=las_footprint_1)
    
    a = doTime(a, "Merged las footprints {}".format(las_footprint_1))
    
    lasd_boundary = getLasdBoundaryPath(fgdb_path)
    lasd_boundary_temp = os.path.join(fgdb_path, "LASDatasetBoundary")
    
    createBoundaryFeatureClass(las_footprint_1, lasd_boundary_temp)
    a = datetime.datetime.now()
    
    # Merge the other footprints before clipping
    deleteFileIfExists(las_footprint_1, True)
    arcpy.Merge_management(inputs=c_file_list, output=las_footprint_1)
    
    a = doTime(a, "Merged las footprints {}".format(las_footprint_1))
    
    checkNullFields(las_footprint_1)
    a = datetime.datetime.now()
    
    deleteFileIfExists(las_footprint, True)
    arcpy.Clip_analysis(in_features=las_footprint_1, clip_features=lasd_boundary, out_feature_class=las_footprint, cluster_tolerance="")
    deleteFileIfExists(las_footprint_1, True)
    a = doTime(a, "Clipped las footprints to dataset boundary {} ".format(las_footprint))
    
    summary_string, field_alter = getStatsFields()
    createBoundaryFeatureClass(las_footprint, lasd_boundary, summary_string, field_alter)
    
    addProjectInfo(las_footprint, lasd_boundary, project_ID, project_path, project_UID)
    
    a = datetime.datetime.now()
    # Clip rasters in ELEVATION since they used a convex hull
    start_dir = os.path.join(target_path, "ELEVATION")
    clipRastersToBoundary(start_dir, lasd_boundary)
    
    a = doTime(a, "Clipped ELEVATION to dataset boundary {} ".format(lasd_boundary))


# if __name__ == '__main__':
#     fgdb_path = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DERIVED\OK_SugarCreek_2008.gdb'
# #     spatial_reference = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DELIVERED\LAS_CLASSIFIED\CR_NAD83UTM14N_NAVD88Meters.prj'
#     target_path = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DERIVED'
#     ProjectID = "eric"
# #     isClassified = True
#     ProjectUID = None
#     
#     createLasdBoundaryAndFootprints(fgdb_path, target_path)
#     # appendLasdStats(fgdb_path, spatial_reference, target_path, ProjectID, isClassified, ProjectUID)
    