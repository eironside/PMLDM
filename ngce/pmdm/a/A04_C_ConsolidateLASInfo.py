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

from ngce import Utility
from ngce.Utility import deleteFileIfExists, doTime
from ngce.cmdr import CMDRConfig
from ngce.folders.FoldersConfig import STATS_METHODS, DATASET_NAMES
from ngce.raster import RasterConfig, Raster
from ngce.raster.RasterConfig import FIELD_INFO, NAME, IS_CLASSIFIED, V_NAME, \
    V_UNIT, H_NAME, H_UNIT, H_WKID, AREA, MAX, MEAN, MIN, RANGE, STAND_DEV, XMIN, \
    YMIN, XMAX, YMAX, FIRST_RETURNS, SECOND_RETURNS, THIRD_RETURNS, \
    FOURTH_RETURNS, SINGLE_RETURNS, FIRST_OF_MANY_RETURNS, LAST_OF_MANY_RETURNS, \
    ALL_RETURNS, POINT_COUNT, POINT_SPACING, STAT_LAS_FOLDER, CANOPY_DENSITY

PARTITION_COUNT = 500

def getLasdBoundaryPath(fgdb_path):
    return os.path.join(fgdb_path, "BoundaryLASDataset")

def getLasFootprintPath(fgdb_path):
    return os.path.join(fgdb_path, "FootprintLASFile")



def deleteField(in_table, drop_field):
    arcpy.AddMessage("\t\tDeleting field '{}' from '{}'".format(drop_field, in_table))
    try:    
        arcpy.DeleteField_management(in_table=in_table, drop_field=drop_field)
    except:
        arcpy.AddWarning("\tWARNING: Failed to delete field '{}' from '{}'".format(drop_field, in_table))
        pass

def deleteFields(in_table):
    fields = arcpy.ListFields(in_table)
    existing_fields = []
    for field in fields:
        existing_fields.append(field.name)
        
    arcpy.AddMessage("\t\tDropping unused fields. Existing fields in '{}' from '{}'".format(existing_fields, in_table))
    drop_fields=["MinSimpTol", "MaxSimpTol", "Orig_FID", "InPoly_FID", "SimPgnFlag", "Id",
                 "MINSIMPTOL", "MAXSIMPTOL", "ORIG_FID", "INPOLY_FID", "SIMPGNFLAG", "ID",
                 "minsimptol", "maxsimptol", "orig_fid", "inpoly_fid", "simpgnflag", "id"]
    for drop_field in drop_fields:
        #arcpy.AddMessage("\t\tTrying to drop field '{}' from '{}'".format(drop_field, in_table))
        if drop_field in existing_fields:
           deleteField(in_table, drop_field)
        
        


def importMosaicDatasetGeometries(md_path, footprint_path, lasd_boundary_path):
    if footprint_path is not None:
        arcpy.ImportMosaicDatasetGeometry_management(md_path, target_featureclass_type="FOOTPRINT", target_join_field="Name", input_featureclass=footprint_path, input_join_field="name")
        Utility.addToolMessages()
    if lasd_boundary_path is not None:
        arcpy.ImportMosaicDatasetGeometry_management(md_path, target_featureclass_type="BOUNDARY", target_join_field="OBJECTID", input_featureclass=lasd_boundary_path, input_join_field="OBJECTID")
        Utility.addToolMessages()

'''
---------------------------------------------
Clips a derived raster to a given footprint.
Used to clip convex hull to concave boundaries
---------------------------------------------
'''
def clipDerivedRaster(out_raster_path, vector_bound_path):
    # a = datetime.datetime.now()
    raster_name = os.path.split(out_raster_path)[1]
    if os.path.exists(out_raster_path) and not str(raster_name).startswith("C_"):
        raster_path, raster_name = os.path.split(out_raster_path)
        clip_raster_path = os.path.join(raster_path, "C_{}".format(raster_name))
        # arcpy.AddMessage("Clipping raster {} to {}".format(out_raster_path, clip_raster_path))
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

        # doTime(a, "\tClip raster {}".format(clip_raster_path))
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


def createQARasterMosaicDataset(md_name, gdb_path, spatial_reference, input_folder, mxd, footprint_path=None, lasd_boundary_path=None):
    Utility.printArguments(["md_name", "gdb_path", "spatial_reference", "input_folder", "mxd", "footprint_path", "lasd_boundary_path"],
                           [md_name, gdb_path, spatial_reference, input_folder, mxd, footprint_path, lasd_boundary_path], "A04_C CreateQARasterMosaicDatasets")

    md_path = os.path.join(gdb_path, md_name)

    a = datetime.datetime.now()

    if arcpy.Exists(md_path):
        arcpy.AddMessage("\tMD Exists: {}".format(md_path))
    else:
        try:
            raster_count = 0
            arcpy.AddMessage("\tLooking for rasters to add to {} in folder {}".format(md_path, input_folder))
            for root, dirs, files in os.walk(input_folder):  # @UnusedVariable
                for f in files:
                    if f.upper().endswith(".TIF"):
                        raster_count = raster_count+1

            if raster_count <=0:
                arcpy.AddMessage("\tNo rasters to add to {} in folder {}".format(md_path, input_folder))
            else:

                # Create a MD in same SR as LAS Dataset
                arcpy.CreateMosaicDataset_management(in_workspace=gdb_path,
                                                     in_mosaicdataset_name=md_name,
                                                     coordinate_system=spatial_reference,
                                                     num_bands="",
                                                     pixel_type="",
                                                     product_definition="NONE",
                                                     product_band_definitions="")

                arcpy.SetMosaicDatasetProperties_management(in_mosaic_dataset=md_path, rows_maximum_imagesize="4100", columns_maximum_imagesize="15000", allowed_compressions="None;JPEG;LZ77;LERC", default_compression_type="LERC", JPEG_quality="75", LERC_Tolerance="0.01", resampling_type="CUBIC", clip_to_footprints="NOT_CLIP", footprints_may_contain_nodata="FOOTPRINTS_DO_NOT_CONTAIN_NODATA", clip_to_boundary="CLIP", color_correction="NOT_APPLY", allowed_mensuration_capabilities="Basic", default_mensuration_capabilities="Basic", allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None", default_mosaic_method="NorthWest", order_field="", order_base="", sorting_order="ASCENDING", mosaic_operator="FIRST", blend_width="10", view_point_x="600", view_point_y="300", max_num_per_mosaic="2000", cell_size_tolerance="0.8", cell_size="10 10", metadata_level="BASIC", transmission_fields="Name;MinPS;MaxPS;LowPS;HighPS;Tag;GroupName;ProductName;CenterX;CenterY;ZOrder;Shape_Length;Shape_Area;Thumbnail", use_time="DISABLED", start_time_field="", end_time_field="", time_format="", geographic_transform="", max_num_of_download_items="20", max_num_of_records_returned="1000", data_source_type="GENERIC", minimum_pixel_contribution="1", processing_templates="None", default_processing_template="None", time_interval="", time_interval_units="")
                a = doTime(a, "\tCreated MD {}".format(md_name))

                arcpy.AddRastersToMosaicDataset_management(in_mosaic_dataset=md_path,
                                                           raster_type="Raster Dataset",
                                                           input_path=input_folder,
                                                           update_cellsize_ranges="UPDATE_CELL_SIZES",
                                                           update_boundary="UPDATE_BOUNDARY",
                                                           update_overviews="NO_OVERVIEWS",
                                                           maximum_pyramid_levels="",
                                                           maximum_cell_size="0",
                                                           minimum_dimension="150",
                                                           spatial_reference="",
                                                           filter="#",
                                                           sub_folder="SUBFOLDERS",
                                                           duplicate_items_action="EXCLUDE_DUPLICATES",
                                                           build_pyramids="BUILD_PYRAMIDS",
                                                           calculate_statistics="CALCULATE_STATISTICS",
                                                           build_thumbnails="BUILD_THUMBNAILS",
                                                           operation_description="#",
                                                           force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE",
                                                           estimate_statistics="ESTIMATE_STATISTICS",
                                                           aux_inputs="")

                total_rows = int(arcpy.GetCount_management(md_path).getOutput(0))
                if total_rows <= 0:
                    arcpy.AddWarning("No rasters found for '{}'".format(md_path))
                    deleteFileIfExists(md_path, True)
                else:
                    try:
                        importMosaicDatasetGeometries(md_path, footprint_path, lasd_boundary_path)
                    except:
                        arcpy.AddWarning("Failed to update MD boundaries for '{}'".format(md_path))

                a = doTime(a, "\tAdded Rasters to MD {}".format(md_name))


        except:
            arcpy.AddWarning("Failed to create MD for QA Raster Layer '{}'. Please remove any locks and delete related intermediate files".format(md_path))

    return [md_path, md_name]
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
def createBoundaryFeatureClass(raster_footprint, target_raster_boundary, statistics_fields="", alter_field_infos=None):
    a = datetime.datetime.now()
    aa = a
    lasd_boundary_1 = "{}1".format(target_raster_boundary)
    deleteFileIfExists(lasd_boundary_1, True)
    arcpy.AddMessage("\tBuffering")
    arcpy.Buffer_analysis(in_features=raster_footprint, out_feature_class=lasd_boundary_1, buffer_distance_or_field="10 Meters", line_side="FULL", line_end_type="ROUND", dissolve_option="NONE", method="PLANAR")
    Utility.addToolMessages()

    lasd_boundary_2 = "{}2".format(target_raster_boundary)
    deleteFileIfExists(lasd_boundary_2, True)
    arcpy.AddMessage("\tDissolving with statistics: {}".format(statistics_fields))
    arcpy.Dissolve_management(in_features=lasd_boundary_1, out_feature_class=lasd_boundary_2, statistics_fields=statistics_fields)
    a = doTime(a, "\tDissolved to {}".format(lasd_boundary_2))

    if alter_field_infos is not None:
        for alter_field_info in alter_field_infos:
            try:
                alterField(lasd_boundary_2, alter_field_info[0], alter_field_info[1], alter_field_info[2])
            except:
                pass

        a = doTime(a, "\tRenamed summary fields")

    lasd_boundary_3 = "{}3".format(target_raster_boundary)
    deleteFileIfExists(lasd_boundary_3, True)
    arcpy.EliminatePolygonPart_management(in_features=lasd_boundary_2, out_feature_class=lasd_boundary_3, condition="AREA", part_area="10000 SquareMiles", part_area_percent="0", part_option="CONTAINED_ONLY")
    deleteFileIfExists(lasd_boundary_2, True)

    lasd_boundary_4 = "{}4".format(target_raster_boundary)
    deleteFileIfExists(lasd_boundary_4, True)
    arcpy.SimplifyPolygon_cartography(in_features=lasd_boundary_3, out_feature_class=lasd_boundary_4, algorithm="BEND_SIMPLIFY", tolerance="20 Meters", minimum_area="0 Unknown", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP", in_barriers="")
    deleteFields(lasd_boundary_4)
    #try:
    #    arcpy.DeleteField_management(in_table=lasd_boundary_4, drop_field="Id;ORIG_FID;InPoly_FID;SimPgnFlag;MaxSimpTol;MinSimpTol")
    #except:
    #    pass
    deleteFileIfExists(lasd_boundary_3, True)

    deleteFileIfExists(target_raster_boundary, True)
    arcpy.Buffer_analysis(in_features=lasd_boundary_4, out_feature_class=target_raster_boundary, buffer_distance_or_field="-10 Meters", line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", method="PLANAR")
    deleteFileIfExists(lasd_boundary_4, True)

    if alter_field_infos is not None and len(alter_field_infos) > 0:
        fields = ";".join([field[1] for field in alter_field_infos])
        arcpy.JoinField_management(in_data=target_raster_boundary, in_field="OBJECTID", join_table=lasd_boundary_1, join_field="OBJECTID", fields=fields)
        # Utility.addToolMessages()

    deleteFileIfExists(lasd_boundary_1, True)

    a = doTime(aa, "Dissolved las footprints to dataset boundary {} ".format(target_raster_boundary))

def addFieldsIfMissing(feature_class, field_list=None):
    fieldnames = [field.name for field in arcpy.ListFields(feature_class)]
    if field_list is not None:
        for field_info in field_list:
            addFieldIfMissing(feature_class, fieldnames, field_info)
    return fieldnames

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
        if class_field.startswith("c") or class_field.startswith("ra"):
            field_values = class_value_fields
        for value_field_record in field_values:
            value_field_info = value_field_record[0]
            value_field_summary = value_field_record[1]
            base_fields.append([["{}_{}".format(class_field_info[0], value_field_info[0]), "{} {}".format(class_field_info[1], value_field_info[1]) , value_field_info[2], value_field_info[3]], value_field_summary])
        
            
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

    arcpy.AddMessage("Summary String = '{}'".format(summary_string))
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
def createRasterBoundaryAndFootprints(fgdb_path, target_path, project_ID, project_path, project_UID):
    a = datetime.datetime.now()

    stat_out_folder = os.path.join(target_path, STAT_LAS_FOLDER)

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
                deleteFields(c_path)
        except:
            pass

    a = doTime(a, "Found {} footprints".format(len(b_file_list)))


    las_footprint = getLasFootprintPath(fgdb_path)
    lasd_boundary = getLasdBoundaryPath(fgdb_path)
    if arcpy.Exists(las_footprint):
        arcpy.AddMessage("Footprints exist: {}".format(las_footprint))
    else:
        # Delete the boundary if the footprints don't exist (have to recreate anyway)
        deleteFileIfExists(lasd_boundary, True)
        lasd_boundary_B = "{}B".format(lasd_boundary)
        deleteFileIfExists(lasd_boundary_B, True)

        las_footprint_1 = os.path.join(fgdb_path, "{}B1".format(las_footprint))
        las_footprint_2 = os.path.join(fgdb_path, "{}B2".format(las_footprint))
        las_footprint_CP = os.path.join(fgdb_path, "{}B_CP".format(las_footprint))
        a = doTime(a, "\tMerging B footprints to {}".format(las_footprint_2))
        deleteFileIfExists(las_footprint_1, True)
        deleteFileIfExists(las_footprint_2, True)
        deleteFileIfExists(las_footprint_CP, True)
        arcpy.Merge_management(inputs=b_file_list, output=las_footprint_2)
        Utility.addToolMessages()
        deleteFields(las_footprint_2)
        arcpy.RepairGeometry_management(in_features=las_footprint_2, delete_null="DELETE_NULL")
        Utility.addToolMessages()
        a = doTime(a, "\tMerged B and repaired footprints to {}".format(las_footprint_2))
        
        #arcpy.CreateCartographicPartitions_cartography(in_features=las_footprint_2, out_features=las_footprint_CP, feature_count=PARTITION_COUNT)
        #a = doTime(a, "\tCreated B carto parts")

        #arcpy.env.cartographicPartitions = las_footprint_CP
        #a = doTime(a, "\tSet B cartographic partitions to {}".format(las_footprint_CP))
        
        #arcpy.SimplifyPolygon_cartography(in_features=las_footprint_2, out_feature_class=las_footprint_1, algorithm="POINT_REMOVE", tolerance="0.5 Meters", minimum_area="0 Unknown", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP", in_barriers="")
        #Utility.addToolMessages()
        #arcpy.env.cartographicPartitions = None
        #a = doTime(a, "\tSimplified B las footprints to {}".format(las_footprint_1))
        #deleteFileIfExists(las_footprint_2, True)
        #arcpy.RepairGeometry_management(in_features=las_footprint_1, delete_null="DELETE_NULL")
        #Utility.addToolMessages()

        #a = doTime(a, "Merged B las footprints {}".format(las_footprint_1))

        createBoundaryFeatureClass(las_footprint_2, lasd_boundary_B)
        deleteFileIfExists(las_footprint_2, True)
        a = datetime.datetime.now()
        

        # Merge the other footprints before clipping
        las_footprint_1 = os.path.join(fgdb_path, "{}C1".format(las_footprint))
        las_footprint_2 = os.path.join(fgdb_path, "{}C2".format(las_footprint))
        las_footprint_CP = os.path.join(fgdb_path, "{}C_CP".format(las_footprint))
        deleteFileIfExists(las_footprint_1, True)
        deleteFileIfExists(las_footprint_2, True)
        deleteFileIfExists(las_footprint_CP, True)
        a = doTime(a, "\tMerging C las footprints to {}".format(las_footprint_2))
        arcpy.Merge_management(inputs=c_file_list, output=las_footprint_2)
        Utility.addToolMessages()
        deleteFields(las_footprint_2)
        arcpy.RepairGeometry_management(in_features=las_footprint_2, delete_null="DELETE_NULL")
        Utility.addToolMessages()
        a = doTime(a, "\tMerged C las footprints to {}".format(las_footprint_2))

        #arcpy.CreateCartographicPartitions_cartography(in_features=las_footprint_2, out_features=las_footprint_CP, feature_count=PARTITION_COUNT)
        #a = doTime(a, "\tCreated C carto parts")

        #arcpy.env.cartographicPartitions = las_footprint_CP
        #a = doTime(a, "\tSet C cartographic partitions to {}".format(las_footprint_CP))
        
        #arcpy.SimplifyPolygon_cartography(in_features=las_footprint_2, out_feature_class=las_footprint_1, algorithm="POINT_REMOVE", tolerance="0.5 Meters", minimum_area="0 Unknown", error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP", in_barriers="")
        #Utility.addToolMessages()
        #arcpy.env.cartographicPartitions = None
        #a = doTime(a, "\tSimplified C las footprints to {}".format(las_footprint_1))
        #deleteFileIfExists(las_footprint_2, True)
        #arcpy.RepairGeometry_management(in_features=las_footprint_1, delete_null="DELETE_NULL")
        #Utility.addToolMessages()
        
        #a = doTime(a, "Merged C las footprints {}".format(las_footprint_1))

        lasd_boundary_C = "{}C".format(lasd_boundary)
        deleteFileIfExists(lasd_boundary_C, True)
        createBoundaryFeatureClass(las_footprint_2, lasd_boundary_C)

        lasd_boundary_SD = "{}_SD".format(lasd_boundary)
        lasd_boundary_SD1 = "{}_SD1".format(lasd_boundary)
        lasd_boundary_SD2 = "{}_SD2".format(lasd_boundary)
        lasd_boundary_SD3 = "{}_SD3".format(lasd_boundary)
        deleteFileIfExists(lasd_boundary_SD, True)
        deleteFileIfExists(lasd_boundary_SD1, True)
        deleteFileIfExists(lasd_boundary_SD2, True)
        deleteFileIfExists(lasd_boundary_SD3, True)
        # Replace a layer/table view name with a path to a dataset (which can be a layer file) or create the layer/table view within the script
        arcpy.SymDiff_analysis(in_features=lasd_boundary_B, update_features=lasd_boundary_C, out_feature_class=lasd_boundary_SD1, join_attributes="ONLY_FID", cluster_tolerance="3 Meters")
        arcpy.Buffer_analysis(in_features=lasd_boundary_SD1, out_feature_class=lasd_boundary_SD2, buffer_distance_or_field="-3 Meters", line_side="FULL", line_end_type="ROUND", dissolve_option="NONE", dissolve_field="", method="PLANAR")
        arcpy.MultipartToSinglepart_management(in_features=lasd_boundary_SD2, out_feature_class=lasd_boundary_SD3)
        arcpy.Buffer_analysis(in_features=lasd_boundary_SD3, out_feature_class=lasd_boundary_SD, buffer_distance_or_field="3 Meters", line_side="FULL", line_end_type="ROUND", dissolve_option="NONE", dissolve_field="", method="PLANAR")
        arcpy.DeleteField_management(in_table=lasd_boundary_SD, drop_field="FID_BoundaryLASDatasetB;FID_BoundaryLASDatasetC;BUFF_DIST;ORIG_FID")

        deleteFileIfExists(lasd_boundary_C, True)
        deleteFileIfExists(lasd_boundary_SD1, True)
        deleteFileIfExists(lasd_boundary_SD2, True)
        deleteFileIfExists(lasd_boundary_SD3, True)

        a = doTime(a, "Created symetrical difference in boundaries {}".format(lasd_boundary_SD))

        checkNullFields(las_footprint_2)
        a = datetime.datetime.now()

        deleteFileIfExists(las_footprint, True)
        arcpy.Clip_analysis(in_features=las_footprint_2, clip_features=lasd_boundary_B, out_feature_class=las_footprint, cluster_tolerance="")
        #deleteFileIfExists(las_footprint_1, True)
        deleteFileIfExists(lasd_boundary_B, True)

        #deleteFileIfExists(lasd_boundary, True)
        #deleteFileIfExists(las_footprint_CP, True)
        #arcpy.env.cartographicPartitions = None

        deleteFileIfExists(las_footprint_2, True)
        a = doTime(a, "Clipped las footprints to dataset boundary {} ".format(las_footprint))

    deleteFields(las_footprint)
    try:
        arcpy.RepairGeometry_management(in_features=las_footprint, delete_null="KEEP_NULL")
    except:
        pass

    if arcpy.Exists(lasd_boundary):
        arcpy.AddMessage("Boundary exists: {}".format(lasd_boundary))
    else:
        deleteFileIfExists(lasd_boundary, True)

        summary_string, field_alter = getStatsFields(las_footprint)
        createBoundaryFeatureClass(las_footprint, lasd_boundary, summary_string, field_alter)

        addProjectInfo(las_footprint, lasd_boundary, project_ID, project_path, project_UID)

    deleteFields(lasd_boundary)

    try:
        arcpy.RepairGeometry_management(in_features=lasd_boundary, delete_null="KEEP_NULL")
    except:
        pass

    return lasd_boundary, las_footprint

def createReferenceddMosaicDataset(in_md_path, out_md_path, spatial_ref, raster_v_unit):
    a = datetime.datetime.now()
    arcpy.CreateReferencedMosaicDataset_management(in_dataset=in_md_path, out_mosaic_dataset=out_md_path, coordinate_system=spatial_ref, number_of_bands="1", pixel_type="32_BIT_SIGNED", where_clause="", in_template_dataset="", extent="", select_using_features="SELECT_USING_FEATURES", lod_field="", minPS_field="", maxPS_field="", pixelSize="", build_boundary="BUILD_BOUNDARY")

    raster_function_path = Raster.Canopy_Density_function_chain_path

    arcpy.EditRasterFunction_management(in_mosaic_dataset=out_md_path, edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET", edit_options="REPLACE", function_chain_definition=raster_function_path, location_function_name="")
    Utility.addToolMessages()

    # arcpy.CalculateStatistics_management(in_raster_dataset=out_md_path, x_skip_factor="1", y_skip_factor="1", ignore_values="", skip_existing="OVERWRITE", area_of_interest="Feature Set")

    arcpy.AddMessage("\tNOTE: !!! Please edit the MR Point Density function. Change to replace input to 'Multiply LAST by 100' with this project's POINT_COUNT_LAST mosaic dataset.\n\n\t{}\n".format(out_md_path))
    doTime(a, "Created DHM '{}'".format(out_md_path))



def createQARasterMosaics(isClassified, gdb_path, spatial_reference, target_folder, mxd, footprint_path=None, lasd_boundary_path=None):
    mosaics = []
    simple_footprint_path = None
    simple_lasd_boundary_path = None

    stats_methods = STATS_METHODS
    for method in stats_methods:
        arcpy.AddMessage("Creating {} MDS".format(method))
        for dataset_name in DATASET_NAMES:
            name = dataset_name

            if not isClassified:
                # Using a generic name for non-classified data
                name = ""


            md_name = method
            if len(name) > 0:
                md_name = "{}{}".format(method, name)

            input_folder = os.path.join(target_folder, method, name[1:])

            arcpy.AddMessage("Creating {} MD from {}".format(md_name, input_folder))
            try:
                if simple_footprint_path is None:
                    simple_footprint_path = "{}_Simple".format(footprint_path)
                    arcpy.SimplifyPolygon_cartography(in_features=footprint_path, out_feature_class=simple_footprint_path,
                                                    algorithm="POINT_REMOVE", tolerance=Raster.boundary_interval, minimum_area="0 SquareMeters",
                                                    error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP")
                    Utility.addToolMessages()
                    deleteFields(simple_footprint_path)
                    #try:
                    #    arcpy.DeleteField_management(in_table=simple_footprint_path, drop_field="Id;ORIG_FID;InPoly_FID;SimPgnFlag;MaxSimpTol;MinSimpTol")
                    #except:
                    #    pass

                if simple_lasd_boundary_path is None:
                    simple_lasd_boundary_path = "{}_Simple".format(lasd_boundary_path)
                    arcpy.SimplifyPolygon_cartography(in_features=lasd_boundary_path, out_feature_class=simple_lasd_boundary_path,
                                                    algorithm="POINT_REMOVE", tolerance=Raster.boundary_interval, minimum_area="0 SquareMeters",
                                                    error_option="RESOLVE_ERRORS", collapsed_point_option="NO_KEEP")
                    Utility.addToolMessages()
                    deleteFields(simple_lasd_boundary_path)
                    #try:
                    #    arcpy.DeleteField_management(in_table=simple_lasd_boundary_path, drop_field="Id;ORIG_FID;InPoly_FID;SimPgnFlag;MaxSimpTol;MinSimpTol")
                    #except:
                    #    pass
            except:
                arcpy.AddWarning("Failed to create simplified footprints and boundaries in '{}'".format(gdb_path))

            qa_md = createQARasterMosaicDataset(md_name, gdb_path, spatial_reference, input_folder, mxd, simple_footprint_path, simple_lasd_boundary_path)
            if qa_md is not None:
                mosaics.append(qa_md)






    md_name = CANOPY_DENSITY
    dhm_md_path = os.path.join(gdb_path, md_name)
    mosaics.append([dhm_md_path, md_name])
    
    if arcpy.Exists(dhm_md_path):
        arcpy.AddMessage("{} already exists.".format(md_name))
    else:
        try:
            vert_cs_name, vert_unit_name = Utility.getVertCSInfo(spatial_reference)  # @UnusedVariable
            # No need to update boundary and footprints since it will inherit from the original
            pc_all_md_path = os.path.join(gdb_path, "POINT_COUNT_ALL")
            createReferenceddMosaicDataset(pc_all_md_path, dhm_md_path, spatial_reference, vert_unit_name)
        except:
            arcpy.AddMessage("Failed to create {}".format(dhm_md_path))

    
    deleteFileIfExists(simple_footprint_path, True)
    deleteFileIfExists(simple_lasd_boundary_path, True)

    return mosaics
#     a = datetime.datetime.now()

    # DO this later once the elevation products are needed
#     # Clip rasters in ELEVATION since they used a convex hull
#     start_dir = os.path.join(target_path, "ELEVATION")
#     clipRastersToBoundary(start_dir, lasd_boundary)
#
#     a = doTime(a, "Clipped ELEVATION to dataset boundary {} ".format(lasd_boundary))


if __name__ == '__main__':
    fgdb_path = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DERIVED\OK_SugarCreek_2008.gdb'
#     spatial_reference = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DELIVERED\LAS_CLASSIFIED\CR_NAD83UTM14N_NAVD88Meters.prj'
    target_path = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DERIVED'
    project_ID = "OK_SugarCreek_2008"
#     isClassified = True
    project_UID = None
    project_path = r'E:\NGCE\RasterDatasets\OK_SugarCreek_2008'

    createRasterBoundaryAndFootprints(fgdb_path, target_path, project_ID, project_path, project_UID)
    # appendLasdStats(fgdb_path, spatial_reference, target_path, ProjectID, isClassified, ProjectUID)

