from datetime import datetime
from multiprocessing import Pool, cpu_count
import os
import sys

import arcpy
import arcpyproduction  # @UnresolvedImport
from functools import partial
from ngce import Utility
from ngce.Utility import doTime
from ngce.cmdr.JobUtil import getProjectFromWMXJobID
from ngce.contour import ContourConfig
from ngce.contour.ContourConfig import CONTOUR_GDB_NAME, CONTOUR_NAME_OCS
from ngce.folders import ProjectFolders
import shutil


def gen_base_tiling_scheme(base_fc, scratch):

    arcpy.env.overwriteOutput = True

    # Copy Template MXD
    base_mxd = arcpy.mapping.MapDocument(ContourConfig.MXD_TEMPLATE)
    section_mxd_name = os.path.join(scratch, 'Tiling_Scheme.mxd')
    base_mxd.saveACopy(section_mxd_name)

    # Set MXD For Processing
    first_mxd = arcpy.mapping.MapDocument(section_mxd_name)

    # Set Layers to Reference Input FC
    broken = arcpy.mapping.ListBrokenDataSources(first_mxd)
    db = os.path.split(base_fc)[0]
    fc = os.path.split(base_fc)[1]
    for item in broken:
        if item.name.startswith(r'Contour'):
            item.replaceDataSource(db, "FILEGDB_WORKSPACE", fc)
    first_mxd.save()

    # Generate Tiling Scheme for Input MXD
    base_tiling_scheme = os.path.join(db, 'Base_Tiling_Scheme')
    arcpy.MapServerCacheTilingSchemeToPolygons_cartography(
        map_document=first_mxd.filePath,
        data_frame='Layers',
        tiling_scheme=ContourConfig.TILING_SCHEME,
        output_feature_class=base_tiling_scheme,
        use_map_extent='USE_MAP_EXTENT',
        clip_to_horizon='CLIP_TO_HORIZON',
        antialiasing='ANTIALIASING',
        levels="9027.977411;4513.988705;2256.994353;1128.497176"
    )

    return base_tiling_scheme


def contour_prep(in_fc, scheme_poly, scratch, name):

    print 'Started: ', name

    arcpy.env.overwriteOutput = True

    try:
        # Copy Template MXD
        base_mxd = arcpy.mapping.MapDocument(ContourConfig.MXD_TEMPLATE)
        section_mxd_name = os.path.join(scratch, name, name + '.mxd')
        base_mxd.saveACopy(section_mxd_name)

        # Set MXD For Processing
        mxd = arcpy.mapping.MapDocument(section_mxd_name)

        # Set Layers to Reference Input FC
        broken = arcpy.mapping.ListBrokenDataSources(mxd)
        db = os.path.split(in_fc)[0]
        fc = os.path.split(in_fc)[1]
        for item in broken:
            if item.name.startswith(r'Contour'):
                item.replaceDataSource(db, "FILEGDB_WORKSPACE", fc)
        mxd.save()

        # Create FGDB For Annotation Storage
        scratch_db = os.path.join(scratch, name, name + '.gdb')
        filter_folder = os.path.join(scratch, name)
        if arcpy.Exists(scratch_db):
            pass
        else:
            arcpy.CreateFileGDB_management(filter_folder, name + '.gdb')

        # Filter for Section of Input FC
        feat = arcpy.MakeFeatureLayer_management(
            in_features=in_fc,
            out_layer=name,
            where_clause="name='" + name + "'"
        )

        # Select Subsection of Tiling Scheme
        sel_lyr = arcpy.MakeFeatureLayer_management(scheme_poly, 'scheme_poly')
        arcpy.SelectLayerByLocation_management(
            in_layer=sel_lyr,
            overlap_type="INTERSECT",
            select_features=feat,
            selection_type="NEW_SELECTION",
            invert_spatial_relationship="NOT_INVERT"
        )

        # Save Subsection of Tilinng Scheme for TiledLabelsToAnnotation
        target_scheme_polys = os.path.join(scratch_db, name + '_scheme_polys')
        arcpy.CopyFeatures_management(
            in_features=sel_lyr,
            out_feature_class=target_scheme_polys
        )

        # Enable Labels for TiledLabelsToAnnotation tool
        for lyr in arcpy.mapping.ListLayers(mxd):
            if lyr.name.upper().startswith("CONTOUR"):
                lyr.showLabels = True

        # Reference Annotation FCs created with TiledLabelsToAnnotation
        df = arcpy.mapping.ListDataFrames(mxd, 'Layers')[0]
        anno1128 = os.path.join(scratch_db, r"Contours_1128Anno1128")
        anno2257 = os.path.join(scratch_db, r"Contours_2257Anno2256")
        anno4514 = os.path.join(scratch_db, r"Contours_4514Anno4513")
        anno9028 = os.path.join(scratch_db, r"Contours_9028Anno9027")

        # Delete Existing Annotation FCs to Avoid Confusion with TiledLabelsToAnnotation Output
        for a in [anno1128, anno2257, anno4514, anno9028]:
            arcpy.Delete_management(in_data=a, data_type='FeatureClass')

        # Create Annotation with Filtered FC Extent
        arcpy.TiledLabelsToAnnotation_cartography(
            map_document=mxd.filePath,
            data_frame='Layers',
            polygon_index_layer=target_scheme_polys,
            out_geodatabase=scratch_db,
            out_layer='GroupAnno',
            anno_suffix='Anno',
            reference_scale_value='9028',
            reference_scale_field="Tile_Scale",
            tile_id_field="OBJECTID",
            feature_linked="STANDARD",
            generate_unplaced_annotation="NOT_GENERATE_UNPLACED_ANNOTATION"
        )
        mxd.save()

        # Create layer files for each of the Anno feature classes, and add to the map
        annotation_set = [
            [anno1128, "Contours_1128Anno1128.lyr", "Cont_1128Anno1128"],
            [anno2257, "Contours_2257Anno2256.lyr", "Cont_2257Anno2256"],
            [anno4514, "Contours_4514Anno4513.lyr", "Cont_4514Anno4513"],
            [anno9028, "Contours_9028Anno9027.lyr", "Cont_9028Anno9027"]
        ]

        # Create .lyr Files & Add to MXD
        df = arcpy.mapping.ListDataFrames(mxd, 'Layers')[0]
        for a in annotation_set:
            lyr = os.path.join(filter_folder, a[1])
            arcpy.MakeFeatureLayer_management(a[0], a[2])
            arcpy.SaveToLayerFile_management(
                in_layer=a[2],
                out_layer=lyr,
                is_relative_path='ABSOLUTE',
                version='CURRENT'
            )
            add_lyr = arcpy.mapping.Layer(lyr)
            arcpy.mapping.AddLayer(df, add_lyr, 'BOTTOM')
        mxd.save()

        # Create Mask FC to Hide Contour Beneath Annotation
        arcpy.env.workspace = filter_folder
        for lyr_file in arcpy.ListFiles('Contours*.lyr'):
            try:
                lyr_path = os.path.join(filter_folder, lyr_file)
                ref_scale = lyr_file[9:13]
                mask_fc = os.path.join(scratch_db, r'Mask' + ref_scale)
                arcpy.FeatureOutlineMasks_cartography(
                    input_layer=lyr_path,
                    output_fc=mask_fc,
                    reference_scale=ref_scale,
                    spatial_reference=ContourConfig.WEB_AUX_SPHERE,
                    margin='1 Points',
                    method='BOX',
                    mask_for_non_placed_anno='ALL_FEATURES',
                    attributes='ALL'
                )
            except Exception as e:
                print 'Exception:', e
                pass
        mxd.save()

    except Exception as e:
        print 'Dropped: ', name
        print 'Exception: ', e

    print 'Finished: ', name


def db_list_gen(scratch, dirs, names):

    lists = [
        [],
        [],
        [],
        []
    ]

    for dir_name in dirs:
        db = os.path.join(scratch, dir_name, dir_name + '.gdb')
        e_names = enumerate(names)
        for index, name in e_names:
            target = os.path.join(db, name)
            if arcpy.Exists(target):
                lists[index].append(target)
    return lists


def run_merge(lists, results):

    arcpy.env.overwriteOutput = True

    # Merge Each Set of Inputs
    for x in lists:
        output_name = os.path.split(x[0])[1]
        arcpy.Merge_management(x, os.path.join(results, output_name))


def handle_merge(scratch):

    print 'Merging Multiprocessing Results'

    arcpy.env.overwriteOutput = True

    # Create FGDB For Annotation/Mask Storage
    results = os.path.join(scratch, 'RESULTS.gdb')
    if arcpy.Exists(results):
        pass
    else:
        arcpy.CreateFileGDB_management(scratch, 'RESULTS.gdb')

    # Create Folder Directory for MXD files
    results_folder = os.path.join(scratch, 'RESULTS')
    if os.path.exists(results_folder):
        pass
    else:
        os.mkdir(results_folder)

    # Get Directories in Scratch Folder
    dirs = [name for name in os.listdir(scratch) if os.path.isdir(os.path.join(scratch, name))]

    # Annotation Names
    a_names = [
        'Contours_1128Anno1128',
        'Contours_2257Anno2256',
        'Contours_4514Anno4513',
        'Contours_9028Anno9027'
    ]

    # Mask Names
    m_names = [
        'Mask1128',
        'Mask2257',
        'Mask4514',
        'Mask9028'
    ]

    # Generate DB Merge Lists
    a_list = db_list_gen(scratch, dirs, a_names)
    m_list = db_list_gen(scratch, dirs, m_names)

    # Merge Features
    run_merge(a_list, results)
    run_merge(m_list, results)

    return results, results_folder


def build_results_mxd(in_fc, final_db, folder):

    print 'Create Results MXD'

    arcpy.env.overwriteOutput = True

    # Copy Template MXD
    base_mxd = arcpy.mapping.MapDocument(ContourConfig.MXD_TEMPLATE)
    section_mxd_name = os.path.join(folder, 'Results.mxd')
    base_mxd.saveACopy(section_mxd_name)

    # Set MXD For Processing
    final_mxd = arcpy.mapping.MapDocument(section_mxd_name)

    # Set Layers to Reference Input FC
    broken = arcpy.mapping.ListBrokenDataSources(final_mxd)
    fc_db = os.path.split(in_fc)[0]
    fc = os.path.split(in_fc)[1]
    for item in broken:
        if item.name.startswith(r'Contour'):
            item.replaceDataSource(fc_db, "FILEGDB_WORKSPACE", fc)
    final_mxd.save()

    anno_1128 = os.path.join(final_db, r"Contours_1128Anno1128")
    anno_2257 = os.path.join(final_db, r"Contours_2257Anno2256")
    anno_4514 = os.path.join(final_db, r"Contours_4514Anno4513")
    anno_9028 = os.path.join(final_db, r"Contours_9028Anno9027")

    # Create .lyr Files for Results Contours
    annotation_set = [
        [anno_1128, "Contours_1128Anno1128.lyr", "Cont_1128Anno1128"],
        [anno_2257, "Contours_2257Anno2256.lyr", "Cont_2257Anno2256"],
        [anno_4514, "Contours_4514Anno4513.lyr", "Cont_4514Anno4513"],
        [anno_9028, "Contours_9028Anno9027.lyr", "Cont_9028Anno9027"]
    ]

    df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]

    # Create .lyr Files & Add to MXD
    for a in annotation_set:
        lyr = os.path.join(folder, a[1])
        arcpy.MakeFeatureLayer_management(a[0], a[2])
        arcpy.SaveToLayerFile_management(
            in_layer=a[2],
            out_layer=lyr,
            is_relative_path='ABSOLUTE',
            version='CURRENT'
        )
        add_lyr = arcpy.mapping.Layer(a[2])
        arcpy.mapping.AddLayer(df, add_lyr, 'BOTTOM')
    final_mxd.save()

    df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]

    # Copy Blank Symbology
    base_mask_symbology = ContourConfig.SYMBOLOGY_LAYER_PATH
    mask_sym_lyr = os.path.join(folder, ContourConfig.SYMBOLOGY_LAYER_NAME)
    shutil.copyfile(base_mask_symbology, mask_sym_lyr)
    lyr_sym_file = arcpy.mapping.Layer(mask_sym_lyr)

    arcpyproduction.mapping.EnableLayerMasking(df, 'true')

    mask_list = [
        ["Mask1128", 1129, None, "Contours 1128"],
        ["Mask2257", 2258, 1130, "Contours 2257"],
        ["Mask4514", 4515, 2259, "Contours 4514"],
        ["Mask9028", 9029, 4516, "Contours 9028"]
    ]

    # Apply Masking to Contour Layers
    for m in mask_list:
        m_lyr = arcpy.mapping.Layer(os.path.join(final_db, m[0]))
        m_lyr.minScale = m[1]
        m_lyr.maxScale = m[2]
        arcpy.mapping.AddLayer(df, m_lyr, 'BOTTOM')
        update = arcpy.mapping.ListLayers(final_mxd, m[0], df)[0]
        arcpy.mapping.UpdateLayer(df, update, lyr_sym_file, True)
        for lyr in arcpy.mapping.ListLayers(final_mxd):
            if lyr.name == m[3]:
                arcpyproduction.mapping.MaskLayer(
                    df,
                    'APPEND',
                    update,
                    lyr
                )

    # MXD Metadata
    final_mxd.relativePaths = True
    final_mxd.title = "NRCS Contour dataset with a 2 foot interval, labeled in 10 foot intervals"
    final_mxd.tags = "Contour, Elevation, Annotation"
    final_mxd.description = "This service represents NRCS contours with a 2 foot interval, generated from Lidar datasets."

    # Ensure Labels are Disabled
    for lyr in arcpy.mapping.ListLayers(final_mxd):
        if lyr.name.upper().startswith("CONTOUR"):
            lyr.showLabels = False
    final_mxd.save()


def processJob(ProjectJob, project, strUID):
#     in_cont_fc = r'C:\Users\jeff8977\Desktop\NGCE\CONTOUR\Contours.gdb\Contours_ABC'
#     scratch_path = r'C:\Users\jeff8977\Desktop\NGCE\CONTOUR\Scratch'

    project_folder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    derived = project_folder.derived
    con_folder = derived.contour_path
    contour_file_gdb_path = os.path.join(con_folder, CONTOUR_GDB_NAME)
    scratch_path = os.path.join(con_folder, 'Scratch')
    
    in_cont_fc = os.path.join(contour_file_gdb_path, CONTOUR_NAME_OCS)
    
    # Create Base Tiling Scheme for Individual Raster Selection
    base_scheme_poly = gen_base_tiling_scheme(in_cont_fc, scratch_path)

    # Collect Unique Names from Input Feature Class
    name_list = list(set([row[0] for row in arcpy.da.SearchCursor(in_cont_fc, ['name'])]))  # @UndefinedVariable

    # Run Contour Preparation for Each Unique Name Found within  Input FC
    pool = Pool(processes=cpu_count() - 2)
    pool.map(
        partial(
            contour_prep,
            in_cont_fc,
            base_scheme_poly,
            scratch_path
        ),
        name_list
    )
    pool.close()
    pool.join()

    # Merge Multiprocessing Results
    res_db, res_dir = handle_merge(scratch_path)

    # Create Final MXD
    build_results_mxd(in_cont_fc, res_db, res_dir)
    
def PrepareContoursForJob(strJobId):
    
    
    Utility.printArguments(["WMXJobID"],
                           [strJobId], "C02 PrepareContoursForPublishing")
    aa = datetime.now()
    
    ProjectJob, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable
    
    processJob(ProjectJob, project, strUID)
    
    doTime(aa, "Operation Complete: C01 Create Contours From MD")

    

if __name__ == '__main__':

    jobID = sys.argv[1]
    PrepareContoursForJob(jobID)
    
#     jobID = 4801
#     in_cont_fc = r'C:\Users\jeff8977\Desktop\NGCE\CONTOUR\Contours.gdb\Contours_ABC'
#     scratch_path = r'C:\Users\jeff8977\Desktop\NGCE\CONTOUR\Scratch'
# 
#     try:
#     except Exception as e:
#         print 'Exception: ', e
