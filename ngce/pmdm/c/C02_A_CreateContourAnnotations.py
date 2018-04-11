
'''
DANGER: 
DANGER: 
DANGER: 
DANGER: ERIC IS MODIFYING THIS FILE.
DANGER: 
DANGER: 
DANGER: 
DANGER: 
DANGER: 
'''




import arcpy
import datetime
from functools import partial
from multiprocessing import Pool, cpu_count
import os
import shutil
import sys
import time
import traceback

from ngce import Utility
from ngce.Utility import doTime
from ngce.cmdr.CMDR import ProjectJob
from ngce.cmdr.CMDRConfig import DTM
from ngce.cmdr.JobUtil import getProjectFromWMXJobID
from ngce.contour import ContourConfig
from ngce.contour.ContourConfig import CONTOUR_GDB_NAME, CONTOUR_NAME_WM
from ngce.folders import ProjectFolders
from ngce.pmdm import RunUtil
from ngce.pmdm.a import A05_C_ConsolidateRasterInfo




CPU_HANDICAP = 0  # set higher to use fewer CPUs

# @TODO: Determine if final contours are moved to Publish directory


def gen_base_tiling_scheme(base_fc, scratch):
    
    arcpy.env.overwriteOutput = True
    db, fc = os.path.split(base_fc)
    base_tiling_scheme = os.path.join(db, 'Base_Tiling_Scheme')
    if arcpy.Exists(base_tiling_scheme):
        arcpy.AddMessage("Tiling Scheme Exists: {}".format(base_tiling_scheme))
    else:
        a = datetime.datetime.now()
        # Copy Template MXD
        base_mxd = arcpy.mapping.MapDocument(ContourConfig.MXD_TEMPLATE)
        section_mxd_name = os.path.join(scratch, 'Tiling_Scheme.mxd')
        if not os.path.exists(section_mxd_name):
            base_mxd.saveACopy(section_mxd_name)
    
        # Set MXD For Processing
        first_mxd = arcpy.mapping.MapDocument(section_mxd_name)
    
        # Set Layers to Reference Input FC
        broken = arcpy.mapping.ListBrokenDataSources(first_mxd)
        for item in broken:
            if item.name.startswith(r'Contour'):
                item.replaceDataSource(db, "FILEGDB_WORKSPACE", fc)
        first_mxd.save()
    
        # Generate Tiling Scheme for Input MXD
        
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
        Utility.doTime(a, "Generated base tiling scheme {}".format(base_tiling_scheme))

        # JWS - 3/29
        del base_mxd

    return base_tiling_scheme


'''
No good way to re-create the annotation layer without doing them all.
So delete all the feature classes, layers, and masks and start over.
'''
def clearScratchFiles(section_mxd_name, anno_paths, mask_paths, annoLyr_paths):
    directory = os.path.split(section_mxd_name)[0]
    if os.path.exists(directory):
        try:
                shutil.rmtree(directory) 
        except:
            try:
                os.remove(section_mxd_name)
            except:
                pass
            
            for layer_path in annoLyr_paths:
                try:
                    os.remove(layer_path)
                except:
                    pass
                
            for anno_path in anno_paths:
                try:
                    arcpy.Delete_management(anno_path)
                except:
                    pass
                
            for mask_path in mask_paths:
                try:
                    arcpy.Delete_management(mask_path)
                except:
                    pass
            
            try:
                shutil.rmtree(directory) 
            except:
                pass
#     if not os.path.exists(directory):
#         os.makedirs(directory)
    try:
        if not os.path.exists(directory):
            os.makedirs(directory) 
    except:
        pass
    


    
    arcpy.AddMessage("\t\tCleared scratch directory {}".format(directory))

def isProcessFile(scratch, name):
    filter_folder_name = "T{}".format(name)
    filter_folder = os.path.join(scratch, filter_folder_name)
    
    section_mxd_name = os.path.join(filter_folder, '{}.mxd'.format(filter_folder_name))
    scratch_db = os.path.join(filter_folder, '{}.gdb'.format(filter_folder_name))
    target_scheme_polys = os.path.join(filter_folder, '{}SP.shp'.format(filter_folder_name))
    
    anno1128 = os.path.join(filter_folder, r"Contours_1128Anno1128.shp")
    anno2257 = os.path.join(filter_folder, r"Contours_2257Anno2256.shp")
    anno4514 = os.path.join(filter_folder, r"Contours_4514Anno4513.shp")
    anno9028 = os.path.join(filter_folder, r"Contours_9028Anno9027.shp")
    anno_paths = [anno1128, anno2257, anno4514, anno9028]
    
    mask1128 = os.path.join(filter_folder, r"Mask1128.shp")
    mask2257 = os.path.join(filter_folder, r"Mask2256.shp")
    mask4514 = os.path.join(filter_folder, r"Mask4513.shp")
    mask9028 = os.path.join(filter_folder, r"Mask9027.shp")
    mask_paths = [mask1128, mask2257, mask4514, mask9028]
    
    annoLyr1128 = os.path.join(filter_folder, r"Contours_1128Anno1128.lyr")
    annoLyr2257 = os.path.join(filter_folder, r"Contours_2257Anno2256.lyr")
    annoLyr4514 = os.path.join(filter_folder, r"Contours_4514Anno4513.lyr")
    annoLyr9028 = os.path.join(filter_folder, r"Contours_9028Anno9027.lyr")
    annoLyr_paths = [annoLyr1128, annoLyr2257, annoLyr4514, annoLyr9028]
    
    isOk = True
    
    isFolder = True
    isMxd = True
    isLyrFile = True
    isMask = True
    isAnno = True
    isTargetPoly = True
    isScratchDB = True
    
    if not os.path.exists(filter_folder):
        isFolder = False
    
    isOk = isOk and isFolder
    
    if not os.path.exists(section_mxd_name):
        isMxd = False
    
    isOk = isOk and isMxd 
    
    if isOk and not os.path.exists(scratch_db): 
        isScratchDB = False
    
    isOk = isOk and isScratchDB
        
    if isOk and not os.path.exists(target_scheme_polys):
        isTargetPoly = False 
            
    isOk = isOk and isTargetPoly
    
    if isOk:
        for anno_path in anno_paths:
            if isOk and not os.path.exists(anno_path):
                isAnno = False
            isOk = isOk and isAnno
    
    if isOk:
        for mask_path in mask_paths:
            if isOk and not os.path.exists(mask_path):
                isMask = False
            isOk = isOk and isMask
    if isOk:
        for annoLyr_path in annoLyr_paths:
            if isOk and not os.path.exists(annoLyr_path):
                isLyrFile = False
            isOk = isOk and isLyrFile
    
#     isOk = not (isMxd and isLyrFile and isMask and isAnno and isTargetPoly and isScratchDB)
    
    return not isOk
                
def getContourPrepList(scratch, name_list):
    process_list = []
    for name in name_list:
        if isProcessFile(scratch, name):
            process_list.append(name)
    
    return process_list
    

def contour_prep(in_fc, scheme_poly, scratch, footprint_path, name):
    a = datetime.datetime.now()
    aa = a
    # arcpy.AddMessage('Started: {}'.format(name))
    db = os.path.split(in_fc)[0]
    fc = os.path.split(in_fc)[1]
    
    arcpy.env.overwriteOutput = True
    
    filter_folder = os.path.join(scratch, 'T{}'.format(name))
    if not os.path.exists(filter_folder):
        os.makedirs(filter_folder)
    section_mxd_name = os.path.join(filter_folder, 'T{}.mxd'.format(name))
    scratch_db = os.path.join(filter_folder, 'T{}.gdb'.format(name))
    target_scheme_polys = os.path.join(filter_folder, 'T{}SP.shp'.format(name))
    target_scheme_polys_fgdb = os.path.join(scratch_db, 'T{}SP'.format(name))
    
#     Utility.printArguments(["in_fc", "scheme_poly", "scratch", "name", "db", "fc", "filter_folder", "section_mxd_name", "scratch_db", "target_scheme_polys"],
#                            [in_fc, scheme_poly, scratch, name, db, fc, filter_folder, section_mxd_name, scratch_db, target_scheme_polys], "C02_B Contour Prep")
    
    
    anno1128 = os.path.join(scratch_db, r"Contours_1128Anno1128")
    anno2257 = os.path.join(scratch_db, r"Contours_2257Anno2256")
    anno4514 = os.path.join(scratch_db, r"Contours_4514Anno4513")
    anno9028 = os.path.join(scratch_db, r"Contours_9028Anno9027")
    anno_paths = [anno1128, anno2257, anno4514, anno9028]
    
    annoShp1128 = os.path.join(filter_folder, r"Contours_1128Anno1128.shp")
    annoShp2257 = os.path.join(filter_folder, r"Contours_2257Anno2256.shp")
    annoShp4514 = os.path.join(filter_folder, r"Contours_4514Anno4513.shp")
    annoShp9028 = os.path.join(filter_folder, r"Contours_9028Anno9027.shp")
    annoShp_paths = [annoShp1128, annoShp2257, annoShp4514, annoShp9028]  # @UnusedVariable
    
    mask1128 = os.path.join(filter_folder, r"Mask1128.shp")
    mask2257 = os.path.join(filter_folder, r"Mask2256.shp")
    mask4514 = os.path.join(filter_folder, r"Mask4513.shp")
    mask9028 = os.path.join(filter_folder, r"Mask9027.shp")
    mask_paths = [mask1128, mask2257, mask4514, mask9028]
    
    annoLyr1128 = os.path.join(filter_folder, r"Contours_1128Anno1128.lyr")
    annoLyr2257 = os.path.join(filter_folder, r"Contours_2257Anno2256.lyr")
    annoLyr4514 = os.path.join(filter_folder, r"Contours_4514Anno4513.lyr")
    annoLyr9028 = os.path.join(filter_folder, r"Contours_9028Anno9027.lyr")
    annoLyr_paths = [annoLyr1128, annoLyr2257, annoLyr4514, annoLyr9028]
    
    clearScratch = True    
    TRIES_ALLOWED = 10
    
    if not isProcessFile(scratch, name):
        arcpy.AddMessage("{}: All artifacts exist".format(name))
    else:
        created1 = False
        tries1 = 0
        while not created1 and tries1 <= TRIES_ALLOWED:
            tries1 = tries1 + 1
            try:
                # Clear out everything that was created before, we cant trust it
                if clearScratch:
                    clearScratch = False
                    clearScratchFiles(section_mxd_name, anno_paths, mask_paths, annoLyr_paths)
                
                mxd_tries1 = 0
                while not os.path.exists(section_mxd_name) and mxd_tries1 < TRIES_ALLOWED:
                    mxd_tries1 = mxd_tries1 + 1
                    try:
                        if not os.path.exists(filter_folder):
                            os.makedirs(filter_folder)
                            arcpy.AddMessage('\tREPEAT: Made section Scratch Folder Name: {}'.format(filter_folder))
                        else:
                            arcpy.AddMessage('\tEXISTS: Section Scratch Folder Name: {}'.format(filter_folder))
                    
                        arcpy.AddMessage('\tSection MXD Name: {}'.format(section_mxd_name))
                        shutil.copyfile(ContourConfig.MXD_ANNO_TEMPLATE, section_mxd_name)
                        
                        a = Utility.doTime(a, "\t{}: Saved a copy of the mxd template to '{}'".format(name, section_mxd_name))
                        arcpy.AddMessage('\tSection MXD Name {} exists? {}'.format(section_mxd_name, os.path.exists(section_mxd_name)))
                        
                    except Exception as e:
                        time.sleep(mxd_tries1)
                        
                        arcpy.AddWarning('Copying Section MXD Failed: {}'.format(section_mxd_name))
                        arcpy.AddWarning('Error: {}'.format(e))
                        type_, value_, traceback_ = sys.exc_info()
                        tb = traceback.format_exception(type_, value_, traceback_, 3)
                        arcpy.AddWarning('Error: \n{}: {}\n{}\n'.format(type_, value_, tb[1]))
                        
                        try:
                            arcpy.AddMessage('\t\t\t: Removing folder: {}'.format(filter_folder))
                            shutil.rmtree(filter_folder)
                            arcpy.AddMessage('\t\t\t: folder{} exists? {}'.format(filter_folder, os.path.exists(filter_folder)))
                            if not os.path.exists(filter_folder):
                                os.makedirs(filter_folder)
                                arcpy.AddMessage('\tREPEAT: Made section Scratch Folder Name: {}'.format(filter_folder))
                            arcpy.AddMessage('\t\t\t: folder{} exists? {}'.format(filter_folder, os.path.exists(filter_folder)))
                        except:
                            arcpy.AddWarning('\t\t\t: folder{} exists: {}'.format(filter_folder, os.path.exists(filter_folder)))
                        
                        if mxd_tries1 >= TRIES_ALLOWED:
                            raise e
                                
                # Set MXD For Processing
                mxd = arcpy.mapping.MapDocument(section_mxd_name)
        
                # Set Layers to Reference Input FC
                broken = arcpy.mapping.ListBrokenDataSources(mxd)
                
                for item in broken:
                    if item.name.startswith(r'Contour'):
                        item.replaceDataSource(db, "FILEGDB_WORKSPACE", fc)
                
                mxd.save()
                a = Utility.doTime(a, "\t{}: Fixed broken paths in '{}'".format(name, section_mxd_name))
        
                # Create FGDB For Annotation Storage
                if arcpy.Exists(scratch_db):
                    pass
                else:
                    arcpy.CreateFileGDB_management(filter_folder, 'T{}.gdb'.format(name))
                    a = Utility.doTime(a, "\t{}: Created 'T{}.gdb' at {}".format(name, name, filter_folder))
                
                
                if arcpy.Exists(target_scheme_polys):
                    arcpy.AddMessage("\t{}: Scheme Poly exists: {}".format(name, target_scheme_polys))
                else:
                    
                    # Filter for Section of Input FC
                    feat = arcpy.MakeFeatureLayer_management(
                        in_features=footprint_path,
                        out_layer=name,
                        where_clause="name='{}'".format(name)
                    )
                    a = Utility.doTime(a, "\t{}: Created feature layer '{}'".format(name, feat))
                            
                    arcpy.Clip_analysis(in_features=scheme_poly, clip_features=feat, out_feature_class=target_scheme_polys, cluster_tolerance="")
                    if arcpy.Exists(target_scheme_polys_fgdb):
                        arcpy.Delete_management(target_scheme_polys_fgdb)
                    created = False
                    tries = 0
                    while not created and tries <= TRIES_ALLOWED:
                        tries = tries + 1
                        try:
                            arcpy.CopyFeatures_management(in_features=target_scheme_polys, out_feature_class=target_scheme_polys_fgdb)
                            a = Utility.doTime(a, "\t{}: Copied target scheme polys '{}'".format(name, target_scheme_polys))
                            created = True
                        except:
                            time.sleep(1)
        
                # Reference Annotation FCs created with TiledLabelsToAnnotation
                df = arcpy.mapping.ListDataFrames(mxd, 'Layers')[0]
                a = Utility.doTime(a, "\t{}: Got data frame '{}'".format(name, df))
                        
                for lyr in arcpy.mapping.ListLayers(mxd):
                    try:
                        lyr.showLabels = False
                        if lyr.name.upper().startswith("CONTOURS "):
                            lyr.showLabels = True
                            if lyr.supports("DEFINITIONQUERY"):
                                lyr.definitionQuery = "{} and name = '{}'".format(lyr.definitionQuery, name) 
                    except:
                        pass  # some layers don't support labels. If not, just move one
                
                a = Utility.doTime(a, "\t{}: Creating annotation from tiled labels".format(name))
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
                    tile_id_field="FID",
                    feature_linked="STANDARD",
                    generate_unplaced_annotation="NOT_GENERATE_UNPLACED_ANNOTATION"
                )
                Utility.addToolMessages()
                                
                mxd.save()
                a = Utility.doTime(a, "\t{}: Exported tiled labels to annotation '{}'".format(name, target_scheme_polys))
        
                # Create layer files for each of the Anno feature classes, and add to the map
                annotation_set = [
                    [anno1128, annoLyr1128, "Cont_1128Anno1128", annoShp1128],
                    [anno2257, annoLyr2257, "Cont_2257Anno2256", annoShp2257],
                    [anno4514, annoLyr4514, "Cont_4514Anno4513", annoShp4514],
                    [anno9028, annoLyr9028, "Cont_9028Anno9027", annoShp9028]
                ]
        
                # Create .lyr Files & Add to MXD
                df = arcpy.mapping.ListDataFrames(mxd, 'Layers')[0]
                for anno in annotation_set:
                    lyr_path = anno[1]
                    if not arcpy.Exists(anno[0]):
                        arcpy.AddWarning("{}: WARNING: Annotation Layer Missing: {}".format(name, anno[0]))
                    else:
                        if arcpy.Exists(lyr_path):
                            arcpy.AddMessage("\t{}: Annotation Layer Exists: {}".format(name, lyr_path))
                        else:
                            arcpy.MakeFeatureLayer_management(anno[0], anno[2])
                            arcpy.SaveToLayerFile_management(
                                in_layer=anno[2],
                                out_layer=lyr_path,
                                is_relative_path='ABSOLUTE',
                                version='CURRENT'
                            )
                            arcpy.AddMessage("\t{}: Annotation Layer Exported: {}".format(name, lyr_path))
                            
                        shp_path = anno[3]
                        if os.path.exists(shp_path):
                            arcpy.AddMessage("\t{}: Annotation shapefile Exported: {}".format(name, shp_path))
                        else:
                            arcpy.FeatureToPoint_management(in_features=anno[0], out_feature_class=shp_path, point_location="INSIDE")
                            arcpy.AddMessage("\t{}: Annotation shapefile Exported: {}".format(name, shp_path))
                        
                        addLayer = True
                        for cur_lyr in arcpy.mapping.ListLayers(mxd):
                            if cur_lyr.name.upper().startswith(str(anno[2]).upper()):
                                addLayer = False
                                break
                        if addLayer:
                            add_lyr = arcpy.mapping.Layer(lyr_path)
                            arcpy.mapping.AddLayer(df, add_lyr, 'BOTTOM')
                mxd.save()
                a = Utility.doTime(a, "\t{}: Exported layer files for annotation set {}".format(name, annotation_set))
                
                for lyr_path in annoLyr_paths:  # arcpy.ListFiles('Contours*.lyr_path'):
                    try:
                        ref_scale = lyr_path[-8:-4]
                        mask_fc = os.path.join(filter_folder, r'Mask{}.shp'.format(ref_scale))
                        if arcpy.Exists(mask_fc):
                            arcpy.AddMessage("\t{}: Mask Layer Exists: {}".format(name, mask_fc))
                        else:
                            if os.path.exists(lyr_path):
                                arcpy.FeatureOutlineMasks_cartography(
                                    input_layer=lyr_path,
                                    output_fc=mask_fc,
                                    reference_scale=ref_scale,
                                    spatial_reference=ContourConfig.WEB_AUX_SPHERE,
                                    margin='0 Points',
                                    method='BOX',
                                    mask_for_non_placed_anno='ALL_FEATURES',
                                    attributes='ALL'
                                )
                            else:
                                arcpy.AddWarning("t{}: WARNING: Can't create masking layer. Layer file missing {}".format(name, lyr_path))
                    except Exception as e:
                        arcpy.AddError('{}: Exception: {}'.format(name, e))
                        pass
                mxd.save()
                a = Utility.doTime(a, "\t{}: Created masking polygons".format(name))
                
                Utility.doTime(aa, 'Finished: {}'.format(name))
                created1 = True
                del mxd
                
            except Exception as e:
                arcpy.AddError('Exception: {}'.format(e))
                tb = sys.exc_info()[2]
                tbinfo = traceback.format_tb(tb)[0]
                arcpy.AddError("PYTHON ERRORS:\nTraceback info:\n{}\nError Info:\n{}".format(tbinfo, str(sys.exc_info()[1])))
                arcpy.AddError("ArcPy ERRORS:\n{}\n".format(arcpy.GetMessages(2)))
                if tries1 > TRIES_ALLOWED:
                    arcpy.AddError('Dropped: {}'.format(name))
                    raise e
    
    


def db_list_gen(scratch, dirs, names, shp=False):

    lists = [
        [],
        [],
        [],
        []
    ]

    for dir_name in dirs:
        db = os.path.join(scratch, dir_name, '{}.gdb'.format(dir_name))
        folder = os.path.join(scratch, dir_name)
        e_names = enumerate(names)
        for index, name in e_names:
            target = os.path.join(db, name)
            target_shp = os.path.join(folder, "{}.shp".format(name))
            if not shp and arcpy.Exists(target):
                lists[index].append(target)
            elif shp and os.path.exists(target_shp):
                lists[index].append(target_shp)
            
    return lists


def run_merge(lists, results):

    arcpy.env.overwriteOutput = True
    a = datetime.datetime.now()
    # Merge Each Set of Inputs
    for x in lists:
        output_name = os.path.split(x[0])[1]
        merge_fc_path = os.path.join(results, output_name)
        if arcpy.Exists(merge_fc_path):
            arcpy.AddMessage("Merged exists: {}".format(merge_fc_path))
        else:
            arcpy.Merge_management(x, merge_fc_path)
            a = Utility.doTime(a, "Merged {}".format(merge_fc_path))

def handle_merge(scratch):
    a = datetime.datetime.now()
    aa = a
    arcpy.AddMessage('Merging Multiprocessing Results')

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
    a = Utility.doTime(aa, "Generated dir List of length {}".format(len(dirs)))
    
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
        'Mask2256',
        'Mask4513',
        'Mask9027'
    ]

    # Generate DB Merge Lists
    m_list = db_list_gen(scratch, dirs, m_names, True)
    a = Utility.doTime(aa, "Generated mask List [[{}], [{}], [{}], [{}]]".format(len(m_list[0]), len(m_list[1]), len(m_list[2]), len(m_list[3])))
    a_list = db_list_gen(scratch, dirs, a_names)
    a = Utility.doTime(aa, "Generated anno List [[{}], [{}], [{}], [{}]]".format(len(a_list[0]), len(a_list[1]), len(a_list[2]), len(a_list[3])))

    # Merge Features
    run_merge(a_list, results)
    run_merge(m_list, results_folder)
    
    Utility.doTime(aa, "Finished merging results")
    return results, results_folder

    
def buildAnnotations(scratch_path, in_cont_fc, base_scheme_poly, name_list, footprint_path, runAgain=True):
    a = datetime.datetime.now()
    updated_name_list = getContourPrepList(scratch_path, name_list)
    if len(updated_name_list) <= 0:
        arcpy.AddMessage("All tile artifacts up to date")
    else:
        # Run Contour Preparation for Each Unique Name Found within  Input FC
        pool = Pool(processes=cpu_count() - CPU_HANDICAP)
        pool.map(partial(contour_prep, in_cont_fc, base_scheme_poly, scratch_path, footprint_path), name_list)
        pool.close()
        pool.join()
        
        # sometimes things fail for no reason, so try again
        updated_name_list = getContourPrepList(scratch_path, name_list)
        if len(updated_name_list) > 0:
            if runAgain:
                arcpy.AddWarning("WARNING: Building annotations again.")
                buildAnnotations(scratch_path, in_cont_fc, base_scheme_poly, name_list, footprint_path, False)
            
    
    
    Utility.doTime(a, "Finished building annotations")
    
        
        
def build_results_mxd(in_fc, final_db, folder):
    a = datetime.datetime.now()
    aa = a
    arcpy.AddMessage('Create Results MXD')

    arcpy.env.overwriteOutput = True

    # Copy Template MXD
    section_mxd_name = os.path.join(folder, 'Results.mxd')
    if os.path.exists(section_mxd_name):
        arcpy.Delete_management(section_mxd_name)
    base_mxd = arcpy.mapping.MapDocument(ContourConfig.MXD_TEMPLATE)
    base_mxd.saveACopy(section_mxd_name)

    # MXD For final processing and set Metadata
    final_mxd = arcpy.mapping.MapDocument(section_mxd_name)
    final_mxd.relativePaths = True
    final_mxd.title = "NRCS Contour dataset with a 2 foot interval, labeled in 10 foot intervals"
    final_mxd.tags = "Contour, Elevation, Annotation"
    final_mxd.description = "This service represents NRCS contours with a 2 foot interval, generated from Lidar datasets."
    a = Utility.doTime(a, "Updated mxd at {}".format(section_mxd_name))
    
    # Set Layers to Reference Input FC
    broken = arcpy.mapping.ListBrokenDataSources(final_mxd)
    fc_db = os.path.split(in_fc)[0]
    fc = os.path.split(in_fc)[1]
    for item in broken:
        if item.name.startswith(r'Contour'):
            item.replaceDataSource(fc_db, "FILEGDB_WORKSPACE", fc)
    final_mxd.save()
    a = Utility.doTime(a, "Fixed broken paths on {}".format(section_mxd_name))

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

    

    # Create .lyr Files & Add to MXD
    for anno in annotation_set:
        lyr = os.path.join(folder, anno[1])
        if os.path.exists(lyr):
            arcpy.AddMessage("Layer file exists: {}".format(lyr))
        else:
            arcpy.MakeFeatureLayer_management(anno[0], anno[2])
            arcpy.SaveToLayerFile_management(
                in_layer=anno[2],
                out_layer=lyr,
                is_relative_path='ABSOLUTE',
                version='CURRENT'
            )
        add_lyr = arcpy.mapping.Layer(lyr)
        df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
        arcpy.mapping.AddLayer(df, add_lyr, 'BOTTOM')
    final_mxd.save()
    a = Utility.doTime(a, "Created and added layer files {}".format(section_mxd_name))
    

    # Copy Blank Symbology
    base_mask_symbology = ContourConfig.SYMBOLOGY_LAYER_PATH
    mask_sym_lyr = os.path.join(folder, ContourConfig.SYMBOLOGY_LAYER_NAME)
    if os.path.exists(mask_sym_lyr):
        arcpy.AddMessage("Mask lyr exists: {}".format(mask_sym_lyr))
    else:
        shutil.copyfile(base_mask_symbology, mask_sym_lyr)
    lyr_sym_file = arcpy.mapping.Layer(mask_sym_lyr)

    df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
#     arcpyproduction.mapping.EnableLayerMasking(df, 'true')
    final_mxd.save()
    a = Utility.doTime(a, "Enabled masking on {}".format(section_mxd_name))

    mask_list = [
        ["Mask1128", 1129, None, "Contours 1128"],
        ["Mask2256", 2258, 1130, "Contours 2257"],
        ["Mask4513", 4515, 2259, "Contours 4514"],
        ["Mask9027", 9029, 4516, "Contours 9028"]
    ]

    # Apply Masking to Contour Layers
    for m in mask_list:
        m_path = os.path.join(folder, "{}.shp".format(m[0]))
        arcpy.AddMessage("Adding masking layer {}".format(m_path))
        m_lyr = arcpy.mapping.Layer(m_path)
        m_lyr.minScale = m[1]
        m_lyr.maxScale = m[2]
        df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
        arcpy.mapping.AddLayer(df, m_lyr, 'BOTTOM')
        df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
        update = arcpy.mapping.ListLayers(final_mxd, m[0], df)[0]
        df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
        arcpy.mapping.UpdateLayer(df, update, lyr_sym_file, True)
        for lyr in arcpy.mapping.ListLayers(final_mxd):
            if lyr.name == m[3]:
                df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
#                 arcpyproduction.mapping.MaskLayer(
#                     df,
#                     'APPEND',
#                     update,
#                     lyr
#                 )
                
                a = Utility.doTime(a, "\tEnabled masking on {}".format(lyr))
                break
            
    final_mxd.save()
    a = datetime.datetime.now()
    # Ensure Labels are Disabled
    for lyr in arcpy.mapping.ListLayers(final_mxd):
        if lyr.name.upper().startswith("CONTOUR"):
            lyr.showLabels = False
    final_mxd.save()
    a = Utility.doTime(a, "Updated label props on {}".format(section_mxd_name))
    
    Utility.doTime(aa, "Finished Annotation Results for {}".format(section_mxd_name))

    # JWS - 3/29
    del base_mxd

def processJob(project_job, project, strUID):
    a = datetime.datetime.now()
    aa = a
#     in_cont_fc = r'C:\Users\jeff8977\Desktop\NGCE\CONTOUR\Contours.gdb\Contours_ABC'
#     scratch_path = r'C:\Users\jeff8977\Desktop\NGCE\CONTOUR\Scratch'

    project_folder = ProjectFolders.getProjectFolderFromDBRow(project_job, project)
    
    derived = project_folder.derived
    project_fgdb_path = derived.fgdb_path
    con_folder = derived.contour_path
    contour_file_gdb_path = os.path.join(con_folder, CONTOUR_GDB_NAME)
    footprint_path = A05_C_ConsolidateRasterInfo.getRasterFootprintPath(fgdb_path=project_fgdb_path, elev_type=DTM)
    
    # Set up the scratch directory
    scratch_path = os.path.join(con_folder, 'C02Scratch')
    if not os.path.exists(scratch_path):
        os.makedirs(scratch_path)
    
    in_cont_fc = os.path.join(contour_file_gdb_path, CONTOUR_NAME_WM)
    a = Utility.doTime(a, "Set up for run")
    
    # Create Base Tiling Scheme for Individual Raster Selection
    base_scheme_poly = gen_base_tiling_scheme(in_cont_fc, scratch_path)
    a = Utility.doTime(a, "Generated tiling scheme")

    # Collect Unique Names from Input Feature Class
    name_list_len = -1
    name_list = list(set([row[0] for row in arcpy.da.SearchCursor(footprint_path, ['name'])]))  # @UndefinedVariable
    try:
        name_list_len = len(name_list)
    except:
        pass
    a = Utility.doTime(a, "Retrieved name list of size {}".format(name_list_len))

    buildAnnotations(scratch_path, in_cont_fc, base_scheme_poly, name_list, footprint_path, False)
    a = Utility.doTime(a, "Built annotations")
    updated_name_list = getContourPrepList(scratch_path, name_list)
    
    if len(updated_name_list) > 0:
        arcpy.AddWarning("Failed to build artifacts for {} tiles".format(len(updated_name_list)))
        for fail in updated_name_list:
            arcpy.AddWarning("\t{}: Failed".format(fail))
        # raise Exception("Failed to build artifacts for {} tiles".format(len(updated_name_list)))
    
    # Merge Multiprocessing Results
    res_db, res_dir = handle_merge(scratch_path)
    a = Utility.doTime(a, "Merged results")

    # Create Final MXD
    build_results_mxd(in_cont_fc, res_db, res_dir)
    RunUtil.runTool(r'ngce\pmdm\C\C02_B_PrepContForPub.py', [in_cont_fc, res_db, res_dir], bit32=True, log_path=project_folder.derived.path)
    
    #     build_results_mxd(in_cont_fc, res_db, res_dir)
    a = Utility.doTime(aa, "Processed Job")
    
def PrepareContoursForJob(strJobId):
    Utility.printArguments(["WMXJobID"],
                           [strJobId], "C02 PrepareContoursForPublishing")
    aa = datetime.datetime.now()

    project_job, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable
    
    processJob(project_job, project, strUID)
    
    doTime(aa, "Operation Complete: C01 Create Contours From MD")

    

def setupForDebug():
    UID = None  # field_ProjectJob_UID
    wmx_job_id = 1
    project_Id = "OK_SugarCreek_2008"
    alias = "Sugar Creek"
    alias_clean = "SugarCreek"
    state = "OK"
    year = 2008
    parent_dir = r"E:\NGCE\RasterDatasets"
    archive_dir = r"E:\NGCE\RasterDatasets"
    project_dir = r"E:\NGCE\RasterDatasets\OK_SugarCreek_2008"
    project_AOI = None
    project_job = ProjectJob()
    project = [
               UID,  # field_ProjectJob_UID
               wmx_job_id,  # field_ProjectJob_WMXJobID,
               project_Id,  # field_ProjectJob_ProjID,
               alias,  # field_ProjectJob_Alias
               alias_clean,  # field_ProjectJob_AliasClean
               state ,  # field_ProjectJob_State
               year ,  # field_ProjectJob_Year
               parent_dir,  # field_ProjectJob_ParentDir
               archive_dir,  # field_ProjectJob_ArchDir
               project_dir,  # field_ProjectJob_ProjDir
               project_AOI  # field_ProjectJob_SHAPE
               ]
          
    return project_job, project, UID

if __name__ == '__main__':
    exception = None

    arcpy.CheckOutExtension("Foundation")
    
    if len(sys.argv) > 1:
        jobID = sys.argv[1]
        PrepareContoursForJob(jobID)
    else:
        project_job, project, strUID = setupForDebug()
        processJob(project_job, project, strUID)
    
    arcpy.CheckInExtension("Foundation")
        
