
import arcpy
import arcpyproduction  # @UnresolvedImport
import datetime
import os
import sys

from ngce import Utility


def build_results_mxd(in_fc, final_db, folder):
    a = datetime.datetime.now()
    aa = a
    arcpy.AddMessage('Starting Masking Annotations in MXD')

    # arcpy.env.overwriteOutput = True

#     # Copy Template MXD
    section_mxd_name = os.path.join(folder, 'Results.mxd')
#     if os.path.exists(section_mxd_name):
#         arcpy.Delete_management(section_mxd_name)
#     base_mxd = arcpy.mapping.MapDocument(ContourConfig.MXD_TEMPLATE)
#     base_mxd.saveACopy(section_mxd_name)

    # MXD For final processing and set Metadata
    final_mxd = arcpy.mapping.MapDocument(section_mxd_name)
#     final_mxd.relativePaths = True
#     final_mxd.title = "NRCS Contour dataset with a 2 foot interval, labeled in 10 foot intervals"
#     final_mxd.tags = "Contour, Elevation, Annotation"
#     final_mxd.description = "This service represents NRCS contours with a 2 foot interval, generated from Lidar datasets."
    a = Utility.doTime(a, "Results mxd at {}".format(section_mxd_name))
    
#     # Set Layers to Reference Input FC
#     broken = arcpy.mapping.ListBrokenDataSources(final_mxd)
#     fc_db = os.path.split(in_fc)[0]
#     fc = os.path.split(in_fc)[1]
#     for item in broken:
#         if item.name.startswith(r'Contour'):
#             item.replaceDataSource(fc_db, "FILEGDB_WORKSPACE", fc)
#     final_mxd.save()
#     a = Utility.doTime(a, "Fixed broken paths on {}".format(section_mxd_name))

#     anno_1128 = os.path.join(final_db, r"Contours_1128Anno1128")
#     anno_2257 = os.path.join(final_db, r"Contours_2257Anno2256")
#     anno_4514 = os.path.join(final_db, r"Contours_4514Anno4513")
#     anno_9028 = os.path.join(final_db, r"Contours_9028Anno9027")
# 
#     # Create .lyr Files for Results Contours
#     annotation_set = [
#         [anno_1128, "Contours_1128Anno1128.lyr", "Cont_1128Anno1128"],
#         [anno_2257, "Contours_2257Anno2256.lyr", "Cont_2257Anno2256"],
#         [anno_4514, "Contours_4514Anno4513.lyr", "Cont_4514Anno4513"],
#         [anno_9028, "Contours_9028Anno9027.lyr", "Cont_9028Anno9027"]
#     ]

    

#     # Create .lyr Files & Add to MXD
#     for anno in annotation_set:
#         lyr = os.path.join(folder, anno[1])
#         if os.path.exists(lyr):
#             arcpy.AddMessage("Layer file exists: {}".format(lyr))
#         else:
#             arcpy.MakeFeatureLayer_management(anno[0], anno[2])
#             arcpy.SaveToLayerFile_management(
#                 in_layer=anno[2],
#                 out_layer=lyr,
#                 is_relative_path='ABSOLUTE',
#                 version='CURRENT'
#             )
#         add_lyr = arcpy.mapping.Layer(lyr)
#         df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
#         arcpy.mapping.AddLayer(df, add_lyr, 'BOTTOM')
#     final_mxd.save()
#     a = Utility.doTime(a, "Created and added layer files {}".format(section_mxd_name))
#     
# 
#     # Copy Blank Symbology
#     base_mask_symbology = ContourConfig.SYMBOLOGY_LAYER_PATH
#     mask_sym_lyr = os.path.join(folder, ContourConfig.SYMBOLOGY_LAYER_NAME)
#     if os.path.exists(mask_sym_lyr):
#         arcpy.AddMessage("Mask lyr exists: {}".format(mask_sym_lyr))
#     else:
#         shutil.copyfile(base_mask_symbology, mask_sym_lyr)
#     lyr_sym_file = arcpy.mapping.Layer(mask_sym_lyr)

    df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
    arcpyproduction.mapping.EnableLayerMasking(df, 'true')
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
#         m_lyr = arcpy.mapping.Layer(os.path.join(folder, "".format(m[0])))
#         m_lyr.minScale = m[1]
#         m_lyr.maxScale = m[2]
#         df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
#         arcpy.mapping.AddLayer(df, m_lyr, 'BOTTOM')
        df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
        update = arcpy.mapping.ListLayers(final_mxd, m[0], df)[0]
#         df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
#         arcpy.mapping.UpdateLayer(df, update, lyr_sym_file, True)
        for lyr in arcpy.mapping.ListLayers(final_mxd):
            if lyr.name == m[3]:
                df = arcpy.mapping.ListDataFrames(final_mxd, 'Layers')[0]
                arcpyproduction.mapping.MaskLayer(
                    df,
                    'APPEND',
                    update,
                    lyr
                )
                
                a = Utility.doTime(a, "\tEnabled masking on {}".format(lyr))
    final_mxd.save()
    a = datetime.datetime.now()
#     # Ensure Labels are Disabled
#     for lyr in arcpy.mapping.ListLayers(final_mxd):
#         if lyr.name.upper().startswith("CONTOUR"):
#             lyr.showLabels = False
#     final_mxd.save()
#     a = Utility.doTime(a, "Updated label props on {}".format(section_mxd_name))
    
    Utility.doTime(aa, "Finished Masking Annotations for {}".format(section_mxd_name))
    
def setupForDebug():
    in_cont_fc = "E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DERIVED\CONTOUR\Contours.gdb\Contours_WM"
    res_db = "E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DERIVED\CONTOUR\C02Scratch\RESULTS.gdb"
    res_dir = "E:\NGCE\RasterDatasets\OK_SugarCreek_2008\DERIVED\CONTOUR\C02Scratch\RESULTS"
    return in_cont_fc, res_db, res_dir

if __name__ == '__main__':

    arcpy.CheckOutExtension("Foundation")
    
    in_cont_fc, res_db, res_dir = None, None, None
    if len(sys.argv) > 3:
        in_cont_fc = sys.argv[1] 
        res_db = sys.argv[2]
        res_dir = sys.argv[3]
        
    else:
        in_cont_fc, res_db, res_dir = setupForDebug()
    
    build_results_mxd(in_cont_fc, res_db, res_dir)
    
    arcpy.CheckInExtension("Foundation")
