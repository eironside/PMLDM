#-------------------------------------------------------------------------------
# Name:        gp_NRCS_PrepareContoursForPublishing
#
# Purpose:     To create a map document with the applicable Contour, Anno, and Mask
#                layers to use as a basis for creating a cache of the contours.
#                This GP tool requires a Production Mapping license to enable masking
#                  of the Annotation
#
#                Note: This tool requires an accomanpanying MaskSymbology.lyr in
#                        the same directory to assign symbology to the Mask layers
#                        (no fill and no outline).
#                        This tool also requires a template ArcMap document as input.
#                        The existing layers in the template are all broken contour
#                        layers (one for each of four scale levels). These broken layers
#                        will be repaired with the specified contour feature class.
#
#                
#
# Author:         Roslyn Dunn
# Organization: Esri Inc.
#
# Created:     07/02/2015
#
# Last Edited:
# *
#-------------------------------------------------------------------------------
import arcpy
import os
import shutil

import production  # @UnresolvedImport
from ngce import Utility
from ngce.cmdr import CMDR
from ngce.contour import ContourConfig
from ngce.folders import ProjectFolders


def PrepareContoursForPublishing(jobID):
    mxdTemplate = ContourConfig.MXD_TEMPLATE
    Utility.printArguments(["WMX Job ID", "ContourMXDTemplate"],
                           [jobID, mxdTemplate], "C02 PrepareContoursForPublishing")
    
    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
    
    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)  # @UnusedVariable
    
    
    if project is not None:
        ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
        projectID = ProjectJob.getProjectID(project)
        
#         ContourFolder = ProjectFolder.derived.contour_path
        PublishFolder = ProjectFolder.published.path
        contourMerged_Name = (ContourConfig.MERGED_FGDB_NAME).format(projectID)
        contourMerged_file_gdb_path = os.path.join(PublishFolder, contourMerged_Name)
        
        ContourFC = os.path.join(contourMerged_file_gdb_path, ContourConfig.CONTOUR_FC_WEBMERC)
                                 
        ContourBoundFC = os.path.join(contourMerged_file_gdb_path, ContourConfig.CONTOUR_BOUND_FC_WEBMERC)
    
#         ProjectID = ProjectJob.getProjectID(project)
#         ContourFolder = ProjectFolder.derived.contour_path
#         rasterFolder = ProjectFolder.published.demFirstTiff_path
    
    
    
    
        arcpy.env.overwriteOutput = True

        newMxdName = ContourConfig.CONTOUR_MXD_NAME
        
        # check out Production Mapping
        productionMappingAvailable = 0
        if arcpy.CheckExtension("Foundation") == "Available":
            arcpy.CheckOutExtension("Foundation")
            productionMappingAvailable = 1
        
        if productionMappingAvailable == 1:
            arcpy.AddMessage("\nProduction Mapping license found and checked out...")
     
        arcpy.env.overwriteOutput = True

        # Get the extent of the Contour Feature Class   
        descFC = arcpy.Describe(ContourFC)
        arcpy.env.extent = ContourFC
        arcpy.AddMessage("\nContourFC extent before: {0},{1},{2},{3}".format(descFC.extent.XMin, descFC.extent.YMin, descFC.extent.XMax, descFC.extent.YMax))

        loc = ContourFC.rfind(".gdb")
        ContourFGDB = ContourFC[:loc + 4]
        arcpy.AddMessage("Contour Feature Class FGDB:  {0}".format(ContourFGDB))

        ContourFCName = ContourFC[loc + 5:]
        arcpy.AddMessage("Contour Feature Class name:  {0}".format(ContourFCName))

        # The folder that contains the File GDB that contains the Contour feature class
        ContourFGDBPath = os.path.dirname(ContourFGDB)
        arcpy.AddMessage("Contour File FGDB folder location:  {0}".format(ContourFGDBPath))

        # open the Map template mxd 
        mxdTemp = arcpy.mapping.MapDocument(mxdTemplate)

        # Name of target mxd (to save the Template map document to)
        newMxd = os.path.join(ContourFGDBPath, newMxdName)
        
        # But don't overwrite an existing mxd
        if os.path.isfile(newMxd):
            targetMxd = arcpy.CreateUniqueName(newMxdName, ContourFGDBPath)
            arcpy.AddMessage("Renaming existing contour map document {} to {}".format(newMxd, targetMxd))
            os.rename(newMxd, targetMxd)
        # else:
        targetMxd = newMxd

        # Create a copy of the input mxd template to avoid writing to the template
        arcpy.AddMessage("\nSaving Template mxd to {0}".format(targetMxd))
        mxdTemp.saveACopy(targetMxd)

        del mxdTemp

        # open the Project-specific mxd
        mxd = arcpy.mapping.MapDocument(targetMxd)
        arcpy.AddMessage("Open target mxd:  {0}".format(targetMxd))

        # the existing layers in the template are all broken contour layers (one
        #   for each of four scale levels). Fix the data source with the Contour FC
        #   specified as a parameter
        arcpy.AddMessage("Fix broken data sources for mxd:  {0}".format(targetMxd))
        brknList = arcpy.mapping.ListBrokenDataSources(mxd)

        for brknItem in brknList:
            # arcpy.AddMessage("\nBroken Layer Name: {0}".format(brknItem.name))
            if brknItem.name.startswith(r"Contour"):
                arcpy.AddMessage("Replacing data source for:  {0}".format(brknItem.name))
                brknItem.replaceDataSource(ContourFGDB, "FILEGDB_WORKSPACE", ContourFCName)

        mxd.save()

        # Create a file of polygons to represent the location of each cache tile
        # This is useful, since anno can be created at each of the tiles to avoid
        # cutting off anno at the border of a tile
        tilingSchemeFC = os.path.join(ContourFGDB, r"cacheTilingScheme")
        
        arcpy.MapServerCacheTilingSchemeToPolygons_cartography(map_document=targetMxd,
                        data_frame="Layers", tiling_scheme=ContourConfig.TILING_SCHEME,
                        output_feature_class=tilingSchemeFC, use_map_extent="USE_MAP_EXTENT",
                        clip_to_horizon="CLIP_TO_HORIZON", antialiasing="ANTIALIASING",
                        levels="9027.977411;4513.988705;2256.994353;1128.497176")
        messages = arcpy.GetMessages()
        arcpy.AddMessage("Results output from MapServerCacheTilingSchemeToPolygons are: \n{0}\n".format(messages))

        mxd.save()
        
        # Enable Labels for Contour layers
        # This is needed for TiledLabelsToAnnotation gp tool
        for lyr in arcpy.mapping.ListLayers(mxd):
            # arcpy.AddMessage("\nLayer name is: {0}".format(lyr.name))
            if lyr.name.upper().startswith("CONTOUR"):
                lyr.showLabels = True

        # Select those tiles that intersect with the contour features to avoid errors
        # in the generation of Annotation
        arcpy.MakeFeatureLayer_management(in_features=tilingSchemeFC, out_layer="cacheTilingScheme_Layer",
                  where_clause="", workspace="", field_info="OID OID VISIBLE NONE;Shape Shape VISIBLE NONE;Tile_Scale Tile_Scale VISIBLE NONE;Tile_Level Tile_Level VISIBLE NONE;Tile_Row Tile_Row VISIBLE NONE;Tile_Col Tile_Col VISIBLE NONE;Shape_Length Shape_Length VISIBLE NONE;Shape_Area Shape_Area VISIBLE NONE")

        # If the boundary of the contours was specified, then the boundary will be checked for intersection
        # with the tiles (as opposed to the contours themselves). This will speed up the process.
        if len(ContourBoundFC) > 0:
            inputFC = ContourBoundFC
        else:
            inputFC = ContourFC

        arcpy.SelectLayerByLocation_management(in_layer="cacheTilingScheme_Layer", overlap_type="INTERSECT", select_features=inputFC,
                  search_distance="", selection_type="NEW_SELECTION", invert_spatial_relationship="NOT_INVERT")

        # Save the selection set to a file
        tilingSchemeSubsetFC = os.path.join(ContourFGDB, r"cacheTilingSchemeSubset")
        arcpy.CopyFeatures_management(in_features="cacheTilingScheme_Layer",
                  out_feature_class=tilingSchemeSubsetFC, config_keyword="", spatial_grid_1="0",
                  spatial_grid_2="0", spatial_grid_3="0")

        # The names of the GDB Anno feature classes to be created
        anno1128 = os.path.join(ContourFGDB, r"Contours_1128Anno1128")
        anno2257 = os.path.join(ContourFGDB, r"Contours_2257Anno2256")
        anno4514 = os.path.join(ContourFGDB, r"Contours_4514Anno4513")
        anno9028 = os.path.join(ContourFGDB, r"Contours_9028Anno9027")
        arcpy.AddMessage("Contours_1128Anno1128 Feature Class:  {0}".format(anno1128))
        arcpy.AddMessage("Contours_2257Anno2256 Feature Class:  {0}".format(anno2257))
        arcpy.AddMessage("Contours_4514Anno4513 Feature Class:  {0}".format(anno4514))
        arcpy.AddMessage("Contours_9028Anno9027 Feature Class:  {0}".format(anno9028))

        # Delete any existing Anno feature classes to avoid confusion
        # This is done because the TiledLabelsToAnnotation tool does NOT
        # overwrite the annotation (instead it appends "_n" to the name
        arcpy.Delete_management(in_data=anno1128, data_type="FeatureClass")
        arcpy.Delete_management(in_data=anno2257, data_type="FeatureClass")
        arcpy.Delete_management(in_data=anno4514, data_type="FeatureClass")
        arcpy.Delete_management(in_data=anno9028, data_type="FeatureClass")

        # Create annotations using the subset of tiles (that intersect with contours)
        arcpy.TiledLabelsToAnnotation_cartography(map_document=targetMxd, data_frame="Layers",
                        polygon_index_layer=tilingSchemeSubsetFC, out_geodatabase=ContourFGDB,
                        out_layer="GroupAnno", anno_suffix="Anno", reference_scale_value="9028",
                        reference_scale_field="Tile_Scale", tile_id_field="OBJECTID",
                        coordinate_sys_field="", map_rotation_field="", feature_linked="STANDARD",
                        generate_unplaced_annotation="NOT_GENERATE_UNPLACED_ANNOTATION")
        
        messages = arcpy.GetMessages()
        arcpy.AddMessage("Results output from TiledLabelsToAnnotation are: \n{0}\n".format(messages))

        mxd.save()

        # Disable Labels for Contour layers
        # We don't need labels now that gdb anno has been created
        for lyr in arcpy.mapping.ListLayers(mxd):
            # arcpy.AddMessage("\nLayer name is: {0}".format(lyr.name))
            if lyr.name.upper().startswith("CONTOUR"):
                lyr.showLabels = False
                
        mxd.save()
        
        df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]


        # Create layer files for each of the Anno feature classes, and add to the map
        
        anno1128Lyr = os.path.join(ContourFGDBPath, r"Contours_1128Anno1128.lyr")
        arcpy.AddMessage("anno1123lyr name:  {0}".format(anno1128Lyr))
        arcpy.MakeFeatureLayer_management(anno1128, "Cont_1128Anno1128")
        arcpy.SaveToLayerFile_management(in_layer="Cont_1128Anno1128", out_layer=anno1128Lyr, is_relative_path="ABSOLUTE", version="CURRENT")
        # annoLayer = arcpy.mapping.Layer(anno1128Lyr)
        annoLayer1128 = arcpy.mapping.Layer("Cont_1128Anno1128")
        arcpy.mapping.AddLayer(df, annoLayer1128, "BOTTOM")

        anno2257Lyr = os.path.join(ContourFGDBPath, r"Contours_2257Anno2256.lyr")
        arcpy.AddMessage("anno2257lyr name:  {0}".format(anno2257Lyr))
        arcpy.MakeFeatureLayer_management(anno2257, "Cont_2257Anno2256")
        arcpy.SaveToLayerFile_management(in_layer="Cont_2257Anno2256", out_layer=anno2257Lyr, is_relative_path="ABSOLUTE", version="CURRENT")
        # annoLayer = arcpy.mapping.Layer(anno2257Lyr)
        annoLayer2257 = arcpy.mapping.Layer("Cont_2257Anno2256")
        arcpy.mapping.AddLayer(df, annoLayer2257, "BOTTOM")

        anno4514Lyr = os.path.join(ContourFGDBPath, r"Contours_4514Anno4513.lyr")
        arcpy.AddMessage("anno4514lyr name:  {0}".format(anno4514Lyr))
        arcpy.MakeFeatureLayer_management(anno4514, "Cont_4514Anno4513")
        arcpy.SaveToLayerFile_management(in_layer="Cont_4514Anno4513", out_layer=anno4514Lyr, is_relative_path="ABSOLUTE", version="CURRENT")
        # annoLayer = arcpy.mapping.Layer(anno4514Lyr)
        annoLayer4514 = arcpy.mapping.Layer("Cont_4514Anno4513")
        arcpy.mapping.AddLayer(df, annoLayer4514, "BOTTOM")

        anno9028Lyr = os.path.join(ContourFGDBPath, r"Contours_9028Anno9027.lyr")
        arcpy.AddMessage("anno9028lyr name:  {0}".format(anno9028Lyr))
        arcpy.MakeFeatureLayer_management(anno9028, "Cont_9028Anno9027")
        arcpy.SaveToLayerFile_management(in_layer="Cont_9028Anno9027", out_layer=anno9028Lyr, is_relative_path="ABSOLUTE", version="CURRENT")
        # annoLayer = arcpy.mapping.Layer(anno9028Lyr)
        annoLayer9028 = arcpy.mapping.Layer("Cont_9028Anno9027")
        arcpy.mapping.AddLayer(df, annoLayer9028, "BOTTOM")

        mxd.save()

        # For each of the Anno-related layers, create a Mask feature class to mask out
        # the area under the Annotation (so the contour layer doesn't run through the Anno)
        arcpy.env.workspace = ContourFGDBPath
        
        for lyrfile in arcpy.ListFiles("Contours*.lyr"):
            # Generate Anno Masks
            lyrpath = os.path.join(ContourFGDBPath, lyrfile)
            arcpy.AddMessage("\n Full layer path used as input to FeatureOutlineMasks: {0}".format(lyrpath))
            refScale = lyrfile[9:13]
            # arcpy.AddMessage("Reference scale for this layer: {0}".format(refScale))
            maskFC = os.path.join(ContourFGDB, r"Mask" + refScale)
            arcpy.AddMessage("Mask feature class name is: {0}".format(maskFC))
            
            arcpy.FeatureOutlineMasks_cartography(input_layer=lyrpath, output_fc=maskFC, reference_scale=refScale,
                spatial_reference="PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]];-11557726.487 5143156.3073 1909376159694.14;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision",
                margin="2 Points", method="CONVEX_HULL", mask_for_non_placed_anno="ALL_FEATURES", attributes="ONLY_FID")
            messages = arcpy.GetMessages()
            arcpy.AddMessage("Results output from FeatureOutlineMasks are: \n{0}\n".format(messages))
            
        mxd.save()

        df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]

        mask1128FC = os.path.join(ContourFGDB, r"Mask1128")
        mask2257FC = os.path.join(ContourFGDB, r"Mask2257")
        mask4514FC = os.path.join(ContourFGDB, r"Mask4514")
        mask9028FC = os.path.join(ContourFGDB, r"Mask9028")

        # Copy the MaskSymbology.lyr file into the working directory to apply symbology to all mask layers
        # This is needed because Mask layers shouldn't have any fill area or outline
        originalMaskSymbologyLayerPath = ContourConfig.SYMBOLOGY_LAYER_PATH
        maskSymbologyLyrName = os.path.join(ContourFGDBPath, ContourConfig.SYMBOLOGY_LAYER_NAME)
        shutil.copyfile(originalMaskSymbologyLayerPath, maskSymbologyLyrName)

        lyrSymFile = arcpy.mapping.Layer(maskSymbologyLyrName)
        # arcpy.AddMessage("\nExisting Symbology file type: {0}".format(lyrSymFile.symbologyType))
        
        # add the mask layer to the map
        maskLayer1128 = arcpy.mapping.Layer(mask1128FC)
        maskLayer1128.minScale = 1129
        arcpy.mapping.AddLayer(df, maskLayer1128, "BOTTOM")
        # change it's symbology to be invisible (no outline or fill)
        lyr1128 = arcpy.mapping.ListLayers(mxd, "Mask1128", df)[0]
        # arcpy.AddMessage("\nlyr1128 Symbology type: {0}".format(lyr1128.symbologyType))
        arcpy.mapping.UpdateLayer(df, lyr1128, lyrSymFile, True)

        # add the mask layer to the map
        maskLayer2257 = arcpy.mapping.Layer(mask2257FC)
        maskLayer2257.minScale = 2258
        maskLayer2257.maxScale = 1130
        arcpy.mapping.AddLayer(df, maskLayer2257, "BOTTOM")
        # change it's symbology to be invisible (no outline or fill)
        lyr2257 = arcpy.mapping.ListLayers(mxd, "Mask2257", df)[0]
        # arcpy.AddMessage("\nlyr2257 Symbology type: {0}".format(lyr2257.symbologyType))
        arcpy.mapping.UpdateLayer(df, lyr2257, lyrSymFile, True)
        
        # add the mask layer to the map
        maskLayer4514 = arcpy.mapping.Layer(mask4514FC)
        maskLayer4514.minScale = 4515
        maskLayer4514.maxScale = 2259
        arcpy.mapping.AddLayer(df, maskLayer4514, "BOTTOM")
        # change it's symbology to be invisible (no outline or fill)
        lyr4514 = arcpy.mapping.ListLayers(mxd, "Mask4514", df)[0]
        # arcpy.AddMessage("\nlyr4514 Symbology type: {0}".format(lyr4514.symbologyType))
        arcpy.mapping.UpdateLayer(df, lyr4514, lyrSymFile, True)
        
        # add the mask layer to the map
        maskLayer9028 = arcpy.mapping.Layer(mask9028FC)
        maskLayer9028.minScale = 9029
        maskLayer9028.maxScale = 4516
        arcpy.mapping.AddLayer(df, maskLayer9028, "BOTTOM")
        # change it's symbology to be invisible (no outline or fill)
        lyr9028 = arcpy.mapping.ListLayers(mxd, "Mask9028", df)[0]
        # arcpy.AddMessage("\nlyr9028 Symbology type: {0}".format(lyr9028.symbologyType))
        arcpy.mapping.UpdateLayer(df, lyr9028, lyrSymFile, True)

        # Enable masking of the contour layers
        #  (this requires a Production Mapping and Charting license)
        arcpyproduction.mapping.EnableLayerMasking(df, 'true')

        for lyr in arcpy.mapping.ListLayers(mxd):
            # arcpy.AddMessage("Layer name is: {0}".format(lyr.name))
            if   lyr.name.find(r"Contours 1128") >= 0:
                # arcpy.AddMessage("\nMasking Contour 1128")
                arcpyproduction.mapping.MaskLayer(df, 'APPEND', lyr1128, lyr)

            elif lyr.name.find(r"Contours 2257") >= 0:
                # arcpy.AddMessage("\nMasking Contour 2257")
                arcpyproduction.mapping.MaskLayer(df, 'APPEND', lyr2257, lyr)

            elif lyr.name.find(r"Contours 4514") >= 0:
                # arcpy.AddMessage("\nMasking Contour 4514")
                arcpyproduction.mapping.MaskLayer(df, 'APPEND', lyr4514, lyr)

            elif lyr.name.find(r"Contours 9028") >= 0:
                # arcpy.AddMessage("\nMasking Contour 9028")
                arcpyproduction.mapping.MaskLayer(df, 'APPEND', lyr9028, lyr)

        mxd.relativePaths = True
        mxd.title = "NRCS Contour dataset with a 2 foot interval, labeled in 10 foot intervals"
        mxd.tags = "Contour, Elevation, Annotation"
        mxd.description = "This service represents NRCS contours with a 2 foot interval, generated from Lidar datasets."
        mxd.save()

        del mxd
        
    else:
        arcpy.AddError("Failed to find project for job.")
    
    arcpy.AddMessage("Operation complete")

if __name__ == '__main__':
    jobID = arcpy.GetParameterAsText(0)
    # jobID = 4801

    PrepareContoursForPublishing(jobID)

    
#     arcpy.AddMessage(inspect.getfile(inspect.currentframe()))
#     arcpy.AddMessage(sys.version)
#     arcpy.AddMessage(sys.executable)
#     
#     executedFrom = sys.executable.upper()
#     
#     if not ("ARCMAP" in executedFrom or "ARCCATALOG" in executedFrom or "RUNTIME" in executedFrom):
#         arcpy.AddMessage("Getting parameters from command line...")
# 
#         mxdTemplate = sys.argv[1]
#         arcpy.AddMessage("ArcMap Document:  {0}".format(mxdTemplate))
#         mxdTemplate = mxdTemplate.strip()
# 
#         ContourFC = sys.argv[2]
#         arcpy.AddMessage("Contour Feature Class:  {0}".format(ContourFC))
#         ContourFC = ContourFC.strip()
# 
#         ContourBoundFC = sys.argv[3]
#         arcpy.AddMessage("Contour Boundary Feature Class:  {0}".format(ContourBoundFC))
#         ContourBoundFC = ContourBoundFC.strip()
#     else:
#         arcpy.AddMessage("Getting parameters from GetParameterAsText...")
#         mxdTemplate = arcpy.GetParameterAsText(0)
#         arcpy.AddMessage("ArcMap Document:  {0}".format(mxdTemplate))
#         mxdTemplate = mxdTemplate.strip()
# 
#         ContourFC = arcpy.GetParameterAsText(1)
#         arcpy.AddMessage("Contour Feature Class:  {0}".format(ContourFC))
#         ContourFC = ContourFC.strip()
# 
#         ContourBoundFC = arcpy.GetParameterAsText(2)
#         arcpy.AddMessage("Contour Boundary Feature Class:  {0}".format(ContourBoundFC))
#         ContourBoundFC = ContourBoundFC.strip()
# 
#     PrepareContoursForPublishing(mxdTemplate, ContourFC, ContourBoundFC)
