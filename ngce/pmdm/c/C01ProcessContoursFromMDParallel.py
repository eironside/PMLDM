import arcpy
from arcpy.sa import Functions
from datetime import datetime
from functools import partial
from multiprocessing import Pool, cpu_count
import os
import sys
import time
import glob #using in handle_results BJN
import arcpy.cartography as ca
from ngce import Utility
from ngce.cmdr.CMDR import ProjectJob
from ngce.cmdr.CMDRConfig import OCS
from ngce.cmdr.JobUtil import getProjectFromWMXJobID
from ngce.contour.ContourConfig import CONTOUR_GDB_NAME, WEB_AUX_SPHERE, \
    CONTOUR_INTERVAL, CONTOUR_UNIT, CONTOUR_SMOOTH_UNIT, \
    DISTANCE_TO_CLIP_MOSAIC_DATASET, DISTANCE_TO_CLIP_CONTOURS, SKIP_FACTOR, CONTOUR_NAME_OCS, CONTOUR_NAME_WM
from ngce.folders import ProjectFolders
from ngce.folders.FoldersConfig import DTM
from ngce.pmdm.a import A05_C_ConsolidateRasterInfo
from ngce.pmdm.a.A04_B_CreateLASStats import doTime, deleteFileIfExists
from ngce.pmdm.a.A05_B_RevalueRaster import FIELD_INFO, V_UNIT
from ngce.raster import Raster

CPU_HANDICAP = 1
TRIES_ALLOWED = 10
USE_FEATURE_CLASS = True

def generateHighLow(workspace, name, clip_contours, ref_md):
    cont_poly1 = os.path.join(workspace, 'O12_poly_' + name + '.shp')
    cont_poly2 = os.path.join(workspace, 'O13_poly_' + name + '.shp')
    arcpy.FeatureToPolygon_management(in_features=clip_contours, out_feature_class=cont_poly1, cluster_tolerance="", attributes="ATTRIBUTES", label_features="")
    arcpy.MultipartToSinglepart_management(in_features=cont_poly1, out_feature_class=cont_poly2)
    select_set = []
    with arcpy.da.UpdateCursor(cont_poly2, ["FID", "SHAPE@"]) as cursor:  # @UndefinedVariable
        for row in cursor:
            parts = row[1].partCount
            boundaries = row[1].boundary().partCount
            if boundaries > parts:
                select_set.append(row[0])

    cont_poly3 = 'O13_poly_' + name + '_layer'
    arcpy.MakeFeatureLayer_management(in_features=cont_poly2, out_layer=cont_poly3, where_clause='"FID" IN(' + ','.join(select_set) + ')', workspace="", field_info="")
    arcpy.DeleteFeatures_management(cont_poly3)
    arcpy.AddSurfaceInformation_3d(in_feature_class=cont_poly2, in_surface=ref_md, out_property="Z_MEAN", method="BILINEAR")

def generate_con_workspace(con_folder):

    # Create File GDB for Contours
    if not os.path.exists(con_folder):
        os.makedirs(con_folder)

    contour_file_gdb_path = os.path.join(con_folder, CONTOUR_GDB_NAME)
    if not os.path.exists(contour_file_gdb_path):
        arcpy.AddMessage("\nCreating Contour GDB:   {0}".format(contour_file_gdb_path))
        arcpy.CreateFileGDB_management(
            con_folder,
            CONTOUR_GDB_NAME,
            out_version="CURRENT"
        )

    # Create Scratch Folder for Intermediate Products
    scratch_path = os.path.join(con_folder, 'C01Scratch')
    arcpy.AddMessage("\nCreating Scratch Folder:    " + scratch_path)
    if not os.path.exists(scratch_path):
        os.makedirs(scratch_path)

    return (contour_file_gdb_path, scratch_path)

def createRefDTMMosaic(in_md_path, out_md_path, v_unit):
    from datetime import datetime
    
    a = datetime.now()
    if arcpy.Exists(out_md_path):
        arcpy.AddMessage("Referenced mosaic dataset exists " + out_md_path)
    else:
        arcpy.CreateReferencedMosaicDataset_management(in_dataset=in_md_path, out_mosaic_dataset=out_md_path, where_clause="TypeID = 1")

        raster_function_path = Raster.Contour_Meters_function_chain_path
        v_unit = str(v_unit).upper()
        if v_unit.find("FEET") >= 0 or v_unit.find("FOOT") >= 0 or  v_unit.find("FT") >= 0:
            raster_function_path = Raster.Contour_IntlFeet_function_chain_path
            #if v_unit.find("INTL") >= 0 or v_unit.find("INTERNATIONAL") >= 0 or v_unit.find("STANDARD") >= 0 or v_unit.find("STD") >= 0:
            #    raster_function_path = Raster.Contour_IntlFeet_function_chain_path
            if v_unit.find("US") >= 0 or v_unit.find("SURVEY") >= 0:
                arcpy.AddMessage("Using US FOOT Raster Function")
                raster_function_path = Raster.Contour_Feet_function_chain_path
            else:
                arcpy.AddMessage("Using INT FOOT Raster Function")
        else:
            arcpy.AddMessage("Using METER Raster Function")

        arcpy.EditRasterFunction_management(in_mosaic_dataset=out_md_path, edit_mosaic_dataset_item="EDIT_MOSAIC_DATASET", edit_options="REPLACE", function_chain_definition=raster_function_path, location_function_name="")
        Utility.addToolMessages()

        arcpy.CalculateStatistics_management(in_raster_dataset=out_md_path, x_skip_factor=SKIP_FACTOR, y_skip_factor=SKIP_FACTOR, ignore_values="", skip_existing="OVERWRITE", area_of_interest="Feature Set")

        doTime(a, "Created referenced mosaic dataset " + out_md_path)





def create_iterable(scratch_folder, prints, distance_to_clip_md, distance_to_clip_contours):
    from datetime import datetime
    
    a = datetime.now()
    arcpy.AddMessage('Create Multiprocessing Iterable')

    # Make sure that our footprints have the zran field
    zranField = arcpy.ListFields(prints, "zran")
    if len(zranField) <= 0:
        arcpy.AddField_management(prints, "zran", "DOUBLE")
        arcpy.CalculateField_management(prints, "zran", 1 + 1, "PYTHON_9.3")

    # Go up one directory so we don't have to delete if things go wrong down in scratch
    tmp_scratch_folder = os.path.split(scratch_folder)[0]
    ftprints_path = tmp_scratch_folder
    ftprints_clip_md = "footprints_clip_md.shp"
    ftprints_clip_cont = "footprints_clip_cont.shp"
    if USE_FEATURE_CLASS:
        ftprints_path = os.path.join(tmp_scratch_folder, "Scratch.gdb")
        ftprints_clip_md = "footprints_clip_md"
        ftprints_clip_cont = "footprints_clip_cont"
        if not os.path.exists(ftprints_path):
            arcpy.AddMessage("\nCreating Scratch GDB:   {0}".format(ftprints_path))
            arcpy.CreateFileGDB_management(
                tmp_scratch_folder,
                "Scratch.gdb",
                out_version="CURRENT"
            )

    ext_dict = {}
    tmp_buff_name = os.path.join(ftprints_path, ftprints_clip_md)
    if not os.path.exists(tmp_buff_name):
        arcpy.Buffer_analysis(
            prints,
            tmp_buff_name,
            "{} METERS".format(distance_to_clip_md)
        )
        arcpy.AddMessage("Created new {}".format(tmp_buff_name))
    else:
        arcpy.AddMessage("Using existing {}".format(tmp_buff_name))

    with arcpy.da.SearchCursor(tmp_buff_name, ["Name", "SHAPE@", "zran"]) as cursor:  # @UndefinedVariable

        for row in cursor:

            row_info = []

            # Get Values
            rowname = row[0]
            geom = row[1]
            zran = row[2]
            if zran > 0 and isProcessFile(rowname, scratch_folder):
                box = geom.extent.polygon

                row_info.append(box)
                ext_dict[rowname] = row_info

    tmp_buff_name2 = os.path.join(ftprints_path, ftprints_clip_cont)
    if not os.path.exists(tmp_buff_name2):
        arcpy.Buffer_analysis(
            prints,
            tmp_buff_name2,
            "{} METERS".format(distance_to_clip_contours)
        )
        arcpy.AddMessage("Created new {}".format(tmp_buff_name2))
    else:
        arcpy.AddMessage("Using existing {}".format(tmp_buff_name2))

    with arcpy.da.SearchCursor(tmp_buff_name2, ["Name", "SHAPE@", "zran"]) as cursor:  # @UndefinedVariable

        for row in cursor:

            # Get Values
            rowname = row[0]
            geom = row[1]
            zran = row[2]
            if zran > 0 and isProcessFile(rowname, scratch_folder):
                row_info = ext_dict[rowname]
                row_info.append(geom)
                ext_dict[rowname] = row_info

    for index, item in enumerate(ext_dict.items()):
        row = item[1]
        row.append(index)


    arcpy.AddMessage('Multiprocessing Tasks: ' + str(len(ext_dict)))
    a = doTime(a, "Created Runnable Dictionary")
    return ext_dict




def generate_contour(md, cont_int, contUnits, rasterUnits, smooth_tol, scratch_path, proc_dict):
    from datetime import datetime
    
    name = proc_dict[0]
    index = str(proc_dict[1][2])

    arcpy.AddMessage("Checking out licenses")
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")


    created = False
    tries = 0
    outOfMemory = False
    vertexLimit = 100
    featureCount = 25000
    while not created and tries <= TRIES_ALLOWED:
        tries = tries + 1
        try:
            a = datetime.now()
            aa = a
            Utility.setArcpyEnv(True)
            arcpy.AddMessage('STARTING ' + name + ' ' + index + ': Generating Contours')

            buff_poly = proc_dict[1][0]
            clip_poly = proc_dict[1][1]
            #arcpy.AddMessage("\t{}: Buffer Poly '{}'".format(name, buff_poly))
            #arcpy.AddMessage("\t{}: Clip Poly '{}'".format(name, clip_poly))

            arcpy.env.extent = buff_poly.extent
            arcpy.env.XYResolution = "0.0001 Meters"

            if USE_FEATURE_CLASS:
                gdbName = "{}.gdb".format(name)
                fileExtension = ""
                workspace = os.path.join(scratch_path, gdbName)
            else:
                fileExtension = ".shp"
                workspace = os.path.join(scratch_path, name)

            if not os.path.exists(workspace):
                # Don't delete if it exists, keep our previous work to save time
                if USE_FEATURE_CLASS:
                    arcpy.AddMessage("\nCreating Workspace GDB:   {0}".format(workspace))
                    arcpy.CreateFileGDB_management(
                        scratch_path,
                        gdbName,
                        out_version="CURRENT"
                    )
                else:
                    os.mkdir(workspace)

            arcpy.env.workspace = workspace
            a = doTime(a, '\t' + name + ' ' + index + ': Created scratch workspace' + workspace)

            focal2_path = md
            md_desc = arcpy.Describe(md)
            if not md_desc.referenced:
                arcpy.AddError("\t{}: ERROR Referenced Mosaic not found '{}'".format(name, focal2_path))
                
            arcpy.AddMessage("\t{}: Referenced Mosaic found '{}'".format(name, focal2_path))
            base_name = 'O08_BaseCont_' + name + fileExtension
            base_contours = os.path.join(workspace, base_name)
            if not os.path.exists(base_contours):
                arcpy.MakeRasterLayer_management(in_raster=focal2_path, out_rasterlayer=base_name)
                Functions.Contour(
                    base_name,
                    base_contours,
                    int(cont_int)
                )
                a = doTime(a, '\t' + name + ' ' + index + ': Contoured to ' + base_contours)
            del base_name

            simple_contours = os.path.join(workspace, 'O09_SimpleCont_' + name + fileExtension)
            if outOfMemory:
                diced_contours = os.path.join(workspace, 'O08ADicedCont_' + name + fileExtension)
                arcpy.Dice_management(base_contours, diced_contours, vertexLimit)
                a = doTime(a, '\t' + name + ' ' + index + ': Diced to ' + diced_contours)

                cartographic_partitions = os.path.join(workspace, 'Carto_Partitions_' + name + fileExtension)
                arcpy.CreateCartographicPartitions_cartography(diced_contours, cartographic_partitions, featureCount)
                a = doTime(a, '\t' + name + ' ' + index + ': Created Cartographic Partitions at ' + cartographic_partitions)
                arcpy.env.cartographicPartitions = cartographic_partitions

                base_contours = diced_contours

                if arcpy.Exists(simple_contours):
                    arcpy.Delete_management(simple_contours)

            if not os.path.exists(simple_contours):
                ca.SimplifyLine(
                    base_contours,
                    simple_contours,
                    "POINT_REMOVE",
                    "0.000001 DecimalDegrees",
                    "FLAG_ERRORS",
                    "NO_KEEP",
                    "NO_CHECK"
                )
                a = doTime(a, '\t' + name + ' ' + index + ': Simplified to ' + simple_contours)
            del base_contours

            if rasterUnits == "Foot" or rasterUnits == "FT":
                maxShapeLength = 16.404
            elif rasterUnits == "Meter" or rasterUnits == "MT":
                maxShapeLength = 5
            else:
                maxShapeLength = 0

            # BJN Need to add Shape_Length attribute to shapefile & calculate length if USE_FEATURE_CLASS = False
            if not USE_FEATURE_CLASS:
                SHAPE_LENGTH = 'Length'
                arcpy.AddField_management(simple_contours, SHAPE_LENGTH, 'Double')
                arcpy.CalculateField_management(simple_contours, SHAPE_LENGTH, '!shape.length!', 'PYTHON_9.3')
            else:
                SHAPE_LENGTH = 'Shape_Length'
                
            greaterThan2MetersSelection = 'greaterThan2MetersSelection' #BJN
            arcpy.MakeFeatureLayer_management(simple_contours, greaterThan2MetersSelection, "{} > {}".format(SHAPE_LENGTH, maxShapeLength))

            # TODO: Select anything under 2 meters in length to a new 'small_contours' feature class
            # Delete the selection from the simple_contours
            # Delete any small contours snippets that are within 2 meters of the tile boundary
            # Run Feature to Point on the small contours and use the output in our contour map and service

            smooth_contours = os.path.join(workspace, 'O10_SmoothCont_' + name + fileExtension)
            if not os.path.exists(smooth_contours):
                ca.SmoothLine(
                    greaterThan2MetersSelection,
                    smooth_contours,
                    "PAEK",
                    "{} DecimalDegrees".format(smooth_tol),
                    "",
                    "NO_CHECK"
                )
                a = doTime(a, '\t' + name + ' ' + index + ': Smoothed to ' + smooth_contours)
            del simple_contours

            # put this up one level to avoid re-processing all of above if something goes wrong below
            clip_workspace = os.path.split(workspace)[0]
            clip_contours = os.path.join(clip_workspace, 'O11_ClipCont_{}.shp'.format(name)) #BJN
            if not os.path.exists(clip_contours):
                arcpy.Clip_analysis(
                    in_features=smooth_contours,
                    clip_features=clip_poly,
                    out_feature_class=clip_contours
                )
                a = doTime(a, '\t' + name + ' ' + index + ': Clipped to ' + clip_contours)
            del smooth_contours

            arcpy.RepairGeometry_management(in_features=clip_contours,
                                            delete_null="DELETE_NULL")

            Utility.addAndCalcFieldLong(dataset_path=clip_contours,
                                        field_name="CTYPE",
                                        field_value="getType( !CONTOUR! )",
                                        code_block="def getType(contour):\n\n   type = 2\n\n   if contour%10 == 0:\n\n      type = 10\n\n   if contour%20 == 0:\n\n      type = 20\n\n   if contour%50 == 0:\n      type = 50\n   if contour%100 == 0:\n      type = 100\n   if contour%500 == 0:\n      type = 500\n   if contour%1000 == 0:\n      type = 1000\n   if contour%5000 == 0:\n      type = 5000\n   return type",
                                        add_index=False)

            Utility.addAndCalcFieldLong(dataset_path=clip_contours,
                                        field_name="INDEX",
                                        field_value="getType( !CONTOUR! )",
                                        code_block="def getType(contour):\n\n   type = 0\n\n   if contour%" + str(int(cont_int * 5)) + " == 0:\n\n      type = 1\n   return type",
                                        add_index=False)
    #             Utility.addAndCalcFieldText(dataset_path=clip_contours,
    #                                         field_name="LastMergedFC",
    #                                         field_length=100,
    #                                         field_value=name,
    #                                         add_index=False)
    #             Utility.addAndCalcFieldText(dataset_path=clip_contours,
    #                                         field_name="ValidationCheck",
    #                                         field_length=100,
    #                                         field_value='"'+name+'"',
    #                                         add_index=False)
            Utility.addAndCalcFieldText(dataset_path=clip_contours,
                                        field_name="UNITS",
                                        field_length=20,
                                        field_value='"' + CONTOUR_UNIT + '"',
                                        add_index=False)
            Utility.addAndCalcFieldText(dataset_path=clip_contours,
                                        field_name="name",
                                        field_length=79,
                                        field_value='"' + name + '"',
                                        add_index=False)
            a = doTime(a, '\t' + name + ' ' + index + ': Added fields to ' + clip_contours)

            try:
                arcpy.DeleteField_management(in_table=clip_contours, drop_field="ID;InLine_FID;SimLnFlag;MaxSimpTol;MinSimpTol")
                a = doTime(a, '\t' + name + ' ' + index + ': Deleted fields from ' + clip_contours)
            except:
                pass

            doTime(aa, 'FINISHED ' + name + ' ' + index)
            created = True

        except arcpy.ExecuteError as exeErr:
            errorCode = str(exeErr).split(':')[0]
            if errorCode == 'ERROR 000426':
                if tries > 1 and outOfMemory:
                    vertexLimit *= 0.75
                    featureCount *= 0.75
                outOfMemory = True
            arcpy.AddMessage('\t\tProcess Dropped: ' + name)
            arcpy.AddMessage('\t\tException: ' + str(exeErr))
            if tries > TRIES_ALLOWED:
                arcpy.AddError('Too many tries, Dropped: {}'.format(name))
        except Exception as e:
            arcpy.AddMessage('\t\tProcess Dropped: ' + name)
            arcpy.AddMessage('\t\tException: ' + str(e))
            if tries > TRIES_ALLOWED:
                arcpy.AddError('Too many tries, Dropped: {}'.format(name))
    try:
        arcpy.AddMessage("Checking in licenses")
        arcpy.CheckInExtension("3D")
        arcpy.CheckInExtension("Spatial")
    except:
        pass


def handle_results(scratch_dir, contour_dir):
    from datetime import datetime
##    output_folders = os.listdir(scratch_dir)

    merge_list = glob.glob(os.path.join(scratch_dir, 'O11_ClipCont_*.shp')) #formerly [] BJN

    a = datetime.now()
    merge_name = os.path.join(contour_dir, CONTOUR_NAME_OCS)
    project_name = os.path.join(contour_dir, CONTOUR_NAME_WM)
    if arcpy.Exists(merge_name):
        arcpy.AddMessage("Merged OCS Contours exist: " + merge_name)
    else:
        # Delete the projected since they might have changed with the merge
        deleteFileIfExists(project_name, True)
        arcpy.Merge_management(merge_list, merge_name)
        try:
            arcpy.DeleteField_management(in_table=merge_name, drop_field="ID;InLine_FID;SimLnFlag;MaxSimpTol;MinSimpTol")
        except:
            pass
        doTime(a, 'Merged ' + str(len(merge_list)) + ' Multiprocessing Results into ' + merge_name)


    if arcpy.Exists(project_name):
        arcpy.AddMessage("Projected Contours exist: " + project_name)
    else:
        arcpy.Project_management(
            merge_name,
            project_name,
            WEB_AUX_SPHERE
        )
        try:
            arcpy.DeleteField_management(in_table=merge_name, drop_field="ID;InLine_FID;SimLnFlag;MaxSimpTol;MinSimpTol")
        except:
            pass
        doTime(a, 'Projected Multiprocessing Results to ' + project_name)

def isProcessFile(f_name, scratch_dir):
    process_file = False
    if f_name is not None:
        cont = os.path.join(scratch_dir, 'O11_ClipCont_{}.shp'.format(f_name)) #BJN
        if not os.path.exists(cont):
            arcpy.AddMessage("PROCESS (Missing): " + cont)
            process_file = True
        else:
            try:
                rows = [row for row in arcpy.da.SearchCursor(cont, "OID@")]  # @UndefinedVariable
                rows = len(rows)
                if rows <= 0:
                    arcpy.AddMessage("PROCESS (0 Rows): " + cont)
                    arcpy.Delete_management(cont)
                    process_file = True
            except:
                arcpy.AddMessage("\tFailed to isProcess contour file: " + cont)
                process_file = True

    return process_file



def createTiledContours(ref_md, cont_int, cont_unit, raster_vertical_unit, smooth_unit, scratch_path, run_dict, run_again=True):
    arcpy.AddMessage("---- Creating Contours on {} -----".format(len(run_dict.items())))
    # Map Generate Contour Function to Footprints
    items = run_dict.items()

    pool = Pool(processes=cpu_count() - CPU_HANDICAP)
    pool.map(
        partial(
            generate_contour,
            ref_md,
            cont_int,
            cont_unit,
            raster_vertical_unit,
            smooth_unit,
            scratch_path
        ),
        items
    )

    pool.close()
    pool.join()

    if run_again:
        # run again to re-create missing tiles if one or more dropped
        # @TODO: Figure out why we have to do this!!
        createTiledContours(ref_md, cont_int, cont_unit, raster_vertical_unit, smooth_unit, scratch_path, run_dict, False)

def processJob(ProjectJob, project, ProjectUID):
    start = time.time()
    a = start
    # From ContourConfig
    cont_int = CONTOUR_INTERVAL
    cont_unit = CONTOUR_UNIT
    smooth_unit = CONTOUR_SMOOTH_UNIT
    distance_to_clip_md = DISTANCE_TO_CLIP_MOSAIC_DATASET
    distance_to_clip_contours = DISTANCE_TO_CLIP_CONTOURS

    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    derived_folder = ProjectFolder.derived.path
    published_folder = ProjectFolder.published.path
#     project_id = ProjectJob.getProjectID(project)
    ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
    contour_folder = ProjectFolder.derived.contour_path
#     raster_folder = ProjectFolder.published.demLastTiff_path


    filegdb_name, filegdb_ext = os.path.splitext(ProjectFolder.published.fgdb_name)  # @UnusedVariable
    publish_filegdb_name = "{}_{}.gdb".format(filegdb_name, DTM)

#     published_path = os.path.join(published_folder, DTM)
    published_filegdb_path = os.path.join(published_folder, publish_filegdb_name)
    md = os.path.join(published_filegdb_path, "{}{}".format(DTM, OCS))

    derived_filegdb_path = os.path.join(derived_folder, ProjectFolder.derived.fgdb_name)
    ref_md = os.path.join(derived_filegdb_path, "ContourPrep")
    ft_prints = A05_C_ConsolidateRasterInfo.getRasterFootprintPath(derived_filegdb_path, DTM)

    ###############################################################################
    # CMDR Class Variables & Inputs From Previous Jobs
    ###############################################################################
#     contour_folder    = r'C:\Users\jeff8977\Desktop\NGCE\OK_Sugar_Testing\DERIVED\CONTOURS'
#     published_folder  = r'C:\Users\jeff8977\Desktop\NGCE\OK_Sugar_Testing\PUBLISHED'
#     raster_folder     = r'C:\Users\jeff8977\Desktop\NGCE\OK_Sugar_Testing\PUBLISHED\DTM'
#     project_id = r'OK_SugarCreek_2008'

#     md = r'C:\Users\jeff8977\Desktop\NGCE\OK_Sugar\DERIVED\CONTOURS\Temp_MD_origCS.gdb\MD'
#     ft_prints = r'C:\Users\jeff8977\Desktop\NGCE\OK_Sugar\DERIVED\CONTOURS\Temp_MD_origCS.gdb\MD_Footprints'

    raster_vertical_unit = 'MT'
    foot_fields = [FIELD_INFO[V_UNIT][0]]
    for row in arcpy.da.SearchCursor(ft_prints, foot_fields):  # @UndefinedVariable
        raster_vertical_unit = row[0]
        break
    del row
    arcpy.AddMessage("Got input raster vertical unit: {}".format(raster_vertical_unit))

#     PYTHON_EXE = os.path.join(r'C:\Python27\ArcGISx6410.5', 'pythonw.exe')
#
#     jobId = '1'
    ###############################################################################
    ###############################################################################

    try:
        a = datetime.now()
        # Generate Script Workspaces
        contour_gdb, scratch_path = generate_con_workspace(contour_folder)
        a = doTime(a, "Created Contour Workspace\n\t{}\n\t{}".format(contour_gdb, scratch_path))

        # Create referenced DTM mosaic with the pixel pre-setup for contour output
        createRefDTMMosaic(md, ref_md, raster_vertical_unit)

        # Collect Processing Extents
        run_dict = create_iterable(scratch_path, ft_prints, distance_to_clip_md, distance_to_clip_contours)


    except Exception as e:
        arcpy.AddWarning('Exception Raised During Script Initialization')
        arcpy.AddWarning('Exception: ' + str(e))


    try:
        createTiledContours(ref_md, cont_int, cont_unit, raster_vertical_unit, smooth_unit, scratch_path, run_dict)

        # Merge Contours
        handle_results(scratch_path, contour_gdb)

    except Exception as e:
        arcpy.AddMessage('Exception Raised During Multiprocessing')
        arcpy.AddError('Exception: ' + str(e))

    finally:
        run = time.time() - start
        arcpy.AddMessage('Script Ran: ' + str(run))
        
def processExternalJob(md, ft_prints, contour_folder, cont_int = 2, raster_vertical_unit = 'MT'):
    '''
    # contour_folder = Folder to generate the contour features in. A FGDB and Scratch folder are created here to generate the artifacts
    # md = Path to the mosaic dataset that contains ground elevation in original coordinate system.
    # ft_prints = Path to a shapefile that represents the image footprint in the mosaic dataset.
    # cont_int = Contour interval (default is 2 valid values are 1 and 2)
    # raster_vertical_unit = Vertical units of the original coordinate system (default is MT Valid values are MT, FT, and FT_US)
    # TODO lookup valid raster vertical units
    '''
    start = time.time()
    a = start
    # From ContourConfig
    cont_unit = CONTOUR_UNIT
    smooth_unit = CONTOUR_SMOOTH_UNIT
    distance_to_clip_md = DISTANCE_TO_CLIP_MOSAIC_DATASET
    distance_to_clip_contours = DISTANCE_TO_CLIP_CONTOURS

    arcpy.AddMessage("Got input raster vertical unit: {}".format(raster_vertical_unit))

    try:
        a = datetime.now()
        # Generate Script Workspaces
        contour_gdb, scratch_path = generate_con_workspace(contour_folder)
        a = doTime(a, "Created Contour Workspace\n\t{}\n\t{}".format(contour_gdb, scratch_path))

        ref_md = os.path.join(contour_gdb, "ContourPrep")
        # Create referenced DTM mosaic with the pixel pre-setup for contour output
        createRefDTMMosaic(md, ref_md, raster_vertical_unit)

        # Collect Processing Extents
        run_dict = create_iterable(scratch_path, ft_prints, distance_to_clip_md, distance_to_clip_contours)


    except Exception as e:
        arcpy.AddWarning('Exception Raised During Script Initialization')
        arcpy.AddWarning('Exception: ' + str(e))

    try:
        createTiledContours(ref_md, cont_int, cont_unit, raster_vertical_unit, smooth_unit, scratch_path, run_dict)

        # Merge Contours
        handle_results(scratch_path, contour_gdb)

    except Exception as e:
        arcpy.AddMessage('Exception Raised During Multiprocessing')
        arcpy.AddError('Exception: ' + str(e))

    finally:
        run = time.time() - start
        arcpy.AddMessage('Script Ran: ' + str(run))

def CreateContoursFromMD(strJobId):
    Utility.printArguments(["WMXJobID"],
                           [strJobId], "C01 CreateContoursFromMD")
    aa = datetime.now()

    project_job, project, strUID = getProjectFromWMXJobID(strJobId)  # @UnusedVariable

    processJob(project_job, project, strUID)

    doTime(aa, "Operation Complete: C01 Create Contours From MD")

if __name__ == '__main__':
    arcpy.env.overwriteOutput = True

    arcpy.AddMessage("Checking out licenses")
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")

    if len(sys.argv) == 2:
        projId = sys.argv[1]

        CreateContoursFromMD(projId)
    elif len(sys.argv) > 2:
        md = sys.argv[1]
        ft_prints = sys.argv[2]
        contour_folder = sys.argv[3]
        cont_int = sys.argv[4]
        raster_vertical_unit = sys.argv[5]
        processExternalJob(md, ft_prints, contour_folder, cont_int, raster_vertical_unit)
    else:
        # DEBUG
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

        processJob(project_job, project, UID)


    try:
        arcpy.AddMessage("Checking in licenses")
        arcpy.CheckInExtension("3D")
        arcpy.CheckInExtension("Spatial")
    except:
        pass




