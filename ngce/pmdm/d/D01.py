from multiprocessing import Pool, cpu_count
from ngce.pmdm.d.D_Config import *
from functools import partial
import tempfile
import arcpy
import time
import sys
import os


def collect_table_inputs(j_id):

    print('Collecting Inputs From Database Table')

    j_id = int(j_id)

    table = JOB_SOURCE

    values = []
    with arcpy.da.SearchCursor(table, ['WMX_Job_ID', 'Project_ID' ,'Project_Dir']) as cursor:
        for r in cursor:
            if r[0] == j_id:
                values.append(r[1])
                values.append(r[2])

    if not values:
        raise Exception('Script Was Unable to Acquire Inputs From Table. Please Check Job ID')
    else:
        return values


def grid_calc():

    process_count = cpu_count() - 2
    return process_count, process_count


def collect_extents(lasd, base_dir, row, col):

    print('Collecting Task Extents')

    fishnet_path = os.path.join(base_dir, 'FISHNET')
    os.mkdir(fishnet_path)

    # Build Fishnet From Input LASD
    desc = arcpy.Describe(lasd)
    extent = desc.extent
    origin_point = str(extent.XMin) + ' ' + str(extent.YMin)
    y_axis = str(extent.XMin) + ' ' + str(extent.YMax)
    grid = arcpy.CreateFishnet_management(
        os.path.join(fishnet_path, 'fishnet.shp'),
        origin_point,
        y_axis,
        '0',
        '0',
        row,
        col,
        '#',
        'False',
        lasd,
        'POLYGON'
    )

    #  Buffer Fishnet To Ensure Grid Cells Overlap
    fish_buff = os.path.join(fishnet_path, 'fishnet_buff.shp')
    buff_grid = arcpy.Buffer_analysis(grid, fish_buff, '5 Meters', 'FULL', 'FLAT', 'NONE', '#', 'PLANAR')

    # Polygon Neighbors
    neighbor_table = os.path.join(fishnet_path, 'neigbor_table.dbf')
    neighbor_poly = arcpy.PolygonNeighbors_analysis(fish_buff, neighbor_table, 'FID')

    # Summarize Table
    summary_name = os.path.join(fishnet_path, 'summary.dbf')
    summary_table = arcpy.Statistics_analysis(neighbor_poly, summary_name, [['src_FID', 'COUNT']], 'src_FID')

    # Join Summary To Fishnet Buffer
    arcpy.JoinField_management(buff_grid, 'FID', summary_table, 'src_FID')

    # Populate Dictionary With Grid Cell Extents
    # Append Underscore To Inner Grid  Cells (if row[1] == 8)
    ext_dict = {}
    for r in arcpy.da.SearchCursor(buff_grid, ['FID', 'COUNT_src_', 'SHAPE@']):
        ext = r[2].extent
        box = [ext.XMin, ext.YMin, ext.XMax, ext.YMax]
        if r[1] == 8:
            id = ''.join((str(r[0]), '_'))
        else:
            id = str(r[0])
        ext_dict[id] = box

    print('Tasks: ', len(ext_dict))

    return ext_dict


def check_surface_constraints(lasd):

    print('Checking Existing Surface Constraints')

    if arcpy.Describe(lasd).constraintCount == 0:
        pass
    else:
        raise Exception('LASD Has Existing Surface Constraints.')


def task(target_lasd, task_dir, dictionary):

    print('Starting Task: ', dictionary[0])

    try:
        # Get Task ID
        task_id = dictionary[0]

        # Create Working Directory for Task Output
        dir_name = task_id + '__' + str(os.getpid()) + '__'
        working_dir = tempfile.mkdtemp(prefix=dir_name, dir=task_dir)

        # Set Extent for Task Processing
        proc_ext = dictionary[1]
        XMin = proc_ext[0]
        YMin = proc_ext[1]
        XMax = proc_ext[2]
        YMax = proc_ext[3]
        arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

        # Create LAS Dataset Layer
        filter_name = 'in_memory/lasd_lyr_' + task_id
        filter_lasd = arcpy.MakeLasDatasetLayer_management(target_lasd, filter_name, '2;8')

        # A - Create Raster
        a_name = os.path.join(working_dir, 'a.tif')
        a = arcpy.LasPointStatsAsRaster_management(filter_lasd, a_name, 'POINT_COUNT', 'CELLSIZE', '10')

        # B - Zero Raster
        b = arcpy.Raster(a) * 0
        b.save(os.path.join(working_dir, 'b.tif'))

        # B - Raster To Poly
        b_rtp_name = os.path.join(working_dir, 'b_rtp.shp')
        b_rtp = arcpy.RasterToPolygon_conversion(b, b_rtp_name, 'True', 'VALUE')

        # B - Get Envelope Based On Inner/Outer Grid Cell
        b_env_name = os.path.join(working_dir, 'b_env.shp')
        if task_id.endswith('_'):
            b_env = arcpy.MinimumBoundingGeometry_management(b_rtp, b_env_name, "ENVELOPE", "ALL")
        else:
            b_env = arcpy.MinimumBoundingGeometry_management(b_rtp, b_env_name, "CONVEX_HULL", "ALL")

        # C - Reclassify Raster
        c = arcpy.sa.Reclassify(b, 'VALUE', '0 0; NODATA 1')
        c.save(os.path.join(working_dir, 'c.tif'))

        # C - Raster To Poly
        c_rtp_name = os.path.join(working_dir, 'c_rtp.shp')
        c_rtp = arcpy.RasterToPolygon_conversion(c, c_rtp_name, 'False', 'VALUE')

        # D - Clip c_rtp by b_env
        d_name = os.path.join(working_dir, 'd.shp')
        d = arcpy.Clip_analysis(c_rtp, b_env, d_name)

        # Create Acres Field & Calculate on Merged D
        arcpy.AddField_management(d, 'ACRES', 'DOUBLE')
        arcpy.CalculateField_management(d, 'ACRES', '!shape.area@acres!')

        # E - Select & Copy Features
        e_sel_criteria = '"gridcode" = 1 AND "ACRES" > 1.5'
        e_sel_layer_name = 'e_sel_lyr_' + task_id
        e_sel_layer = arcpy.MakeFeatureLayer_management(d, e_sel_layer_name, e_sel_criteria)
        e_name = os.path.join(working_dir, 'e.shp')
        arcpy.CopyFeatures_management(e_sel_layer, e_name)
        arcpy.Delete_management(e_sel_layer)

        # F - Select & Copy Features
        f_sel_criteria = '"gridcode" = 0'
        f_sel_layer_name = 'f_sel_lyr_' + task_id
        f_sel_layer = arcpy.MakeFeatureLayer_management(c_rtp, f_sel_layer_name, f_sel_criteria)
        f_name = os.path.join(working_dir, 'f.shp')
        arcpy.CopyFeatures_management(f_sel_layer, f_name)
        arcpy.Delete_management(f_sel_layer)

        # Clear LAS Dataset Layer
        arcpy.Delete_management(filter_name)

        # Write Text File As Completion Flag
        open(os.path.join(working_dir, task_id + '.txt'), 'a').close()

    except Exception as e:
        print('Task Dropped: ', task_id)
        print('Exception: ', e)

    print('Finished Task: ', dictionary[0])

    return task_id


def handle_results(base_dir, task_dir, target_lasd):

    print('Handling Task Results')

    output_folders = os.listdir(task_dir)

    results = os.path.join(base_dir, 'RESULTS')
    os.mkdir(results)

    e_process = []
    f_process = []

    for folder in output_folders:
        e = os.path.join(task_dir, folder, 'e.shp')
        f = os.path.join(task_dir, folder, 'f.shp')
        if arcpy.Exists(e):
            e_process.append(e)
        if arcpy.Exists(f):
            f_process.append(f)

    # E - Merge
    e_merge_name = os.path.join(results, 'e_merge.shp')
    e_merge = arcpy.Merge_management(e_process, e_merge_name)

    # E - Dissolve (Drivers)
    e_diss_name = os.path.join(results, 'e_dissolve.shp')
    e_diss = arcpy.Dissolve_management(e_merge, e_diss_name, '#', '#', 'False', 'True')

    # Create Acres Field & Calculate on Dissolved E
    arcpy.AddField_management(e_diss, 'ACRES', 'DOUBLE')
    arcpy.CalculateField_management(e_diss, 'ACRES', '!shape.area@acres!')

    # Select & Copy Features (Clean Multipart Edge Slivers After Dissolve)
    e_diss_sel_criteria = '"ACRES" > 1.5'
    e_diss_sel_layer_name = 'e_diss_sel_lyr_'
    e_diss_sel_layer = arcpy.MakeFeatureLayer_management(e_diss, e_diss_sel_layer_name, e_diss_sel_criteria)
    e_diss_name = os.path.join(results, D01_DRIVERS)
    arcpy.CopyFeatures_management(e_diss_sel_layer, e_diss_name)
    arcpy.Delete_management(e_diss_sel_layer)

    # F - Merge
    f_merge_name = os.path.join(results, 'f_merge.shp')
    f_merge = arcpy.Merge_management(f_process, f_merge_name)

    # F - Union
    f_union_name = os.path.join(results, 'f_union.shp')
    f_union = arcpy.Union_analysis(f_merge, f_union_name, 'ALL', '#', 'NO_GAPS')

    # Dissolve G (Data Domain)
    f_diss_name = os.path.join(results, D01_DATA_DOMAIN)
    data_domain = arcpy.Dissolve_management(f_union, f_diss_name, '#', '#', 'False', 'True')

    # Apply Data Domain As Soft Clip to LASD
    constraint_param = [[data_domain, "<None>", "Soft_Clip"]]
    arcpy.AddFilesToLasDataset_management(target_lasd, "", "", constraint_param)


if __name__ == '__main__':

    # Get Script Start Time
    start = time.time()

    try:
        # Collect Job ID from Command Line
        job_id = sys.argv[1]

        # Collect Script Inputs from Table
        inputs = collect_table_inputs(job_id)
        project_id, project_dir = inputs[0], inputs[1]

        # Reference DERIVED Directory
        derived_dir = os.path.join(project_dir, DERIVED)

        # Reference LASD
        target_lasd = os.path.join(derived_dir, project_id + '.lasd')

        # Handle Surface Constraints
        check_surface_constraints(target_lasd)

        # Determine Grid Dimensions For Fishnet
        row, col = grid_calc()

        # Create Directory For Script Results
        base_dir = os.path.join(derived_dir, D01)
        os.mkdir(base_dir)

        # Collect Processing Extent Dictionary
        extent_dict = collect_extents(target_lasd, base_dir, row, col)

    except Exception as e:
        print('Script Encountered Issues While Initializing')
        print('Exception: ', e)

    else:
        try:
            # Create Directory For TASK Results
            task_dir = os.path.join(base_dir, 'TASKS')
            os.mkdir(task_dir)

            # Create Pool & Map Processing Dictionary To Task Function
            pool = Pool(processes=cpu_count() - 2)
            result = pool.map_async(partial(task, target_lasd, task_dir), extent_dict.items())
            pool.close()
            pool.join()

            # Create Results From Task Output
            handle_results(base_dir, task_dir, target_lasd)

        except Exception as e:
            print('Script Encountered Issues While Processing')
            print('Exception: ', e)

    finally:
        print('Program Ran: {0}'.format(time.time() - start))
