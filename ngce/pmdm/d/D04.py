# Name: D04.py
#
# Purpose: Generate fishnets to approximate extent of las files within las dataset and clip a buffered version
# of that fishnet to exclude external edges. The revised output of D03.py (remove slivers/blocks through ArcGIS Pro
# Project using D_APRX.py) is also added as a surface constraint to the input las file.
#
# Author: jeff8977

from ngce.pmdm.d.D_Config import *
import arcpy
import math
import time
import sys
import os


def collect_table_inputs(j_id):

    print('Collecting Inputs From Database Table')

    j_id = int(j_id)

    table = JOB_SOURCE

    values = []
    with arcpy.da.SearchCursor(table, ['WMX_Job_ID', 'Project_ID', 'Project_Dir']) as cursor:
        for r in cursor:
            if r[0] == j_id:
                values.append(r[1])
                values.append(r[2])

    if not values:
        raise Exception('Script Was Unable to Acquire Inputs From Table. Please Check Job ID')
    else:
        return values


def grid_calc(lasd):

    print('Setting Fishnet Dimensions')

    desc = arcpy.Describe(lasd)
    return round(math.sqrt(desc.fileCount))


def handle_fishnet(lasd, base_dir, grid_dim):

    print('Creating Buffered Fishnet & Clipping')

    # Create Path for Fishnet
    fishnet_path = os.path.join(base_dir, 'FISHNET')
    os.mkdir(fishnet_path)

    # Build Fishnet From Input LASD
    desc = arcpy.Describe(lasd)
    extent = desc.extent
    origin_point = str(extent.XMin) + ' ' + str(extent.YMin)
    y_axis = str(extent.XMin) + ' ' + str(extent.YMax)
    fishnet_name = os.path.join(fishnet_path, 'fishnet.shp')
    grid = arcpy.CreateFishnet_management(
        fishnet_name,
        origin_point,
        y_axis,
        '0',
        '0',
        grid_dim,
        grid_dim,
        '#',
        'False',
        lasd,
        'POLYGON'
    )

    #  Buffer Fishnet To Ensure Grid Cells Overlap
    fish_buff = os.path.join(fishnet_path, 'fishnet_buff.shp')
    buff_grid = arcpy.Buffer_analysis(grid, fish_buff, '2 Meters', 'FULL', 'FLAT', 'NONE', '#', 'PLANAR')

    # Clip Buffered Fishnet With Originial To Exclude Outer Buffers
    clip_name = os.path.join(fishnet_path, D04_FINAL)
    arcpy.Clip_analysis(buff_grid, fishnet_name, clip_name)


if __name__ == '__main__':

    # Get Script Start Time
    start = time.time()

    try:
        # Collect Job ID from Command Line
        job_id = sys.argv[1]

        # Collect Script Inputs from Table
        inputs = collect_table_inputs(job_id)
        project_id, project_dir = inputs[0], inputs[1]

        # Create Directory For Script Results
        derived_dir = os.path.join(project_dir, DERIVED)
        base_dir = os.path.join(derived_dir, D04)
        os.mkdir(base_dir)

        # Reference LASD
        target_lasd = os.path.join(derived_dir, project_id + '.lasd')

        # Reference D03 Output
        d03_output = os.path.join(derived_dir, D03, 'RESULTS', D03_FINAL)

        # Populate D03 With Z_MIN Field
        arcpy.LasPointStatsByArea_3d(target_lasd, d03_output, 'Z_MIN')

        # Add D03 As Soft Replace
        arcpy.AddFilesToLasDataset_management(target_lasd, '#', '#', [[d03_output, 'Z_MIN', 'Soft_Replace']])

        # Get Dimension For LAS Fishnet
        grid_dimension = grid_calc(target_lasd)

        # Create Fishnet
        handle_fishnet(target_lasd, base_dir, grid_dimension)

    except Exception as e:
        print('Exception', e)

    finally:
        print('Program Ran: {0}'.format(time.time() - start))
