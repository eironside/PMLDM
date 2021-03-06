# Name: D05.py
#
# Purpose: Uses the fishnet generated in D04 to create multiprocessing extents for creation of raster files.
#
# Author: jeff8977

from multiprocessing import Pool, cpu_count
from ngce.pmdm.d.D_Config import *
from functools import partial
import arcpy
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


def filter_fishnet(data_domain, base_dir, fishnet):

    print('Filtering Fishnet & Collecting Task Extents')

    # Create Path for Fishnet Results
    fishnet_path = os.path.join(base_dir, 'FISHNET')
    os.mkdir(fishnet_path)

    # Filter Fishnet By Data Domain
    layer = arcpy.MakeFeatureLayer_management(fishnet, 'fishnet_layer')
    sel = arcpy.SelectLayerByLocation_management(layer, 'INTERSECT', data_domain)
    filter_fish = arcpy.CopyFeatures_management(sel, os.path.join(fishnet_path, 'filter_fishnet.shp'))

    # Populate Dictionary With Grid Cell Extents
    ext_dict = {}
    for r in arcpy.da.SearchCursor(filter_fish, ['FID', 'SHAPE@']):
        ext = r[1].extent
        box = [ext.XMin, ext.YMin, ext.XMax, ext.YMax]
        id = str(r[0])
        ext_dict[id] = box

    return ext_dict


def generate_raster(las, path, proc_dict):

    # Set Extent for Task Processing
    proc_ext = proc_dict[1]
    XMin = proc_ext[0]
    YMin = proc_ext[1]
    XMax = proc_ext[2]
    YMax = proc_ext[3]
    arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

    # Generate Raster
    try:
        arcpy.LasDatasetToRaster_conversion(
            las,
            os.path.join(path, str(proc_dict[0]) + '.tif'),
            'ELEVATION',
            'TRIANGULATION LINEAR NO_THINNING MINIMUM 0',
            'FLOAT',
            'CELLSIZE',
            1.0
        )

    except Exception as e:
        print('Exception: ', e)


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
        base_dir = os.path.join(derived_dir, D05)
        os.mkdir(base_dir)

        # Reference D04 Clipped Fishnet Output
        d04_output = os.path.join(derived_dir, D04, 'FISHNET', D04_FINAL)

        # Reference LASD
        target_lasd = os.path.join(derived_dir, project_id + '.lasd')

        # Reference Data Domain For Filtering Fishnet
        data_domain = os.path.join(derived_dir, D01, 'RESULTS', D01_DATA_DOMAIN)

        # Create Filtered Fishnet & Return Extent For Processing
        extent_dict = filter_fishnet(data_domain, base_dir, d04_output)

        # Create Path for Output Rasters
        raster_path = os.path.join(base_dir, 'RASTER')
        os.mkdir(raster_path)

        # Use Multiprocessing Pool for Raster  Generation
        pool = Pool(processes=cpu_count() - 2)
        result = pool.map_async(partial(generate_raster, target_lasd, raster_path), extent_dict.items())
        pool.close()
        pool.join()

    except Exception as e:
        print('Exception', e)

    finally:
        print('Program Ran: {0}'.format(time.time() - start))
