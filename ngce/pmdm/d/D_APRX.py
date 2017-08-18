# Name: D_APRX.py
#
# Purpose: Copies the ArcGIS Pro project (APRX) @ ngce/pmdm/d/APRX/D_Base.aprx and inserts the follwing files:
# las file, results of D03, and a fishnet filtered by the "data domain" created in D01.
#
# Notes: A simple task is included in the ArcGIS Pro project that will help the end user set up a Map Series.
#
# Author: jeff8977

from ngce.pmdm.d.D_Config import *
import arcpy
import time
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


def project_set(base_dir, project_id):

    print('Creating Copy of Base APRX File')

    base_aprx = arcpy.mp.ArcGISProject(BASE_APRX)
    new_aprx = os.path.join(base_dir, project_id + '.aprx')
    base_aprx.saveACopy(new_aprx)
    aprx = arcpy.mp.ArcGISProject(new_aprx)

    return aprx


def filter_fishnet(data_domain, base_dir, fishnet):

    print('Filtering Fishnet & Collecting Task Extents')

    # Create Path for Fishnet Results
    fishnet_path = os.path.join(base_dir, 'FISHNET')
    os.mkdir(fishnet_path)

    # Filter Fishnet By Data Domain
    layer = arcpy.MakeFeatureLayer_management(fishnet, 'fishnet_layer')
    sel = arcpy.SelectLayerByLocation_management(layer, 'INTERSECT', data_domain)
    filter_fish = arcpy.CopyFeatures_management(sel, os.path.join(fishnet_path, 'filter_fishnet.shp'))

    return filter_fish


if __name__ == '__main__':

    # Get Script Start Time
    start = time.time()

    try:
        # Collect Job ID from Command Line
        job_id = '808'

        # Collect Script Inputs from Table
        inputs = collect_table_inputs(job_id)
        project_id, project_dir = inputs[0], inputs[1]

        # Set Path For APRX
        derived_dir = os.path.join(project_dir, DERIVED)
        base_dir = os.path.join(derived_dir, 'Editing')
        os.mkdir(base_dir)

        # Return Map Object
        aprx = project_set(base_dir, project_id)
        base_map = aprx.listMaps('*')[0]

        # Reference LASD
        target_lasd = os.path.join(derived_dir, project_id + '.lasd')

        # Reference D01 Fishnet
        d01_fishnet = os.path.join(derived_dir, D01, 'FISHNET', 'fishnet.shp')

        # Reference D03 Output
        d03_output = os.path.join(derived_dir, D03, 'RESULTS', D03_FINAL)

        # Reference Data Domain For Filtering Fishnet
        data_domain = os.path.join(derived_dir, D01, 'RESULTS', D01_DATA_DOMAIN)

        # Filter Fishnet
        filtered_fishnet = filter_fishnet(data_domain, base_dir, d01_fishnet)

        # Add Inputs To Map
        base_map.addDataFromPath(target_lasd)
        base_map.addDataFromPath(filtered_fishnet)
        base_map.addDataFromPath(d03_output)

        # Save APRX
        aprx.save()

    except Exception as e:
        print('Exception: ', e)

    print('Program Ran: {0}'.format(time.time() - start))