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

        # Reference D03 Output
        d03_output = os.path.join(derived_dir, D03, 'RESULTS', D03_FINAL)

        # Reference D01 Fishnet
        d01_fishnet = os.path.join(derived_dir, D01, 'FISHNET', 'fishnet.shp')

        # Add Inputs To Map
        base_map.addDataFromPath(target_lasd)
        base_map.addDataFromPath(d01_fishnet)
        base_map.addDataFromPath(d03_output)

        # Save APRX
        aprx.save()

    except Exception as e:
        print('Exception: ', e)

    print('Program Ran: {0}'.format(time.time() - start))