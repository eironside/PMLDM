import arcpy
import time
import sys
import os


def collect_table_inputs(j_id):

    print('Collecting Inputs From Database Table')

    j_id = int(j_id)

    table = r'C:\Users\jeff8977\Desktop\USDA\CMDR.gdb\ProjectJob'

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


def handle_rasters(lasd, data_domain, base_dir, d04_output):

    print('Generating Rasters From D04 Fishnet')

    # Create Path for Output Rasters
    raster_path = os.path.join(base_dir, 'RASTER')
    os.mkdir(raster_path)

    # Create Path for Other Results
    fishnet_path = os.path.join(base_dir, 'FISHNET')
    os.mkdir(fishnet_path)

    # Filter D04 Fishnet By Data Domain
    layer = arcpy.MakeFeatureLayer_management(d04_output, 'd04_layer')
    sel = arcpy.SelectLayerByLocation_management(layer, 'INTERSECT', data_domain)
    filter_fishnet = arcpy.CopyFeatures_management(sel, os.path.join(fishnet_path, 'filter_fishnet.shp'))

    # Get Count of Filtered Fishnet
    cells = arcpy.GetCount_management(filter_fishnet)

    # Process Rasters From Filtered Fishnet Extents
    proc_count = 0
    fail_count = 0
    tri_count = 0
    reg_count = 0
    for r in arcpy.da.SearchCursor(filter_fishnet, ['FID', 'SHAPE@']):
        ext = r[1].extent
        arcpy.env.extent = arcpy.Extent(ext.XMin, ext.YMin, ext.XMax, ext.YMax)
        out_raster = os.path.join(raster_path, str(r[0]) + '.tif')
        try:
            arcpy.LasDatasetToRaster_conversion(
                lasd,
                out_raster,
                'ELEVATION',
                'TRIANGULATION LINEAR NO_THINNING MINIMUM 0',
                'FLOAT',
                'CELLSIZE',
                1.0
            )
            tri_count += 1


        except Exception as e:
            print('Exception: ', e)
            print('Attempting To Generate Raster Without Interpolation Parameter')

            try:
                arcpy.LasDatasetToRaster_conversion(
                    lasd,
                    out_raster,
                    'ELEVATION',
                    '#',
                    'FLOAT',
                    'CELLSIZE',
                    1.0
                )
                reg_count += 1

            except Exception as e:
                print('Exception: ', e)
                print('Unable to Generate Raster For Fishnet Cell: ', str(r[0]))
                fail_count += 1

        finally:
            proc_count += 1

    # Admin Logging
    print('Total: ', cells)
    print('Processed: ',  proc_count)
    print('Failed: ', fail_count)
    print('Interpolated: ', tri_count)
    print('Not Interpolated: ', reg_count)


if __name__ == '__main__':

    # Get Script Start Time
    start = time.time()

    try:
        # Collect Job ID from Command Line
        job_id = sys.argv[1]

        # Collect Script Inputs from SDE Table
        inputs = collect_table_inputs(job_id)
        project_id, project_dir = inputs[0], inputs[1]

        # Create Target Inputs
        derived_dir = os.path.join(project_dir, 'DERIVED')
        base_dir = os.path.join(derived_dir, 'D05')
        os.mkdir(base_dir)

        # Reference D04 Fishnet Output
        d04_output = os.path.join(derived_dir, 'D04', 'FISHNET', 'fishnet_clip.shp')

        # Reference LASD
        target_lasd = os.path.join(derived_dir, project_id + '.lasd')

        # Data Domain For Fishnet Parse
        data_domain = os.path.join(derived_dir, 'D01', 'RESULTS', 'data_domain.shp')

        # Generate Rasters
        handle_rasters(target_lasd, data_domain, base_dir, d04_output)

    except Exception as e:
        print('Exception', e)

    finally:
        print('Program Ran: {0}'.format(time.time() - start))

