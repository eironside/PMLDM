from ngce.Utility import SDE_CMDR_FILE_PATH
import arcpy
import time
import os


def copy_metadata(input_db, sde_conn):

    # Expected Paramters
    targets = ['FootprintRaster', 'BoundaryRaster', 'BoundaryLASDataset', 'FootprintLASFile']
    proj_id = os.path.basename(input_db)[:-4]

    # Logging
    arcpy.AddMessage('Running A05_D_UpdateCMDRMetaData')
    arcpy.AddMessage('Input DB: {}'.format(input_db))
    arcpy.AddMessage('SDE File: {}'.format(sde_conn))
    arcpy.AddMessage('Proj ID:  {}'.format(proj_id))

    # Move Features
    for target in targets:
 
        sde_fc = os.path.join(sde_conn, 'LDM_CMDR.DBO.' + target)
        der_fc = os.path.join(input_db, target)

        if not arcpy.Exists(sde_fc) or not arcpy.Exists(der_fc):
            arcpy.AddMessage('Target Feature Class Not Found In Both Databases: {}'.format(target))
            continue
        else:
            arcpy.AddMessage('Moving: {}'.format(target))
            arcpy.AddMessage('Records: {}'.format(arcpy.GetCount_management(der_fc)))

            # Describe FC Fields To Mitigate Schema Conflicts
            der_desc     = arcpy.Describe(der_fc)
            sde_desc     = arcpy.Describe(sde_fc)
            der_fields   = [f.name for f in der_desc.fields if f.name != der_desc.OIDFieldName if f.name != 'Shape']
            sde_fields   = [f.name for f in sde_desc.fields if f.name != sde_desc.OIDFieldName if f.name != 'Shape']
            handled      = list(set(der_fields) & set(sde_fields))

            # Add Fields Needing Specific Attention To End For Access
            if target in ['FootprintRaster', 'BoundaryRaster']:
                der_handled  = handled + ['area', 'nodata', 'SHAPE@']
                sde_handled  = handled + ['src_area', 'nodata', 'SHAPE@']
            else:
                der_handled  = handled + ['area', 'SHAPE@']
                sde_handled  = handled + ['src_area', 'SHAPE@']

            # Remove Existing Records                     
            with arcpy.da.UpdateCursor(sde_fc, ['OBJECTID'], "Project_ID = '{}'".format(proj_id)) as u_cursor:
                arcpy.AddMessage('Deleting Existing Records')
                for row in u_cursor:
                    u_cursor.deleteRow()

            # Insert Incoming Records
            with arcpy.da.SearchCursor(der_fc, der_handled) as s_cursor:
                with arcpy.da.InsertCursor(sde_fc, sde_handled) as i_cursor:
                    
                    arcpy.AddMessage('Inserting Incoming Records')
                    for row in s_cursor:
                        
                        # Pull Centroid If Working With Footprints
                        if target in ['FootprintRaster', 'FootprintLASFile']:
                            centroid = row[-1].projectAs(arcpy.SpatialReference(3857)).centroid
                            geom = arcpy.PointGeometry(arcpy.Point(centroid.X, centroid.Y))
                        else:
                            geom = row[-1].projectAs(arcpy.SpatialReference(3857))

                        # Convert No Data to String & Insert Row
                        if target in ['FootprintRaster', 'BoundaryRaster']:
                            no_d = str(row[-2])
                            i_cursor.insertRow(row[:-2] + (no_d, geom,))    
                        else:
                            i_cursor.insertRow(row[:-1] + (geom,))        
            

if __name__ == "__main__":

    # Testing Parameter
    input_db = r'\\aiotxftw3fp007\e\WMX\MI_Cass_2015_published\DERIVED\MI_Cass_2015.gdb'

    # Copy Content
    print('Running A05_D_UpdateCMDRMetaData')
    copy_metadata(input_db, SDE_CMDR_FILE_PATH)
