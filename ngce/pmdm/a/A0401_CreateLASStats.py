'''
Created on Jun 15, 2017

@author: eric5946
'''
import arcpy
import os
import sys
import  datetime


if __name__ == '__main__':
    spatial_reference = None
    if len(sys.argv) >= 2:
        f_path = sys.argv[1]
        arcpy.AddMessage(f_path)
    if len(sys.argv) >= 3:
        spatial_reference = sys.argv[2]
        arcpy.AddMessage(spatial_reference)
    
    
    if f_path is not None:
        lasd_path = "{}d".format(f_path)
        a = datetime.datetime.now()
##        arcpy.AddMessage("Calling CreateLasDataset on {} to {} with SR {}".format(f_path, lasd_path, spatial_reference))
        arcpy.CreateLasDataset_management(input=f_path,
                                          spatial_reference=spatial_reference,
                                          out_las_dataset=lasd_path,
                                          folder_recursion="NO_RECURSION",
                                          in_surface_constraints="",
                                          compute_stats="COMPUTE_STATS",
                                          relative_paths="RELATIVE_PATHS")
        b = datetime.datetime.now()
        td = (b - a).total_seconds()
        arcpy.AddMessage("Completed {} in {}".format(lasd_path, td))
        os.remove(lasd_path)

