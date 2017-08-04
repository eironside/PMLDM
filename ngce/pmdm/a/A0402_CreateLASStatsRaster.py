'''
Created on Jun 15, 2017

@author: eric5946
'''
import arcpy
import datetime
import os
import sys


if __name__ == '__main__':
    method = None
    out_raster = None
    lasd = None
    sampl_type = None
    cellSize = None
    out_folder = None
    name = None

    if len(sys.argv) >= 2:
        method = sys.argv[1]
        arcpy.AddMessage(method)
    if len(sys.argv) >= 3:
        out_raster = sys.argv[2]
        arcpy.AddMessage(out_raster)
    if len(sys.argv) >= 4:
        lasd = sys.argv[3]
        arcpy.AddMessage(lasd)
    if len(sys.argv) >= 5:
        sampl_type = sys.argv[4]
        arcpy.AddMessage(sampl_type)
    if len(sys.argv) >= 6:
        cellSize = sys.argv[5]
        arcpy.AddMessage(cellSize)
    if len(sys.argv) >= 7:
        out_folder = sys.argv[6]
        arcpy.AddMessage(out_folder)
    if len(sys.argv) >= 8:
        name = sys.argv[7]
        arcpy.AddMessage(name)


    
    if name == "_LAST":
        lasd = arcpy.MakeLasDatasetLayer_management(in_las_dataset=lasd, out_layer="LasDataset_last", class_code="0;2;8;9;10;11;12", return_values="'Last Return'", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
    elif name == "_FIRST":
        lasd = arcpy.MakeLasDatasetLayer_management(in_las_dataset=lasd, out_layer="LasDataset_first", class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="1", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
    elif name == '_ALL':
        lasd = arcpy.MakeLasDatasetLayer_management(in_las_dataset=lasd, out_layer="LasDataset_All", class_code="0;1;2;3;4;5;6;8;9;10;11;12;13;14;15;16;17", return_values="'Last Return';'First of Many';'Last of Many';'Single Return';1;2;3;4;5", no_flag="true", synthetic="true", keypoint="true", withheld="false", surface_constraints="")
        
        
    
    a = datetime.datetime.now()
    arcpy.LasPointStatsAsRaster_management(lasd, out_raster, method, sampl_type, cellSize)
        
    if ((method is "PULSE_COUNT") or (method is "POINT_COUNT")):
        # divide the cells by cell size squared, overwrite
        out_raster1 = os.path.join(out_folder, method + "1.tif")
        os.rename(out_raster, out_raster1)
        ras1 = arcpy.Raster(out_raster1)
        ras = ras1 / (cellSize ^ 2)
        del ras1
        ras.save(out_raster)
        arcpy.Delete_management(out_raster1)
        del ras
    
    b = datetime.datetime.now()
    td = (b - a).total_seconds()
    arcpy.AddMessage("\tCreate LAS Stat Raster: Completed {} in {}".format(out_raster, td))
