'''
Created on Dec 9, 2015

@author: eric5946

'''
#-------------------------------------------------------------------------------
# Name:        NRCS_RemoveErrantValues
#
# Purpose:     To remove any errant values in raster data.
#              This conversion is performed
#              in preparation for ingestion into a Mosaic Dataset. 
#
#              
#              This GP tool executes the "Con" Spatial Analyst tool
#               to effectively eliminate very low values which
#               have difficulty rendering in ArcGIS
#
# Author:       Roslyn Dunn
# Organization: Esri Inc.
#
#
# Created:     06/25/2015
# Modified     09/24/2015   Take two values of NoData into account
#                           Get threshold values from UI
#
# *
#-------------------------------------------------------------------------------

import arcpy
import os

from ngce import Utility
from ngce.cmdr import CMDR, CMDRConfig
from ngce.cmdr.CMDRConfig import DSM, DTM, \
    fields_RasterFileStat, field_RasterFileStat_ProjID, \
    field_RasterFileStat_Name, field_RasterFileStat_ElevType, \
    field_RasterFileStat_Group
from ngce.folders import ProjectFolders
from ngce.folders.FoldersConfig import derived_dir, delivered_dir, published_dir
from ngce.las.LAS import validateZRange
from ngce.raster import RasterConfig 
from ngce.raster.Raster import getRasterSpatialReferenceInfo, getRasterStats


Utility.setArcpyEnv(True)


def RevalueRaster(OutputFolder, curr_raster, minZ, maxZ, elevation_type, rows, ProjectID, ProjectUID):

    # Create a Raster object to gain access to the properties of the raster
    rasterObject = arcpy.Raster(curr_raster)  
    rasterObjectFormat = rasterObject.format
    rasterObjectBands = rasterObject.bandCount
    rasterObjectPixelType = rasterObject.pixelType
    rasterObjectPath = rasterObject.catalogPath
    # rasterObjectNoData = rasterObject.noDataValue
    # RasterNoDataValue = str(rasterObject.noDataValue).strip().upper()
    horz_cs_name, horz_unit_name, horz_cs_wkid, vert_cs_name, vert_unit_name = getRasterSpatialReferenceInfo(rasterObjectPath)
    
    # Used for DEBUG only
    #     arcpy.AddMessage("Raster {} path = '{}'".format(curr_raster, rasterObjectPath))
    #     arcpy.AddMessage("Raster {} format = '{}'".format(curr_raster, rasterObjectFormat))
    #     arcpy.AddMessage("Raster {} bands = '{}'".format(curr_raster, rasterObjectBands))
    #     arcpy.AddMessage("Raster {} pixel type = '{}'".format(curr_raster, rasterObjectPixelType))
    #     arcpy.AddMessage("Raster {} no data value = '{}'".format(curr_raster, RasterNoDataValue))
    #     arcpy.AddMessage("Raster {} Spatial Ref Code = '{}'".format(curr_raster, horz_cs_wkid))
    
    # Set the input raster NoData value to standard: -3.40282346639e+038 
    # This is needed because sometimes the raster's meta data for the NoData value hasn't been set, causing extreme negative elevation values
    nodata = RasterConfig.NODATA_DEFAULT  # NODATA_340282346639E38
    # REMOVED BY EI 20160210: Assume standard no-data value until proven otherwise
#     if RasterNoDataValue == NODATA_340282306074E38:
#         nodata = NODATA_340282306074E38
    arcpy.AddMessage("Raster {} setting no data value to '{}'".format(curr_raster, nodata))
    
    # Set the no data default value on the input raster    
    arcpy.SetRasterProperties_management(rasterObjectPath, data_type="#", statistics="#", stats_file="#", nodata="1 {}".format(nodata))
    
    cellSize = getRasterStats(ProjectUID, ProjectID, curr_raster, rasterObjectPath, delivered_dir, elevation_type, rasterObjectFormat, rasterObjectPixelType, nodata, horz_cs_name, horz_unit_name, horz_cs_wkid, vert_cs_name, vert_unit_name, rows)    
        
    if rasterObjectBands == 1:
        if rasterObjectPixelType == "F32":
            # REMOVED BY EI 20160210: We're only interested in Checking IMG, Esri GRID, Esri File GDB, and TIFF files for errant values
            # if rasterObjectFormat == "TIFF" or rasterObjectFormat == "GRID" or rasterObjectFormat == "IMAGINE Image" or rasterObjectFormat == "FGDBR":
                outputRaster = os.path.join(OutputFolder, curr_raster)
                # Don't maintain fGDB raster format, update to TIFF
                if rasterObjectFormat == "FGDBR":
                    outputRaster += ".TIF"
                
                if not arcpy.Exists(outputRaster):
                    # Compression isn't being applied properly so results are uncompressed
                    outSetNull = arcpy.sa.Con(((rasterObject >= minZ) & (rasterObject <= maxZ)), curr_raster)  # @UndefinedVariable
                    outSetNull.save(outputRaster)
                    arcpy.AddMessage("Raster '{}' copied to '{}' with valid values between {} and {}".format(rasterObjectPath, outputRaster, minZ, maxZ))
                    
                    getRasterStats(ProjectUID, ProjectID, curr_raster, outputRaster, derived_dir, elevation_type, rasterObjectFormat, rasterObjectPixelType, nodata, horz_cs_name, horz_unit_name, horz_cs_wkid, vert_cs_name, vert_unit_name, rows)
                    del outSetNull
                    del rasterObject
                else:
                    arcpy.AddMessage("Skipping Raster {}, output raster already exists {}".format(curr_raster, outputRaster))
            # else: # REMOVED BY EI
            #    arcpy.AddMessage("Skipping Raster {}, not file type TIFF, GRID, IMAGINE, or FGDBR image.".format(curr_raster))
        else:
            arcpy.AddMessage("Skipping Raster {}, not Float32 type image.".format(curr_raster))
    else:
        arcpy.AddMessage("Skipping Raster {}, not 1 band image.".format(curr_raster))
    if horz_cs_wkid <= 0:
        arcpy.AddWarning("Raster {} has a PCSCode (EPSG code) of 0 as well as GCSCode = 0 which indicates a non-standard datum or unit of measure.".format(curr_raster))
    return horz_cs_wkid, cellSize


def processRastersInFolder(minZ, maxZ, InputFolder, OutputFolder, elevation_type, rows, ProjectID, ProjectUID):
    count = 0
    cellSize = 0
    if os.path.exists(InputFolder):
        SRFactoryCodeFlag = 1
        arcpy.env.workspace = InputFolder
        current_raster_list = arcpy.ListRasters("*", "ALL")
        if current_raster_list is not None and len(current_raster_list) > 0:
            Utility.clearFolder(OutputFolder);
            
            for curr_raster in current_raster_list:
                SRFactoryCode, cellSize = RevalueRaster(OutputFolder, curr_raster, minZ, maxZ, elevation_type, rows, ProjectID, ProjectUID)
                count = count + 1
                if SRFactoryCode <= 0:
                    SRFactoryCodeFlag = 0
                del curr_raster
            
            arcpy.AddMessage("\nOperation Complete, output Rasters can be found in: {}".format(OutputFolder))
        else:
            arcpy.AddMessage("No rasters found at '{}'".format(InputFolder))
        if SRFactoryCodeFlag == 0:
            # TODO set an error in the DB ?
            arcpy.AddWarning("WARNING: One or more rasters didn't have a SR set".format(InputFolder)) 
    else:
        arcpy.AddMessage("Input path does not exist '{}'".format(InputFolder))

    return count, cellSize




# jobID = arcpy.GetParameterAsText(0)
# jobID = 16402

def RemoveDEMErrantValues(jobID):
    Utility.printArguments(["WMX Job ID"], [jobID], "A05 RemoveDEMErrantValues")
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")
    
    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
    
    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)
    
    if project is not None:
        ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
        ProjectID = ProjectJob.getProjectID(project)
    
        Deliver = CMDR.Deliver()
        delivery = list(Deliver.getDeliver(ProjectID))
        
        RasterFileStat = CMDR.RasterFileStat()
        
        minZ = Deliver.getValidZMin(delivery)
        maxZ = Deliver.getValidZMax(delivery)
        
        minZ, maxZ = validateZRange(minZ, maxZ)
        
        source_path = ProjectFolder.delivered.path
        target_path = ProjectFolder.derived.path
        publish_path = ProjectFolder.published.path
        
        rows = []
        TotalCount = 0
        targetFolders = [DSM, DTM]
        for targetFolder in targetFolders:
            localrows = []
            InputFolder = os.path.join(source_path, targetFolder)
            OutputFolder = os.path.join(target_path, targetFolder)
            PublishFolder = os.path.join(publish_path, targetFolder)
            
            count, cellSize = processRastersInFolder(minZ, maxZ, InputFolder, OutputFolder, targetFolder, localrows, ProjectID, ProjectUID)
            
            Raster_Files = []
            for row in localrows:
                rows.append(row)
                if RasterFileStat.getGroup(row) == delivered_dir:
                    Raster_Files.append(RasterFileStat.getPath(row))
                    newRow = list(row)
                    RasterFileStat.setGroup(newRow, published_dir)
                    RasterFileStat.setFormat(newRow, "TIFF")
                    RasterFileStat.setPath(newRow, os.path.join(target_path, publish_path, RasterFileStat.getName(newRow)))
                    rows.append(newRow)
            
            
            if len(Raster_Files) > 0:
                Utility.clearFolder(PublishFolder);
                    
                Raster_Files = ";".join(Raster_Files)
                arcpy.RasterToOtherFormat_conversion(Raster_Files, PublishFolder, Raster_Format="TIFF")
                Utility.addToolMessages()
            
            Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
            TotalCount = TotalCount + count
            arcpy.AddMessage("Processed {} rasters in '{}'".format(count, InputFolder))
            
            if targetFolder == DSM:
                Deliver.setDSMCellResolution(delivery, cellSize)
                Deliver.setDSMCountRaster(delivery, count)
                Deliver.setDSMExists(delivery, "No")
                if count > 0:
                    Deliver.setDSMExists(delivery, "Yes")
            elif targetFolder == DTM:
                Deliver.setDTMCellResolution(delivery, cellSize)
                Deliver.setDTMCountRaster(delivery, count)
                Deliver.setDTMExists(delivery, "No")
                if count > 0:
                    Deliver.setDTMExists(delivery, "Yes")
        
        Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
        Deliver.setCountRasterFiles(delivery, TotalCount)
        Deliver.updateDeliver(delivery, ProjectID)
        
        
        arcpy.AddMessage("Saving raster property rows")
        for row in rows:
            arcpy.AddMessage("Saving {}".format(row))
                         
            RasterFileStat.saveOrUpdateRasterFileStat(row,
                                                      row[fields_RasterFileStat.index(field_RasterFileStat_ProjID)],
                                                      row[fields_RasterFileStat.index(field_RasterFileStat_Name)],
                                                      row[fields_RasterFileStat.index(field_RasterFileStat_ElevType)],
                                                      row[fields_RasterFileStat.index(field_RasterFileStat_Group)])
        
        fgdb_path = ProjectFolder.derived.fgdb_path
        if not arcpy.Exists(fgdb_path):
            arcpy.AddMessage("creating fGDB '{}'".format(fgdb_path))
            arcpy.CreateFileGDB_management(ProjectFolder.derived.path, ProjectFolder.derived.fgdb_name)
            Utility.addToolMessages()
        rasterFileStat_path = os.path.join(fgdb_path, CMDRConfig.fcName_RasterFileStat)
        if arcpy.Exists(rasterFileStat_path):
            arcpy.Delete_management(rasterFileStat_path)
            Utility.addToolMessages()
        if not arcpy.Exists(rasterFileStat_path):
            arcpy.AddMessage("creating feature class '{}' '{}' ".format(fgdb_path, CMDRConfig.fcName_RasterFileStat))
            sr = arcpy.Describe(RasterFileStat.fclass).spatialReference
            arcpy.AddMessage("using spatial reference '{}'".format(sr))
            arcpy.CreateFeatureclass_management(out_path=fgdb_path, out_name=CMDRConfig.fcName_RasterFileStat, template=RasterFileStat.fclass, geometry_type="POLYGON", spatial_reference=sr)
#             desc = arcpy.Describe(RasterFileStat.fclass)
#             fieldListComplete = desc.fields
#             # limit field list to all fields except OBJECT_ID
#             fieldList = fieldListComplete[1:]
#             # create fields in the output feature class
#             for i in fieldList:
#                 arcpy.AddField_management(rasterFileStat_path, i.name, i.type, "", "", i.length)
#                 Utility.addToolMessages()
#         edit = Utility.startEditingSession()
        cursor_i = arcpy.da.InsertCursor(rasterFileStat_path, CMDRConfig.fields_RasterFileStat)  # @UndefinedVariable
        for row in rows:
            arcpy.AddMessage("Saving {}".format(row))
            cursor_i.insertRow(row)
            arcpy.AddMessage("Updated {} record: {}".format(rasterFileStat_path, row))
#         Utility.stopEditingSession(edit)
        del cursor_i
        
    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")
    arcpy.AddMessage("Operation complete")


if __name__ == '__main__':
    RemoveDEMErrantValues(16402)
