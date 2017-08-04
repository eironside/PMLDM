def RevalueRaster(OutputFolder, curr_raster, minZ, maxZ, elevation_type, rows, ProjectID, ProjectUID):

    # Create a Raster object to gain access to the properties of the raster
    rasterObject = arcpy.Raster(curr_raster)  
    rasterObjectFormat = rasterObject.format
    rasterObjectBands = rasterObject.bandCount
    rasterObjectPixelType = rasterObject.pixelType
    rasterObjectPath = rasterObject.catalogPath
    # rasterObjectNoData = rasterObject.noDataValue
    # RasterNoDataValue = str(rasterObject.noDataValue).strip().upper()
    
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
        
    if SRFactoryCode <= 0:
        # TODO set an error in the DB ?
        arcpy.AddWarning("WARNING: One or more rasters didn't have a SR set".format(InputFolder))
    arcpy.AddMessage("\nOperation Complete, output Rasters can be found in: {}".format(OutputFolder))
