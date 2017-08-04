#-------------------------------------------------------------------------------
# Name:        gp_processContoursFromMD_parallel
#
# Purpose:     To generate contours from elevation raster datasets
#
#              This tool is very similar to gp_processContoursFromMD except that
#              it utilizes multiprocessing to spawn a process (GenerateContour) which
#              will generate contours from one raster dataset. The tool is not initiated
#              from the toolbox UI, but instead is designed to be run out of process
#              at the command line using 64-bit python. Run this tool if
#              experiencing memory allocation issues (leaks) with gp_processContoursFromMD.
#
#              To create multiple contours at once, some modifications will need to be made
#                to this script to separate each contour dataset into separate
#                workspaces (to avoid multiple writers to one file gdb).
#
#              Note: GenerateContour (defined below) was already written. This tool
#                wraps that routine in a process that creates a Mosaic Dataset from the
#                rasters, then iterates through each (buffered) footprint to create
#                contours that overlap only slightly after they are clipped. This process
#                makes the contours all appear continuous.
#
# Author:       Roslyn Dunn
# Organization: Esri Inc.
#
# Created:     06/25/2015
# Modified     09/24/2015   del as many references as possible
#                           Allow for initiation at the command line
#                           Use python multiprocessing to spawn GenerateContour (and release
#                              allocated memory each time).
#
# *
#-------------------------------------------------------------------------------



import arcpy
from arcpy.sa import Functions
import multiprocessing
from multiprocessing.process import Process
import os
import sys, time

import arcpy.cartography as ca
from ngce import Utility
from ngce.cmdr import CMDR
from ngce.contour import ContourConfig
from ngce.folders import ProjectFolders
from ngce.raster import RasterConfig


arcpy.env.overwriteOutput = True



def GenerateContour(mapid, dem, vertUnits, theInterval, userUnits, SmoothTolerance, outputShapefile, scratchWorkspace, removeDepressions, clipToBoundary, do_areas=False, nhd_file=""):

        # when invoking this method from python multiprocessing, import arcpy and other site packages,
        # and then check out required Extensions
        
        
        arcpy.CheckOutExtension("3D")
        arcpy.CheckOutExtension("Spatial")

        if not os.path.exists(scratchWorkspace):
            os.mkdir(scratchWorkspace)
            arcpy.AddMessage("\n creating scratchWorkspace: {0}".format(scratchWorkspace))

        arcpy.env.workspace = scratchWorkspace
        arcpy.env.scratchWorkspace = scratchWorkspace

        time0 = time.time()
        pid = str(os.getpid())
        # message = "Set TEMP and TMP variables to " + arcpy.env.workspace
#         arcpy.AddMessage("\nSet TEMP and TMP variables to " + arcpy.env.workspace)

#         arcpy.AddMessage("\n arcpy.env.workspace: {0}".format(arcpy.env.workspace))
        db = arcpy.env.workspace + "\\working.gdb\\"  # @UndefinedVariable
        if not arcpy.Exists(db):
            arcpy.AddMessage("creating: {0}".format(db))
            arcpy.CreateFileGDB_management(arcpy.env.workspace, "working.gdb")  # @UndefinedVariable
        else:
            arcpy.AddMessage("compacting: {0}".format(db))
            arcpy.Compact_management(db)

        tempEnvironment0 = arcpy.env.outputZFlag  # @UndefinedVariable
        arcpy.env.outputZFlag = "Disabled"
        tempEnvironment1 = arcpy.env.outputZValue  # @UndefinedVariable
        arcpy.env.outputZValue = "0"
        arcpy.RasterDomain_3d(dem, db + "raster_domain" + mapid, "POLYGON")
        Utility.addToolMessages()
        arcpy.env.outputZFlag = tempEnvironment0
        arcpy.env.outputZValue = tempEnvironment1
        
        # Commented this out - want output to be in CS of input rasters RD 06/17/2015
        # #arcpy.outputCoordinateSystem = "GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]"

        desc = arcpy.Describe(dem)
        thisDEMResolution = desc.MeanCellWidth
        # print thisDEMResolution

        SpatRef = desc.SpatialReference
        SpatRefLinearUnitName = SpatRef.linearUnitName.upper()
        arcpy.AddMessage("Spatial Reference name Raster:  {0}".format(SpatRef.name))
        arcpy.AddMessage("Spatial Reference X,Y Units of Mosaic Dataset: {0}".format(SpatRefLinearUnitName)) 
        del desc, SpatRef
        
        arcpy.AddMessage("Cellsize of input dem = " + str(thisDEMResolution) + ". Specified interval = " + str(theInterval) + ". Specified units = " + userUnits)
        arcpy.AddMessage("Generating the contours...")
        arcpy.AddMessage("userUnits = " + userUnits)
        arcpy.AddMessage("theInterval = " + str(theInterval))

# #    #DO WE NEED THIS?
# #    if 1 != 1:
# #        print "!!!Range of Z values is less than 1, which may cause problems, skipping this quad.!!!"
# #        #raise Exception("!!!Range of Z values is less than 1, which may cause problems, skipping this quad.!!!")
# #        return
# #    else:

        
        arcpy.AddMessage("Filtering...")
        outFS = Functions.FocalStatistics(dem, "Rectangle 3 3 CELL", "MEAN", "DATA")
        Utility.addToolMessages()
        # outFS.save("in_memory/tempdem1")
        outTimes = Functions.Times(outFS, 100)
        Utility.addToolMessages()
        # outFS.save("in_memory/tempdem1")
        del outFS
        # outTimes.save("in_memory/tempdem2")
        outPlus = Functions.Plus(outTimes, 0.5)
        Utility.addToolMessages()
        # outFS.save("in_memory/tempdem1")
        del outTimes
        # outPlus.save("in_memory/tempdem3")
        outRoundDown = Functions.RoundDown(outPlus)
        Utility.addToolMessages()
        # outFS.save("in_memory/tempdem1")
        del outPlus
        # outRoundDown.save("in_memory/tempdem4")
        outDivide = Functions.Divide(outRoundDown, 100)  # prevents conflict with temporary rasters on multiple core servers.
        Utility.addToolMessages()
        # outFS.save("in_memory/tempdem1")
        del outRoundDown
        # outDivide.save("in_memory/tempdem5")
        outFS2 = Functions.FocalStatistics(outDivide, "Rectangle 3 3 CELL", "MEAN", "DATA")
        Utility.addToolMessages()
        # outFS.save("in_memory/tempdem1")
        del outDivide
        # gp.Delete_management("in_memory/tempdem5")
        # outFS2.save("in_memory/tempdem6")
        
        ####################################################################################
        ######### updated this logic 5/27/2015 to accomodate raster elevation units ########
        ####################################################################################

        userUnits = userUnits.upper()
        vertUnits = vertUnits.upper()
        if (userUnits == "MT" or userUnits == "METERS") and (vertUnits == "MT" or vertUnits == "METER"):
            outDivide2 = Functions.Divide(outFS2, 1)
            Utility.addToolMessages()
            # outDivide2.save("in_memory/elevfilt")
        elif (userUnits == "MT" or userUnits == "METERS") and (vertUnits == "FOOT_US"):
            outDivide2 = Functions.Times(outFS2, 0.30480061)
            Utility.addToolMessages()
        elif (userUnits == "MT" or userUnits == "METERS") and (vertUnits == "FOOT_INTL"):
            outDivide2 = Functions.Times(outFS2, 0.3048)
            Utility.addToolMessages()
        elif (userUnits == "FT" or userUnits == "FEET") and (vertUnits == "MT" or vertUnits == "METER"):
            outDivide2 = Functions.Divide(outFS2, 0.3048)
            Utility.addToolMessages()
        elif (userUnits == "FT" or userUnits == "FEET") and (vertUnits == "FOOT_US" or vertUnits == "FOOT_INTL"):
            outDivide2 = Functions.Divide(outFS2, 1)
            Utility.addToolMessages()
            # outDivide2.save("in_memory/elevfilt")
        else:
            arcpy.AddMessage("\nuserUnits: {}, vertUnits: {}".format(userUnits, vertUnits))
            raise Exception("Units not valid")
            arcpy.AddError('\nUnable to create contours.')
        del outFS2
        
        # Run contour
        cona1x = os.path.join(db, "cona1x")
        if arcpy.Exists(cona1x):
            arcpy.Delete_management(cona1x)
        cona1x = Functions.Contour(outDivide2, cona1x, theInterval)
        Utility.addToolMessages()
        del outDivide2
        
        # FOR NED
        # print "Simplifying Line..."
        
        cona1y = os.path.join(db, "cona1y")
        if arcpy.Exists(cona1y):
            arcpy.Delete_management(cona1y)
        cona1y = ca.SimplifyLine(cona1x, cona1y, "POINT_REMOVE", "0.000001 DecimalDegrees", "FLAG_ERRORS", "NO_KEEP", "NO_CHECK")
        Utility.addToolMessages()

    # #       gp.CopyFeatures_management("in_memory/cona1y", scratchWorkspace + "\\cona1y.shp")

        if arcpy.Exists(cona1x):
            arcpy.Delete_management(cona1x)
        del cona1x

        # print "Smoothing Line..."
        # Smooth TOlerance "0.0001 DecimalDegrees"
        cona1z = os.path.join(db, "cona1z")
        if arcpy.Exists(cona1z):
            arcpy.Delete_management(cona1z)
        cona1z = ca.SmoothLine(cona1y, cona1z, "PAEK", "{} DecimalDegrees".format(SmoothTolerance), "", "NO_CHECK")  # @UndefinedVariable
        Utility.addToolMessages()
        # print "Simplifying Line..."

        if arcpy.Exists(cona1y):
            arcpy.Delete_management(cona1y)
        del cona1y
        
        cona1 = os.path.join(db, "cona1")
        if arcpy.Exists(cona1):
            arcpy.Delete_management(cona1)
        cona1 = ca.SimplifyLine(cona1z, cona1, "POINT_REMOVE", "0.000001 DecimalDegrees", "FLAG_ERRORS", "NO_KEEP", "NO_CHECK")  # @UndefinedVariable
        Utility.addToolMessages()
        
        if arcpy.Exists(cona1z):
            arcpy.Delete_management(cona1z)
        del cona1z
        # added by Joe McGlinchy 8/1/2013
        # index contours
        contRes = 2  # 2ft
        indexInt = contRes * 5  # index interval is 5*contour res

        # add the index field
        arcpy.AddField_management(cona1, "INDEX", "LONG", "", "", "", "", "", "", "")
        Utility.addToolMessages()

        fields = ["INDEX", "Contour"]
        # create update cursor for feature class
        with arcpy.da.UpdateCursor(cona1, fields) as rows:  # @UndefinedVariable

            for row in rows:
                # temp = row.getValue("Contour")
                temp = row[1]
                temp_dif = temp % indexInt

                if (temp_dif == 0):
                    row[0] = 1
                    # row.setValue("INDEX", 1)
                    rows.updateRow(row)
                else:
                    row[0] = 0
                    # row.setValue("INDEX",0)
                    rows.updateRow(row)
            del row

        del rows

        # added by Joe 1/28/2014 to have fields for name of last merged FC and
        # text description of problem, per AB request
        arcpy.AddField_management(cona1, "LastMergedFC", "TEXT", "", "", 100)
        Utility.addToolMessages()
        arcpy.AddField_management(cona1, "ValidationCheck", "TEXT", "", "", 100)
        Utility.addToolMessages()
        
        # added by Joe 3/4/2014 to have a field specifying unit of measurement for contour interval
        # just grabbed from input parameters
        arcpy.AddField_management(cona1, "UNITS", "TEXT", "", "", 100)
        Utility.addToolMessages()
        expression = '"' + userUnits + '"'
        arcpy.CalculateField_management(cona1, "UNITS", expression, "PYTHON")
        Utility.addToolMessages()

        if do_areas:
            # print "in HD Analysis"
            if arcpy.Exists(nhd_file):
                arcpy.Buffer_analysis(nhd_file, "in_memory/nhd_area_buff", "0.0000925926 DecimalDegrees", "FULL", "ROUND", "ALL")
                Utility.addToolMessages()
            # else:
                # print "NHD File does NOT exist, cannot run Buffer"

            try:
                if arcpy.Exists("in_memory/junkcount"):
                    arcpy.Delete_management("in_memory/junkcount")
                arcpy.Clip_analysis(nhd_file, db + "\\raster_domain" + mapid, "in_memory/junkcount")
                Utility.addToolMessages()
                arcpy.MakeFeatureLayer_management("in_memory/junkcount", "junk_Layer", '"SHAPE_AREA" > 0.00000001')
                Utility.addToolMessages()
                count4 = arcpy.GetCount_management("junk_Layer")
                Utility.addToolMessages()
                arcpy.Delete_management("junk_Layer")
                if count4 > 0:
                    # print 'makefeaturelayer'
                    arcpy.MakeFeatureLayer_management(cona1, "conga_Layer", "", "", "CONTOUR CONTOUR VISIBLE NONE")
                    Utility.addToolMessages()
                    # print 'selectbylocation'
                    arcpy.SelectLayerByLocation_management("conga_Layer", "COMPLETELY_WITHIN", "in_memory/nhd_area_buff", "", "NEW_SELECTION")
                    Utility.addToolMessages()
                    # print 'delete features'
                    arcpy.DeleteFeatures_management("conga_Layer")
                    Utility.addToolMessages()
                    # print 'clip'
                    arcpy.Clip_analysis("conga_Layer", nhd_file, "in_memory/conga_clip", "")
                    Utility.addToolMessages()
                    arcpy.Delete_management("conga_Layer")
                    # print 'multiparttosinglepart'
                    arcpy.MultipartToSinglepart_management("in_memory/conga_clip", "in_memory/conga_clip_sin")
                    Utility.addToolMessages()
                    # print 'simplifyline just inside river'
                    # GREG REMOVED OFFSET DISTANCE
                    # #offsetDistance = str(maxOffset) + " DecimalDegrees"
                    ca.SimplifyLine("in_memory/conga_clip_sin", "in_memory/conga_erased_sim", "POINT_REMOVE", "0.000001 DecimalDegrees", "FLAG_ERRORS", "NO_KEEP", "NO_CHECK")
                    Utility.addToolMessages()
                    # print 'deletefield'
                    arcpy.DeleteField_management("in_memory/conga_erased_sim", "ORIG_FID")
                    Utility.addToolMessages()
                    try:
                        # print 'erase'
                        arcpy.Erase_analysis(cona1, nhd_file, "in_memory/conga_erased")
                        Utility.addToolMessages()
                    except:
                        raise Exception("Problem with Erase")
                        arcpy.AddError('Problem with Erase')
                        # print 'problem with Erase'
                    try:
                        # print 'append'
                        arcpy.Append_management("in_memory/conga_erased_sim", "in_memory/conga_erased", "TEST", "", "")
                        Utility.addToolMessages()
                    except:
                        raise Exception("Problem with Append")
                        arcpy.AddError('Problem with Append')
                else:
                    # print 'else copyfeatures'
                    if arcpy.Exists("in_memory/cona"):
                        arcpy.Delete_management("in_memory/cona")
                    arcpy.CopyFeatures_management(cona1, "in_memory/cona")
                    Utility.addToolMessages()
                    # print arcpy.GetMessages()
            except:
                raise Exception("Problem with straightening lines across streams")
                arcpy.AddError('Problem with straightening lines across streams')

            if not arcpy.Exists("in_memory/cona"):
                arcpy.CopyFeatures_management("in_memory/conga_erased", "in_memory/cona")
                Utility.addToolMessages()



        else:
            arcpy.CopyFeatures_management(cona1, "in_memory/cona")
            Utility.addToolMessages()
            
        if arcpy.Exists(cona1):
            arcpy.Delete_management(cona1)
        del cona1

        result = arcpy.GetCount_management("in_memory/cona")
        Utility.addToolMessages()
        countContours = int(result.getOutput(0))

        # If there were no Contours generated then return (to avoid an error in FeatureToPolygon_management later)
        if countContours == 0:
            arcpy.AddMessage("\nNo Contours created from current row:".format(dem))
            return

        # print "Creating arc/polygon relationship..."
        arcpy.AddField_management("in_memory/cona", "shape_len", "DOUBLE", "", "", "#", "#", "NULLABLE", "NON_REQUIRED", "#")
        Utility.addToolMessages()
        arcpy.AddField_management("in_memory/cona", "DEPRESSION", "SHORT", "1", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        Utility.addToolMessages()
        arcpy.AddField_management("in_memory/cona", "ALEFT", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        Utility.addToolMessages()
        arcpy.AddField_management("in_memory/cona", "ARIGHT", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        Utility.addToolMessages()
        arcpy.AddField_management("in_memory/cona", "PLEFT", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        Utility.addToolMessages()
        arcpy.AddField_management("in_memory/cona", "PRIGHT", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        Utility.addToolMessages()
# #        desc = arcpy.Describe("in_memory/cona")
# #        shapeField = desc.ShapeFieldName
        arcpy.FeatureToPolygon_management("in_memory/cona", "in_memory/conp", "0.000000001 DecimalDegrees", "ATTRIBUTES", "")
        Utility.addToolMessages()
        arcpy.Identity_analysis("in_memory/cona", "in_memory/conp", arcpy.env.workspace + "\\id.shp", "ALL", "0.000000001 DecimalDegrees", "KEEP_RELATIONSHIPS")  # @UndefinedVariable
        Utility.addToolMessages()

        if arcpy.Exists("in_memory/cona"):
            arcpy.Delete_management("in_memory/cona")

        # print "Transferring attributes from polygons to arcs..."
        arcpy.CopyFeatures_management("in_memory/conp", "in_memory/conp2", "", "0", "0", "0")
        Utility.addToolMessages()
        arcpy.AddField_management("in_memory/conp", "area1", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        Utility.addToolMessages()
        arcpy.AddField_management("in_memory/conp", "length1", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        Utility.addToolMessages()
        arcpy.AddField_management("in_memory/conp2", "area2", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        Utility.addToolMessages()
        arcpy.AddField_management("in_memory/conp2", "length2", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        Utility.addToolMessages()
        arcpy.CalculateField_management("in_memory/conp", "area1", "!Shape.AREA!", "PYTHON", "#")
        Utility.addToolMessages()
        arcpy.CalculateField_management("in_memory/conp", "length1", "!Shape.LENGTH!", "PYTHON", "#")
        Utility.addToolMessages()
        arcpy.CalculateField_management("in_memory/conp2", "area2", "!Shape.AREA!", "PYTHON", "#")
        Utility.addToolMessages()
        arcpy.CalculateField_management("in_memory/conp2", "length2", "!Shape.LENGTH!", "PYTHON", "#")
        Utility.addToolMessages()
        arcpy.MakeFeatureLayer_management(arcpy.env.workspace + "\\id.shp", "identity_layer", "", "", "")  # @UndefinedVariable
        Utility.addToolMessages()
        arcpy.MakeFeatureLayer_management("in_memory/conp", "conp_layer", "")
        Utility.addToolMessages()
        try:
            arcpy.AddJoin_management("identity_layer", "LEFT_conp", "conp_layer", "FID", "KEEP_ALL")
            Utility.addToolMessages()
        except:
            arcpy.AddJoin_management("identity_layer", "LEFT_conp", "conp_layer", "OBJECTID", "KEEP_ALL")
            Utility.addToolMessages()
        arcpy.MakeFeatureLayer_management("in_memory/conp2", "conp2_layer", "")
        Utility.addToolMessages()
        try:
            arcpy.AddJoin_management("identity_layer", "RIGHT_conp", "conp2_layer", "FID", "KEEP_ALL")
            Utility.addToolMessages()
        except:
            arcpy.AddJoin_management("identity_layer", "RIGHT_conp", "conp2_layer", "OBJECTID", "KEEP_ALL")
            Utility.addToolMessages()
        arcpy.CopyFeatures_management("identity_layer", "in_memory/contours")
        Utility.addToolMessages()


        # #BREAKS HERE
        arcpy.MakeFeatureLayer_management(arcpy.env.workspace + "\\id.shp", "arc_View5")  # @UndefinedVariable
        Utility.addToolMessages()
        try:
            arcpy.AddJoin_management("arc_View5", "LEFT_conp", "in_memory/conp", "FID", "KEEP_COMMON")
            Utility.addToolMessages()
        except:
            arcpy.AddJoin_management("arc_View5", "LEFT_conp", "in_memory/conp", "OBJECTID", "KEEP_COMMON")
            Utility.addToolMessages()
        arcpy.CalculateField_management("arc_View5", "id.ALEFT", "!conp.AREA1!", "PYTHON", "#")
        Utility.addToolMessages()
        arcpy.CalculateField_management("arc_View5", "id.PLEFT", "!conp.length1!", "PYTHON", "#")
        Utility.addToolMessages()
        try:
            arcpy.AddJoin_management("arc_View5", "RIGHT_conp", "in_memory/conp2", "FID", "KEEP_COMMON")
            Utility.addToolMessages()
        except:
            arcpy.AddJoin_management("arc_View5", "RIGHT_conp", "in_memory/conp2", "OBJECTID", "KEEP_COMMON")
            Utility.addToolMessages()
        arcpy.CalculateField_management("arc_View5", "id.ARIGHT", "!conp2.AREA2!", "PYTHON", "#")
        Utility.addToolMessages()
        arcpy.CalculateField_management("arc_View5", "id.PRIGHT", "!conp2.length2!", "PYTHON", "#")
        Utility.addToolMessages()
        arcpy.RemoveJoin_management("arc_View5", "conp2")
        Utility.addToolMessages()
        arcpy.Delete_management("arc_View5")
        arcpy.RemoveJoin_management("identity_layer", "conp")
        Utility.addToolMessages()
        arcpy.Delete_management("identity_layer")
        arcpy.Delete_management("conp_layer")
        arcpy.Delete_management("conp2_layer")
        #############################################################################################
        # print "Flagging depressions..."
        #############################################################################################

        arcpy.DeleteField_management("in_memory/contours", "FID_cona")
        Utility.addToolMessages()
        arcpy.MakeFeatureLayer_management("in_memory/contours", "con_Layer2", "", "", "ID ID VISIBLE NONE;CONTOUR CONTOUR VISIBLE NONE;shape_len shape_len VISIBLE NONE;DEPRESSION DEPRESSION VISIBLE NONE;ALEFT ALEFT VISIBLE NONE;ARIGHT ARIGHT VISIBLE NONE;PLEFT PLEFT VISIBLE NONE;PRIGHT PRIGHT VISIBLE NONE;LEFT_conp LEFT_conp VISIBLE NONE;RIGHT_conp RIGHT_conp VISIBLE NONE;LEFT_Id LEFT_Id VISIBLE NONE;RIGHT_Id RIGHT_Id VISIBLE NONE;conp_FID conp_FID VISIBLE NONE;ID_1 ID_1 VISIBLE NONE;conp_area1 conp_area1 VISIBLE NONE;conp_lengt conp_lengt VISIBLE NONE;conp2_FID conp2_FID VISIBLE NONE;ID_12 ID_12 VISIBLE NONE;conp2_area conp2_area VISIBLE NONE;conp2_leng conp2_leng VISIBLE NONE")
        Utility.addToolMessages()
        arcpy.CopyFeatures_management("con_Layer2", "in_memory/conp3")
        Utility.addToolMessages()
        arcpy.Delete_management("con_Layer2")
        arcpy.Delete_management("in_memory/contours")
        arcpy.FeatureToPolygon_management("in_memory/conp3", "in_memory/conp4", "0.000000001 DecimalDegrees", "ATTRIBUTES", "")
        Utility.addToolMessages()
        # gp.MakeFeatureLayer_management("contours.shp", "con_Layer1")
        arcpy.MakeFeatureLayer_management(arcpy.env.workspace + "\\id.shp", "con_Layer1")  # @UndefinedVariable
        Utility.addToolMessages()
        
        if arcpy.Exists("in_memory/conp"):
            arcpy.Delete_management("in_memory/conp")
        if arcpy.Exists("in_memory/conp2"):
            arcpy.Delete_management("in_memory/conp2")
        if arcpy.Exists("in_memory/conp3"):
            arcpy.Delete_management("in_memory/conp3")
        if arcpy.Exists("in_memory/conp4"):
            arcpy.Delete_management("in_memory/conp4")
        
        # #############
        # PEAKS       #
        # #############

        # set all contours to 2 (rise/peak) as default
        # changed VB to PYTHON
        arcpy.CalculateField_management("con_Layer1", "DEPRESSION", "2", "PYTHON", "")
        Utility.addToolMessages()

        # #############
        # DEPRESSIONS #
        # #############

        # select enclosed depressions
        arcpy.SelectLayerByAttribute_management("con_Layer1", "NEW_SELECTION", '"LEFT_conp" > "RIGHT_conp" AND "RIGHT_conp" <> 0')
        Utility.addToolMessages()
        # changed VB to PYTHON
        arcpy.CalculateField_management("con_Layer1", "DEPRESSION", "1", "PYTHON", "")
        Utility.addToolMessages()
        # select unenclosed depressions
        arcpy.SelectLayerByAttribute_management("con_Layer1", "NEW_SELECTION", '"LEFT_conp" = 0 AND "RIGHT_conp" <> 0')
        Utility.addToolMessages()
        # changed VB to PYTHON
        arcpy.CalculateField_management("con_Layer1", "DEPRESSION", "1", "PYTHON", "")
        Utility.addToolMessages()
        # select contours that could not be formed into polygons
        arcpy.SelectLayerByAttribute_management("con_Layer1", "NEW_SELECTION", '"LEFT_conp" = 0 AND "RIGHT_conp" = 0')
        Utility.addToolMessages()
        # changed VB to PYTHON
        arcpy.CalculateField_management("con_Layer1", "DEPRESSION", "0", "PYTHON", "")
        Utility.addToolMessages()
        arcpy.SelectLayerByAttribute_management("con_Layer1", "CLEAR_SELECTION")
        Utility.addToolMessages()

        # gp.MakeFeatureLayer_management("contours.shp", "contours_layer")
        arcpy.MakeFeatureLayer_management(arcpy.env.workspace + "\\id.shp", "contours_layer")  # @UndefinedVariable
        Utility.addToolMessages()
        # print '\n\n Temporary copy for research \n\n'
        # gp.CopyFeatures_management("contours_layer", gp.env.workspace + "\\b4delete.shp")
        count = arcpy.GetCount_management("contours_layer")
        Utility.addToolMessages()
        # print "contours - before deleting small features - has " + str(count) + " features"

        # small feature deletion.
        # select and delete small rises
        # Need to update shape_len values first
        arcpy.CalculateField_management("contours_layer", "shape_len", "!Shape.LENGTH!", "PYTHON", "#")
        Utility.addToolMessages()
        
        
        # changed "PLEFT" < "PRIGHT" to "PLEFT" <= "PRIGHT"
        if "FOOT" in SpatRefLinearUnitName:    
            arcpy.SelectLayerByAttribute_management("contours_layer", "NEW_SELECTION", '"PLEFT" <= "PRIGHT" AND "shape_len" < 65.62 AND "DEPRESSION" <> 0')
        else:
            arcpy.SelectLayerByAttribute_management("contours_layer", "NEW_SELECTION", '"PLEFT" <= "PRIGHT" AND "shape_len" < 20.0 AND "DEPRESSION" <> 0')

        # gp.SelectLayerByAttribute_management("contours_layer", "NEW_SELECTION", '"PLEFT" < "PRIGHT" AND "shape_len" < 0.00004 AND "DEPRESSION" <> 0')
        firstCount = arcpy.GetCount_management("contours_layer")
        Utility.addToolMessages()
        if removeDepressions:
            if firstCount > 0:
                arcpy.DeleteFeatures_management("contours_layer")
        # select and delete small depressions
        # change from "PRIGHT" < "PLEFT" to "PRIGHT" <= "PLEFT"
        if "FOOT" in SpatRefLinearUnitName: 
            arcpy.SelectLayerByAttribute_management("contours_layer", "ADD_TO_SELECTION", '"PRIGHT" <= "PLEFT" AND "shape_len" < 65.62 AND "DEPRESSION" <> 0')
        else:
            arcpy.SelectLayerByAttribute_management("contours_layer", "ADD_TO_SELECTION", '"PRIGHT" <= "PLEFT" AND "shape_len" < 20.0 AND "DEPRESSION" <> 0')
        
        # gp.SelectLayerByAttribute_management("contours_layer", "ADD_TO_SELECTION", '"PRIGHT" < "PLEFT" AND "shape_len" < 0.00004 AND "DEPRESSION" <> 0')
        secondCount = arcpy.GetCount_management("contours_layer")
        if removeDepressions:
            if secondCount > 0:
                arcpy.DeleteFeatures_management("contours_layer")

        count = arcpy.GetCount_management("contours_layer")

        arcpy.SelectLayerByAttribute_management("contours_layer", "CLEAR_SELECTION")
        count = arcpy.GetCount_management("contours_layer")
        # print "contours layer - after deleting small features has " + str(count) + " arcs"

        arcpy.SelectLayerByAttribute_management("contours_layer", "CLEAR_SELECTION")
        arcpy.Delete_management("contours_layer")
        arcpy.Delete_management("con_Layer1")

        # GREG: hardcoded value
        newFolder = "C:\\PROJECTS\\NRCS\\Data" + "\\output"  # D:\\contours\\output"

        newFolder = "C:\\PROJECTS\\NRCS\\Data\\"
        # if not gp.Exists(newFolder):  # one-time creation of subdirectory called output.  All output geodatabases go here.
        #    gp.CreateFolder_management("C:\\PROJECTS\\NRCS\\Data", "output")


        if clipToBoundary:
            # print "Clipping contours back to quad boundary..."
            arcpy.Clip_analysis(arcpy.env.workspace + "\\id.shp", db + "\\raster_domain" + mapid, outputShapefile)  # @UndefinedVariable
        else:
            arcpy.CopyFeatures_management(arcpy.env.workspace + "\\id.shp", outputShapefile)  # @UndefinedVariable


        # print "Done."
        time99 = time.time()
        et = time99 - time0
        # print "Elapsed seconds = " + str(round(et,0))
        arcpy.AddMessage("\nElapsed seconds for current contour = " + str(round(et, 0)))
#         arcpy.CheckInExtension("3D")
#         arcpy.CheckInExtension("Spatial")
    




def CheckRasters(rasterFolder, spatialAnalystAvailable):
    # If a Spatial Analyst license is available, then check the input raster folder to
# ensure that all TIFF and IMG files are 1-band, 32-bit float raster datasets.
# If not all TIFF or IMG, then exit the script with an error, since Add Raster
# will attempt to ingest all TIFF and IMG files regardless of the number of
# bands or bit-depth of the image.
    arcpy.env.workspace = rasterFolder
    if spatialAnalystAvailable == 1:
        foundRastersFlag = 0
        for curr_raster in arcpy.ListRasters("*", "ALL"):
            # Create a Raster object to gain access to the properties of the raster
            rasterObject = arcpy.Raster(curr_raster)
            rasterObjectFormat = rasterObject.format
            if rasterObjectFormat == "TIFF" or rasterObjectFormat == "IMAGINE Image":
        # #              arcpy.AddMessage("\nRaster has NoData value of {0}".format(rasterObject.noDataValue))
                rasterObjectBands = rasterObject.bandCount
                rasterObjectPixelType = rasterObject.pixelType
                if rasterObjectBands != 1 or rasterObjectPixelType != "F32":
                    rasterObjectPath = rasterObject.catalogPath
                    arcpy.AddError("\nExiting...this file has {0} bands with bit-depth {1} : {2}".format(rasterObjectBands, rasterObjectPixelType, rasterObjectPath))
                    sys.exit(0)
                else:
                    foundRastersFlag = 1
            del rasterObject
        
        # Exit if no valid rasters found
        if (foundRastersFlag == 0):
            arcpy.AddError("\nExiting...No valid TIFF or IMG files found in the raster folder.")
            sys.exit(0)
        del curr_raster
    else:
        arcpy.AddMessage("\nNo Spatial Analyst license found...")
        arcpy.AddMessage("Unable to check images in raster folder to ensure they are 1-band 32-bit Floating pt")


def GetSpatialReference(jobId, rasterFolder):
    # Get the spatial reference string of the first raster (to use in creation of MD)
    arcpy.env.workspace = rasterFolder
    rasters = arcpy.ListRasters("*", "All")
# Make sure there's at least one raster in rasterFolder
# If not, then exit the script
# If so, get the raster's Spatial Reference
    SpatRefFirstRaster = None
    if len(rasters) > 0:
        descFirstRaster = arcpy.Describe(rasters[0])
        SpatRefFirstRaster = descFirstRaster.SpatialReference.exportToString()
        arcpy.AddMessage("Spatial reference of first raster in {0} is: \n\n{1}\n".format(rasterFolder, SpatRefFirstRaster))
        arcpy.AddMessage("Number of rasters in {0}:  {1}".format(rasterFolder, len(rasters)))
    else:
        arcpy.AddError("\nExiting: No rasters found in {0}".format(rasterFolder))
        sys.exit(0)
    del descFirstRaster, rasters
    Utility.setWMXJobDataAsEnvironmentWorkspace(jobId)
    return SpatRefFirstRaster

def PrepareContoursForPublishing(jobID):
    
    cont_int = ContourConfig.CONTOUR_INTERVAL
    cont_unit = ContourConfig.CONTOUR_UNIT
    smooth_unit = ContourConfig.CONTOUR_SMOOTH_UNIT
    distance_to_clip_md = ContourConfig.DISTANCE_TO_CLIP_MOSAIC_DATASET
    distance_to_clip_contours = ContourConfig.DISTANCE_TO_CLIP_CONTOURS
        
    Utility.printArguments(["WMX Job ID", "Contour_interval", "Contour_unit", "Smooth_unit(DecDeg)", "Distance_to_clip_md(m)", "distance_to_clip_contours(m)"], [jobID, cont_int, cont_unit, smooth_unit, distance_to_clip_md, distance_to_clip_contours], "C01 PrepareContoursForPublishing")
    
    
    
    Utility.setWMXJobDataAsEnvironmentWorkspace(jobID)
    
    ProjectJob = CMDR.ProjectJob()
    project, ProjectUID = ProjectJob.getProject(jobID)
    
    if project is not None:
        ProjectFolder = ProjectFolders.getProjectFolderFromDBRow(ProjectJob, project)
        ProjectID = ProjectJob.getProjectID(project)
        ContourFolder = ProjectFolder.derived.contour_path
        PublishedFolder = ProjectFolder.published.path
        rasterFolder = ProjectFolder.published.demLastTiff_path
        
        Deliver = CMDR.Deliver()
        delivery = list(Deliver.getDeliver(ProjectID))
        raster_vertical_unit = Deliver.getVertUnit(delivery)
        
        PYTHON_EXE = os.path.join(r'C:\Python27\ArcGISx6410.3', 'pythonw.exe')
        # use pythonw for multiprocessing
        multiprocessing.set_executable(PYTHON_EXE)
        arcpy.AddMessage("\nPython executable used: {0}".format(PYTHON_EXE))

        # Would rather use 64-bit addressing for better stability and accuracy
        branch = True
        if not "64" in PYTHON_EXE:
            branch = False
            arcpy.AddMessage("\n*********Python executable is not 64-bit: {0}  ***************".format(PYTHON_EXE))
            
        arcpy.env.overwriteOutput = True
        # comment this out in parallel version of the code, since 3D isn't needed until inside GenerateContour (where it is checked out)
        # arcpy.CheckOutExtension("3D")

        arcpy.AddMessage("\n Name in def PrepareContoursForPublishing is {0}".format(__name__))

#         # check out spatial analyst
        spatialAnalystAvailable = 0
        if arcpy.CheckExtension("Spatial") == "Available":
            arcpy.CheckOutExtension("Spatial")
            spatialAnalystAvailable = 1
#         
#         if spatialAnalystAvailable == 1:
#             arcpy.AddMessage("\nSpatial Analyst license found and checked out...")

# #        arcpy.AddMessage(inspect.getfile(inspect.currentframe()))
# #        arcpy.AddMessage(sys.version)
# #        arcpy.AddMessage(sys.executable)
# #
# #        ContourFolder = arcpy.GetParameterAsText(0)
# #        ContourFolder = ContourFolder.strip()
# #        arcpy.AddMessage("\nFolder which will contain contours: {0}".format(ContourFolder))
# #          
# #        #The location of the raster files to be ingested into the MD
# #        rasterFolder = arcpy.GetParameterAsText(1)
# #        rasterFolder = rasterFolder.strip()
# #        arcpy.AddMessage("\nRaster Folder:       {0}".format(rasterFolder))
# #
# #        #input_footprints = arcpy.GetParameterAsText(0)              # feature layer
# #        #arcpy.AddMessage("\ninput_footprints: \n{0}".format(input_footprints))
# #        #input_md = arcpy.GetParameterAsText(1)                      # mosaic layer
# #        #arcpy.AddMessage("\ninput_md: \n{0}".format(input_md))
# #        
# #        raster_vertical_unit = arcpy.GetParameterAsText(2)
# #        arcpy.AddMessage("\nraster_vu: {0}".format(raster_vertical_unit))
# #        cont_int = arcpy.GetParameterAsText(3)                      # double
# #        arcpy.AddMessage("\ncont_int: {0}".format(cont_int))
# #        cont_unit = arcpy.GetParameterAsText(4)                     # string
# #        arcpy.AddMessage("\ncont_unit: {0}".format(cont_unit))
# #        smooth_unit = arcpy.GetParameterAsText(5)                   # Linear unit
# #        arcpy.AddMessage("\nsmooth_unit: {0}".format(smooth_unit))
# #        
# #        #workspace = arcpy.GetParameterAsText(6)                     # workspace
# #        #arcpy.AddMessage("\nworkspace: \n{0}".format(workspace))
# #        
# #        distance_to_clip_md = arcpy.GetParameterAsText(6)           # linear unit or field
# #        arcpy.AddMessage("\ndistance_to_clip_md: {0}".format(distance_to_clip_md))
# #        distance_to_clip_contours = arcpy.GetParameterAsText(7)     # linear unit or field
# #        arcpy.AddMessage("\ndistance_to_clip_contours: {0}".format(distance_to_clip_contours))
        
        # Create a file gdb to hold the contours
        contourGDB_Name = ContourConfig.CONTOUR_GDB_NAME
        if not os.path.exists(ContourFolder):
            os.makedirs(ContourFolder)
        contour_file_gdb_path = os.path.join(ContourFolder, contourGDB_Name)

        # If the file gdb doesn't exist, then create it
        if not os.path.exists(contour_file_gdb_path):
            arcpy.AddMessage("\nCreating Contour GDB:   {0}".format(contour_file_gdb_path))
            arcpy.CreateFileGDB_management(ContourFolder, contourGDB_Name, out_version="CURRENT")
            Utility.addToolMessages()
    # #    else:
    # #        arcpy.AddMessage("\n***Warning: Contours in {0} are being replaced. It is most efficient to delete them first, since overwriting takes time".format(ContourFolder))

        # Create a file gdb to hold the Merged contours
#         contourMerged_Name = ContourConfig.MERGED_FGDB_NAME
        projectID = ProjectJob.getProjectID(project)
        contourMerged_Name = (ContourConfig.MERGED_FGDB_NAME).format(projectID)
        contourMerged_file_gdb_path = os.path.join(PublishedFolder, contourMerged_Name)

        # If the file gdb doesn't exist, then create it
        if not os.path.exists(contourMerged_file_gdb_path):
            arcpy.AddMessage("\nCreating GDB to hold Merged results:   {0}".format(contourMerged_file_gdb_path))
            arcpy.CreateFileGDB_management(PublishedFolder, contourMerged_Name, out_version="CURRENT")
            Utility.addToolMessages()

        # Create a file gdb to hold the scratch workspace
        scratchGDB_Name = r"scratch.gdb"
        scratch_file_gdb_path = os.path.join(ContourFolder, scratchGDB_Name)

        # If the scratch file gdb exists, then delete it before re-creating it
        # This is done to improve performance, as it takes a lot longer to overwrite existing datasets
        if not os.path.exists(scratch_file_gdb_path):
            # create a scratch file gdb
            arcpy.AddMessage("\nCreating scratch file GDB:   {0}".format(scratch_file_gdb_path))
            arcpy.CreateFileGDB_management(ContourFolder, scratchGDB_Name, out_version="CURRENT")
            Utility.addToolMessages()
        else:
            arcpy.env.workspace = scratch_file_gdb_path
            arcpy.AddMessage("\nDeleting existing feature classes in existing scratch file GDB: {0}\n".format(scratch_file_gdb_path))
            for objFeatureClass in arcpy.ListFeatureClasses():
                arcpy.AddMessage("\nDeleting feature class: {0}".format(objFeatureClass))
                arcpy.Delete_management(objFeatureClass)
                Utility.addToolMessages()
            for objFeatureClass in arcpy.ListFeatureClasses():
                arcpy.AddMessage("\nDeleted feature class still exists: {0}".format(objFeatureClass))


        # Create a file gdb to hold temporary Mosaic Dataset and Footprints
        #  (stored in original Coordinate system, NOT Web Mercator)
        MD_GDB_Name = r"Temp_MD_origCS.gdb"
        MD_file_gdb_path = os.path.join(ContourFolder, MD_GDB_Name)

        # If the file gdb doesn't exist, then create it
        if not os.path.exists(MD_file_gdb_path):
            arcpy.AddMessage("\nCreating Temp MD GDB:   {0}".format(MD_file_gdb_path))
            arcpy.CreateFileGDB_management(ContourFolder, MD_GDB_Name, out_version="CURRENT")
            Utility.addToolMessages()
        
        # ensure the Mosaic Dataset name is unique
        MD_Name = arcpy.CreateUniqueName("MD", MD_file_gdb_path)
        arcpy.AddMessage("\nTemp MD Name:  {0}".format(MD_Name))

        full_MD_Path = os.path.join(MD_file_gdb_path, MD_Name)
        MD_Footprints = MD_Name + r"_Footprints"
        MD_Boundary = MD_Name + r"_Boundary"

        # Create a scratch folder to contain intermediate products (to be deleted later)
        PathToFiles = os.path.join(ContourFolder, r"Scratch")
        arcpy.AddMessage("\nScratch Folder:             {0}".format(PathToFiles))
        
        if not os.path.exists(PathToFiles):
            arcpy.AddMessage("\nCreating Scratch Folder:    " + PathToFiles)
            os.makedirs(PathToFiles)

        # Commented out by Eric: CheckRasters(rasterFolder, spatialAnalystAvailable)
                
        SpatRefFirstRaster = GetSpatialReference(jobID, rasterFolder)

        # Create a Mosaic Dataset
        arcpy.CreateMosaicDataset_management(MD_file_gdb_path, MD_Name,
                                           coordinate_system=SpatRefFirstRaster,
                                           num_bands="1", pixel_type="32_BIT_FLOAT", product_definition="NONE", product_band_definitions="#")
        Utility.addToolMessages()
        
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from CreateMosaicDataset are: \n{0}\n".format(messages))

        # set the NoData value to -3.40282306074e+038
        arcpy.SetRasterProperties_management(full_MD_Path, data_type="ELEVATION", statistics="", stats_file="#", nodata="1 {}".format(RasterConfig.NODATA_DEFAULT))  # "1 -3.40282306074E+38")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from SetRasterProperties are: \n{0}\n".format(messages))

        # Add rasters from Raster folder to MD
        arcpy.AddRastersToMosaicDataset_management(full_MD_Path, raster_type="Raster Dataset", input_path=rasterFolder,
                                            update_cellsize_ranges="UPDATE_CELL_SIZES", update_boundary="UPDATE_BOUNDARY",
                                            update_overviews="NO_OVERVIEWS", maximum_pyramid_levels="", maximum_cell_size="0",
                                            minimum_dimension="1500", spatial_reference="", filter="#", sub_folder="SUBFOLDERS",
                                            duplicate_items_action="ALLOW_DUPLICATES", build_pyramids="NO_PYRAMIDS",
                                            calculate_statistics="NO_STATISTICS", build_thumbnails="NO_THUMBNAILS",
                                            operation_description="#", force_spatial_reference="NO_FORCE_SPATIAL_REFERENCE")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from AddRastersToMosaicDataset are: \n{0}\n".format(messages))

        # Get a record count just to be sure we found raster products to ingest
        result = arcpy.GetCount_management(full_MD_Path)
        countRasters = int(result.getOutput(0))

        if countRasters == 0:
            arcpy.AddError("\nExiting: {0} has no TIFF or IMG products.".format(full_MD_Path))
            sys.exit(0)
        else:
            arcpy.AddMessage("{0} has {1} raster product(s).".format(full_MD_Path, countRasters))

        # Build Footprints
        arcpy.BuildFootprints_management(full_MD_Path, where_clause="", reset_footprint="RADIOMETRY", min_data_value="1",
                                         max_data_value="4294967295", approx_num_vertices="3000", shrink_distance="0",
                                         maintain_edges="NO_MAINTAIN_EDGES", skip_derived_images="SKIP_DERIVED_IMAGES",
                                         update_boundary="NO_BOUNDARY", request_size="2000", min_region_size="100",
                                         simplification_method="NONE", edge_tolerance="", max_sliver_size="20",
                                         min_thinness_ratio="0.05")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from BuildFootprints are: \n{0}\n".format(messages))

        # Export, simplify, and Import Footprints
        arcpy.MakeMosaicLayer_management(full_MD_Path, "MDLayer", where_clause="", template="#", band_index="#",
                                       mosaic_method="NORTH_WEST", order_field="ProjectDate", order_base_value="",
                                       lock_rasterid="#", sort_order="ASCENDING", mosaic_operator="LAST", cell_size="1")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from MakeMosaicLayer are: \n{0}\n".format(messages))
        arcpy.AddMessage("\nRemoving unnecessary vertices from Footprints")
    # #    MD_FootprintsSimplified = os.path.join(out_file_gdb_path, "FootprintsSimplified")
    # #    arcpy.AddMessage("Footprints Feature Class Name {0}".format(MD_FootprintsSimplified))
        MDFootprints = os.path.join(MD_file_gdb_path, MD_Footprints)
        arcpy.ExportMosaicDatasetGeometry_management(full_MD_Path, MDFootprints, where_clause="", geometry_type="FOOTPRINT")
        Utility.addToolMessages()

        # Simplify the Footprints
        arcpy.SimplifyPolygon_cartography(in_features=r"MDLayer/Footprint", out_feature_class=MDFootprints,
                                        algorithm="POINT_REMOVE", tolerance="3 Meters", minimum_area="0 SquareMeters",
                                        error_option="RESOLVE_ERRORS", collapsed_point_option="KEEP_COLLAPSED_POINTS")
                                        # error_option="FLAG_ERRORS", collapsed_point_option="KEEP_COLLAPSED_POINTS")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from SimplifyPolygon of footprint shapes: \n{0}\n".format(messages))

        # import the simplified Footprints
        arcpy.ImportMosaicDatasetGeometry_management(full_MD_Path, target_featureclass_type="FOOTPRINT", target_join_field="OBJECTID",
                                                       input_featureclass=MDFootprints, input_join_field="OBJECTID")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from ImportMosaicDatasetGeometry of footprint shapes: \n{0}\n".format(messages))

        # update the MD boundary so it will match the recently simplified footprints
        arcpy.BuildBoundary_management(in_mosaic_dataset=full_MD_Path, where_clause="", append_to_existing="OVERWRITE",
                                                       simplification_method="NONE")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from BuildBoundary of footprint shapes: \n{0}\n".format(messages))


    # #    # Export Footprints
    # #
    # #    MDFootprints = os.path.join(MD_file_gdb_path, MD_Footprints)
    # #    arcpy.ExportMosaicDatasetGeometry_management(full_MD_Path, MDFootprints, where_clause="", geometry_type="FOOTPRINT")
    # #
        # Export Boundary to the file GDB which holds the final results
        
        MDBoundary = os.path.join(MD_file_gdb_path, MD_Boundary)
        arcpy.ExportMosaicDatasetGeometry_management(full_MD_Path, MDBoundary, where_clause="", geometry_type="BOUNDARY")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("Results output from ExportMosaicDatasetGeometry are: \n{0}\n".format(messages))

        # Remove Holes in Boundary (this dataset will be used as the boundary of the contours when merging contours)
        
        MD_BoundNoHoles = os.path.join(contourMerged_file_gdb_path, r"ContoursBoundary_origCS")
        arcpy.EliminatePolygonPart_management(MDBoundary, MD_BoundNoHoles, condition="PERCENT", part_area="0 SquareMeters",
                                              part_area_percent="25", part_option="CONTAINED_ONLY")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("Results output from EliminatePolygonPart are: \n{0}\n".format(messages))

        # Simplify the results a bit to remove vertices (making the shape easier to work with) 
    # #    simplifyFC = os.path.join(contourMerged_file_gdb_path,r"ContoursBoundary_origCS")
    # #    arcpy.SimplifyPolygon_cartography(MD_BoundNoHoles, simplifyFC,
    # #                       "POINT_REMOVE", 3 , minimum_area="0 SquareMeters",
    # #                       error_option="RESOLVE_ERRORS", collapsed_point_option="KEEP_COLLAPSED_POINTS")
    # #    messages =  arcpy.GetMessages()
    # #    arcpy.AddMessage("Results output from SimplifyPolygon are: \n{0}\n".format(messages))

        # Set mosaic properties:
        #  clip_to_footprints="NOT_CLIP"
        #  rows_maximum_imagesize="25000",columns_maximum_imagesize="25000"
        #  default_compression_type="LZ77"
        #  
        arcpy.SetMosaicDatasetProperties_management(full_MD_Path, rows_maximum_imagesize="25000", columns_maximum_imagesize="25000",
                            allowed_compressions="LZ77;LERC", default_compression_type="LZ77", JPEG_quality="75", LERC_Tolerance="0.001",
                            resampling_type="BILINEAR", clip_to_footprints="NOT_CLIP", footprints_may_contain_nodata="FOOTPRINTS_MAY_CONTAIN_NODATA",
                            clip_to_boundary="NOT_CLIP", color_correction="NOT_APPLY", allowed_mensuration_capabilities="Basic",
                            default_mensuration_capabilities="Basic", allowed_mosaic_methods="NorthWest;Center;LockRaster;ByAttribute;Nadir;Viewpoint;Seamline;None",
                            default_mosaic_method="NorthWest", order_field="#", order_base="#", sorting_order="ASCENDING", mosaic_operator="FIRST",
                            blend_width="0", view_point_x="600", view_point_y="300", max_num_per_mosaic="20", cell_size_tolerance="0.8",
                            cell_size="", metadata_level="BASIC", transmission_fields="#", use_time="DISABLED",
                            start_time_field="#", end_time_field="#", time_format="#", geographic_transform="#", max_num_of_download_items="20",
                            max_num_of_records_returned="1000", data_source_type="ELEVATION", minimum_pixel_contribution="1")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("Results output from SetMosaicDatasetProperties are: \n{0}\n".format(messages))

        # get number of features
        numfeats = arcpy.GetCount_management(MDFootprints)
        arcpy.AddMessage("\nNumber of footprints to create contours from: \n{0}".format(numfeats))


        # Once in a while an unexpected error occurs during the contour generation process. To mitigate this issue,
        # a second pass is taken through the footprints to generate contours that failed on the first pass

        # count the number of contours that already existed before the first run
        numCountoursExisting = 0
        
        # count the number of contours generated during each pass of the MD table
        numContoursPass1 = 0
        numContoursPass2 = 0

        for x in range(1, 3):
            arcpy.AddMessage("\nIteration number: \n{0}".format(x))

            fields = ["Name", "SHAPE@"]
            with arcpy.da.SearchCursor(MDFootprints, fields) as sc:  # @UndefinedVariable
                # mem = memory_usage_resource()
                # mem = memory2()
                # mem = memory()
                # arcpy.AddMessage("\nMemory Usage in MB: {0}".format(mem))
                # for i,row in enumerate(sc, start=1):
                for row in sc:
                    rowname = row[0]
                    arcpy.AddMessage("\n*****rowname: \n{0}".format(rowname))
                    if   x == 1:
                        arcpy.AddMessage("*****Pass #1, initiating generation of contour #{0}".format(str(numContoursPass1 + 1)))
                    elif x == 2:
                        arcpy.AddMessage("*****Pass #2, initiating generation of contour #{0}".format(str(numContoursPass2 + 1)))

                    geom = row[1]
                    
                    # create output contour name. 
                    out_conts = os.path.join(contour_file_gdb_path, "cntr{}".format(rowname))
                    arcpy.AddMessage("\nout_conts: \n{0}".format(out_conts))

                    # if it exists, go to the next 
                    if arcpy.Exists(out_conts):
                        arcpy.AddMessage("\nContour {0} already exists, moving on to the next...".format(out_conts))
                        if x == 1:
                            numCountoursExisting += 1
                        del rowname, geom
                        continue
                    
                    # make buffer for MD
                    md_buffer_name = os.path.join(scratch_file_gdb_path, "bf{}nt".format(rowname))
                    arcpy.AddMessage("\nmd_buffer_name: \n{0}".format(md_buffer_name))

                    if not arcpy.Exists(md_buffer_name):
                        arcpy.Buffer_analysis(geom, md_buffer_name, "{} METERS".format(distance_to_clip_md))
                        Utility.addToolMessages()
#                         messages = arcpy.GetMessages()
#                         arcpy.AddMessage("\nResults output from first Buffer are: \n{0}".format(messages))
                    if not arcpy.Exists(md_buffer_name):
                        arcpy.AddMessage("\nmd_buffer_name does not exist, continue to next row: {0}".format(md_buffer_name))
                        del rowname, geom
                        continue

                    # make buffer for contours
                    cont_buffer_name = os.path.join(scratch_file_gdb_path, "bf{}ng".format(rowname))
                    arcpy.AddMessage("\ncont_buffer_name: \n{0}".format(cont_buffer_name))
                    
                    if not arcpy.Exists(cont_buffer_name):
                        arcpy.Buffer_analysis(geom, cont_buffer_name, "{} METERS".format(distance_to_clip_contours))
                        Utility.addToolMessages()
#                         messages = arcpy.GetMessages()
#                         arcpy.AddMessage("\nResults output from second Buffer are: \n{0}".format(messages))
                    if not arcpy.Exists(cont_buffer_name):
                        arcpy.AddMessage("\ncont_buffer_name does not exist, continue to next row: {0}".format(cont_buffer_name))
                        del rowname, geom
                        continue

                    # make feature layer to clip

                    # clip mosaic dataset
                    clip_raster = os.path.join(PathToFiles, "{}ras.tif".format(rowname))
                    arcpy.AddMessage("\nclip_raster: {0}".format(clip_raster))

                    if not arcpy.Exists(clip_raster):
                        desc = arcpy.Describe(md_buffer_name)
                        xmin = desc.Extent.XMin
                        ymin = desc.Extent.YMin
                        xmax = desc.Extent.XMax
                        ymax = desc.Extent.YMax

                        md_buffer_name_rectangle = "{} {} {} {}".format(xmin, ymin, xmax, ymax)
                        arcpy.Clip_management(full_MD_Path, md_buffer_name_rectangle, clip_raster, "#", "-3.402823e+038")
                        Utility.addToolMessages()
#                         messages = arcpy.GetMessages()
#                         arcpy.AddMessage("\nResults output from first Clip are: \n{0}".format(messages))

                    if not arcpy.Exists(clip_raster):
                        arcpy.AddMessage("\nclip_raster does not exist, continue to next row: {0}".format(clip_raster))
                        del rowname, geom
                        continue
                    
                    # run contour generator
                    
                    temp_conts = os.path.join(PathToFiles, "temp{}.shp".format(rowname))
                    arcpy.AddMessage("\ntemp_conts: {0}".format(temp_conts))

                    # This script initiates the call to GenerateContour in a separate process
                    # This was done because there is a memory leak somewhere in the GenerateContour procedure 
                    # (Note: the memory leak was only apparent while generating contours in the foreground, so
                    #        either generate contours using background processing, or use this script to generate
                    #        contours in a separate process)
                    if not arcpy.Exists(temp_conts):
                        if branch:
                            arcpy.AddMessage("Creating external process")
                            p = Process(target=GenerateContour, args=("1", clip_raster, raster_vertical_unit, cont_int, cont_unit, smooth_unit, temp_conts, PathToFiles, False, False))
                            # GenerateContour("1", clip_raster, raster_vertical_unit, cont_int, cont_unit, smooth_unit, temp_conts, PathToFiles, False, False)
                            arcpy.AddMessage("Starting external process")
                            p.start()
                            arcpy.AddMessage("Waiting for external process to complete")
                            p.join()
                            arcpy.AddMessage("External process Complete")
                        else:
                            arcpy.AddMessage("Calling internal process Generate Contour")
                            GenerateContour("1", clip_raster, raster_vertical_unit, cont_int, cont_unit, smooth_unit, temp_conts, PathToFiles, False, False)
                    if not arcpy.Exists(temp_conts):
                        arcpy.AddMessage("\ntemp_conts does not exist, Contour not generated: {0}".format(temp_conts))
                        del rowname, geom
                        continue
                    elif x == 1:
                        numContoursPass1 += 1
                    elif x == 2:
                        numContoursPass2 += 1

                    # clip the contours
                    arcpy.Clip_analysis(temp_conts, cont_buffer_name, out_conts)
                    Utility.addToolMessages()
#                     messages = arcpy.GetMessages()
#                     arcpy.AddMessage("\nResults output from second Clip are: \n{0}".format(messages))

                    if not arcpy.Exists(out_conts):
                        arcpy.AddMessage("\nout_conts does not exist: {0}".format(out_conts))
                        del rowname, geom
                        continue
                    
                    del rowname, geom
                                     
                del row, sc
        
        # set required env variables
        # arcpy.outputCoordinateSystem = "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]]"
        arcpy.env.workspace = contour_file_gdb_path
        arcpy.env.overwriteOutput = True
        # arcpy.outputCoordinateSystem = arcpy.SpatialReference(102100)

        # arcpy.AddMessage("\n arcpy.outputCoordinateSystem (string): {}".format(arcpy.outputCoordinateSystem.exportToString()))
        
        contoursOrigCS = os.path.join(contourMerged_file_gdb_path, r"Contours_origCS")
        contourList = arcpy.ListFeatureClasses()
        arcpy.Merge_management(contourList, contoursOrigCS, "#")
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from Merge are: \n{0}".format(messages))
        del contourList
        
        # Project the results to Web Mercator

        describeContours = arcpy.Describe(contoursOrigCS)
        SpatRefStringContours = describeContours.SpatialReference.exportToString()

        contoursWebMerc = os.path.join(contourMerged_file_gdb_path, ContourConfig.CONTOUR_FC_WEBMERC)
        arcpy.Project_management(contoursOrigCS, out_dataset=contoursWebMerc,
                                 out_coor_system="PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]]",
                                 transform_method="#", in_coor_system=SpatRefStringContours)
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from Project are: \n{0}".format(messages))

        # Project the boundary too
        
        MD_Bound_WebMerc = os.path.join(contourMerged_file_gdb_path, ContourConfig.CONTOUR_BOUND_FC_WEBMERC)
        arcpy.Project_management(MD_BoundNoHoles, out_dataset=MD_Bound_WebMerc,
                                 out_coor_system="PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]]",
                                 transform_method="#", in_coor_system=SpatRefStringContours)
        Utility.addToolMessages()
#         messages = arcpy.GetMessages()
#         arcpy.AddMessage("\nResults output from Project are: \n{0}".format(messages))
        del describeContours, SpatRefStringContours
        
        arcpy.AddMessage("\nTotal number of Raster datasets in the Raster Folder: {0}".format(numfeats))
        arcpy.AddMessage("\nTotal number of Contour datasets that already existed before the first pass: {0}".format(numCountoursExisting))
        arcpy.AddMessage("\nNumber of Countour datasets created in the first pass:  {0}".format(numContoursPass1))
        arcpy.AddMessage("\nNumber of Countour datasets created in the second pass: {0}".format(numContoursPass2))
        
        # print "\nTotal number of Raster datasets in the Raster Folder: " + str(numfeats)
        # print "\nTotal number of Contour datasets that already existed before the first pass: " + str(numCountoursExisting)
        # print "\nNumber of Countour datasets created in the first pass: :" + str(numContoursPass1)
        # print "\nNumber of Countour datasets created in the second pass: :" + str(numContoursPass2)
        
    # #        else:
    # #            #arcpy.AddMessage("{} exists, skipping {} of {}...".format(out_conts, i, numfeats))
    # #            arcpy.AddMessage("{} exists, skipping...".format(out_conts))
                

    arcpy.AddMessage("Operation Complete")

if __name__ == '__main__':
    ContourFolder = ""  # Should be in the DERIVED project folder
    rasterFolder = ""  # Should be in the PUBLISHED project folder
    raster_vertical_unit = ""  # Should be in the DERIVED project folder
    cont_int = ""  # 2 foot
    cont_unit = ""  # 2 foot
    smooth_unit = ""  # ? 
    distance_to_clip_md = ""
    distance_to_clip_contours = ""
    
#     jobID = arcpy.GetParameterAsText(0)
    jobID = 4801

    PrepareContoursForPublishing(jobID)
    
    
# if __name__ == '__main__':
#     
#     arcpy.AddMessage(inspect.getfile(inspect.currentframe()))
#     arcpy.AddMessage(sys.version)
#     arcpy.AddMessage(sys.executable)
#     executedFrom = sys.executable.upper()
#     
#     #if len(sys.argv) == 4:
#     if not ("ARCMAP" in executedFrom or "ARCCATALOG" in executedFrom or "RUNTIME" in executedFrom):
#         arcpy.AddMessage("Getting parameters from command line...")
# 
#         ContourFolder = sys.argv[1]
#         ContourFolder = ContourFolder.strip()
#         arcpy.AddMessage("\nFolder which will contain contours: {0}".format(ContourFolder))
#           
#         #The location of the raster files to be ingested into the MD
#         rasterFolder = sys.argv[2]
#         rasterFolder = rasterFolder.strip()
#         arcpy.AddMessage("\nRaster Folder:       {0}".format(rasterFolder))
#         
#         raster_vertical_unit = sys.argv[3]
#         arcpy.AddMessage("\nraster_vu: {0}".format(raster_vertical_unit))
#         
#         cont_int = sys.argv[4]                      # double
#         arcpy.AddMessage("\ncont_int: {0}".format(cont_int))
#         
#         cont_unit = sys.argv[5]                     # string
#         arcpy.AddMessage("\ncont_unit: {0}".format(cont_unit))
#         
#         smooth_unit = sys.argv[6]                  # Linear unit
#         arcpy.AddMessage("\nsmooth_unit: {0}".format(smooth_unit))
#         
#         distance_to_clip_md = sys.argv[7]           # linear unit or field
#         arcpy.AddMessage("\ndistance_to_clip_md: {0}".format(distance_to_clip_md))
#         
#         distance_to_clip_contours = sys.argv[8]     # linear unit or field
#         arcpy.AddMessage("\ndistance_to_clip_contours: {0}".format(distance_to_clip_contours))
# 
#     else:
#         arcpy.AddMessage("Getting parameters from GetParameterAsText...")
#         arcpy.AddMessage("*****NOTE: This script employs the Python multiprocessing package, and is intended to run at the command line (outside Arc)****")
#         ContourFolder = arcpy.GetParameterAsText(0)
#         ContourFolder = ContourFolder.strip()
#         arcpy.AddMessage("\nFolder which will contain contours: {0}".format(ContourFolder))
#           
#         #The location of the raster files to be ingested into the MD
#         rasterFolder = arcpy.GetParameterAsText(1)
#         rasterFolder = rasterFolder.strip()
#         arcpy.AddMessage("\nRaster Folder:       {0}".format(rasterFolder))
# 
#         raster_vertical_unit = arcpy.GetParameterAsText(2)
#         arcpy.AddMessage("\nraster_vu: {0}".format(raster_vertical_unit))
#         cont_int = arcpy.GetParameterAsText(3)                      # double
#         arcpy.AddMessage("\ncont_int: {0}".format(cont_int))
#         cont_unit = arcpy.GetParameterAsText(4)                     # string
#         arcpy.AddMessage("\ncont_unit: {0}".format(cont_unit))
#         smooth_unit = arcpy.GetParameterAsText(5)                   # Linear unit
#         arcpy.AddMessage("\nsmooth_unit: {0}".format(smooth_unit))
#         
#         #workspace = arcpy.GetParameterAsText(6)                     # workspace
#         #arcpy.AddMessage("\nworkspace: \n{0}".format(workspace))
#         
#         distance_to_clip_md = arcpy.GetParameterAsText(6)           # linear unit or field
#         arcpy.AddMessage("\ndistance_to_clip_md: {0}".format(distance_to_clip_md))
#         distance_to_clip_contours = arcpy.GetParameterAsText(7)     # linear unit or field
#         arcpy.AddMessage("\ndistance_to_clip_contours: {0}".format(distance_to_clip_contours))
# 
#     PrepareContoursForPublishing(ContourFolder,rasterFolder,raster_vertical_unit,cont_int,cont_unit,smooth_unit,distance_to_clip_md,distance_to_clip_contours)
    
