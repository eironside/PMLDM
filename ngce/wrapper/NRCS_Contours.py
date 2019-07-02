# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# NRCS_Contours.py
# Created on: 2019-04-11
# Description: Wrapper for the contour generation for NRCS
# ---------------------------------------------------------------------------
import ALClasses
import arcpy
import os
import sys
filePath = os.path.abspath(__file__)
toolsPath = "\\".join(filePath.split("\\")[0:-3])
sys.path.append(toolsPath)
import ngce.pmdm.RunUtil
import traceback

# Set global variables
unitcodes  = [["MT", "FT", "US_FT"],["Meters", "Feet (International)", "Feet (US Survey)"]]
outputfcs  = ['Contours_OCS', 'Contours_WM']
tempshps   = ['footprints_clip_cont.shp', 'footprints_clip_md.shp']
gdbname    = 'Contours.gdb'

def main():
    try:
        # Read parameters from command line
        inType     = arcpy.GetParameterAsText(0)
        inFolder   = arcpy.GetParameterAsText(1)
        inMosaic   = arcpy.GetParameterAsText(2)
        outFolder  = arcpy.GetParameterAsText(3)
        inSpace    = arcpy.GetParameter(4)
        inUnits    = arcpy.GetParameterAsText(5)
        aoiFC      = arcpy.GetParameterAsText(6)
        outLabels  = arcpy.GetParameter(7)
        inUnitCode = ""

        # Check input parameters exist and are set correctly
        if inType == '#' or not inType:
            arcpy.AddError("Input type not set")
            raise arcpy.ExecuteError

        # Check inType has a valid value
        # Also check that the Folder or Mosaic exist as appropriate
        if inType == "Folder":
            if not inFolder or inFolder == '#':
                arcpy.AddError("Input folder not set")
                raise arcpy.ExecuteError
        elif inType == "Mosaic":
            if not inMosaic or inMosaic == '#':
                arcpy.AddError("Input mosaic not set")
                raise arcpy.ExecuteError
        else:
            arcpy.AddError("Input type incorrect = {}".format(inType))
            raise arcpy.ExecuteError

        # Check that the output folder exists
        if not outFolder or outFolder == '#':
            arcpy.AddError("Output folder not set")
            raise arcpy.ExecuteError
        elif not arcpy.Exists(outFolder):
            arcpy.AddError("Output folder does not exist")
            raise arcpy.ExecuteError

        # Check the required contour spacing is of correct type
        if inSpace == None:
            arcpy.AddError("Contour spacing not set")
            raise arcpy.ExecuteError
        elif isinstance(inSpace, str):
            if inSpace == '#':
                arcpy.AddError("Contour spacing not set")
                raise arcpy.ExecuteError
            else:
                inSpace = int(inSpace)
        elif not isinstance(inSpace, int) or isinstance(inSpace, long):
            arcpy.AddError("Contour spacing set to the wrong type {}".format(type(inSpace)))
            raise arcpy.ExecuteError

        # Check the vertical units
        if not inUnits or inUnits == '#':
            arcpy.AddError("Vertical units not set")
            raise arcpy.ExecuteError
        elif not inUnits in unitcodes[1]:
            arcpy.AddError("Vertical units not set correctly = {}".format(inUnits))
            raise arcpy.ExecuteError
        else:
            inUnitCode = unitcodes[0][unitcodes[1].index(inUnits)]

        # Check that the AOI exists if supplied
        if not aoiFC or aoiFC == '#':
            aoiFC = None
        elif not arcpy.Exists(aoiFC):
            arcpy.AddError("AOI folder does not exist")
            raise arcpy.ExecuteError

        # Set default in case outLabels is not set
        outLabels_bool = True
        if outLabels == None:
            outLabels_bool = True
        elif isinstance(outLabels, bool):
            outLabels_bool = outLabels
        elif isinstance(outLabels, str):
            if outLabels.lower() == 'true' or outLabels == '#':
                outLabels_bool = True
            elif outLabels.lower() == 'false':
                outLabels_bool = False
            else:
                arcpy.AddError('Create label option set to incorrect value {}, should be \'true\' or \'false\''.format(outLabels))
                raise arcpy.ExecuteError
        else:
            arcpy.AddError('Create label option set incorrectly, of type {}'.format(type(outLabels)))
            raise arcpy.ExecuteError


        # Make sure that the output gdb exists
        outGdb = os.path.join(outFolder, gdbname)
        if not arcpy.Exists(outGdb):
            arcpy.CreateFileGDB_management(outFolder, gdbname)
            if not arcpy.Exists(outGdb):
                arcpy.AddError('Output file geodatabase {} does not exist and cannot be created'.format(outGdb))
                raise arcpy.ExecuteError

        # Clear out existing feature classes
        for fc in outputfcs:
            out_contours_fc = os.path.join(str(outGdb), fc)
            with ALClasses.FeatureClass(out_contours_fc) as contours_fc:
                if contours_fc.delete() == False:
                    raise arcpy.ExecuteError

        # Delete temp shapefiles used for controlling execution
        for tempsh in tempshps:
            out_footprint_shp = os.path.join(outFolder, tempsh)
            with ALClasses.FeatureClass(out_footprint_shp) as footprint_shp:
                if footprint_shp.delete() == False:
                    raise arcpy.ExecuteError


        # Create temporary mosaic
        # If this is an existing mosaic create a temporary reference mosaic
        # Otherwise, make a new mosaic from the folder of data
        refMosaic = None
        if inType == "Folder":
            # Get reference from first image
            firstraster = None
            walk = arcpy.da.Walk(inFolder, datatype="RasterDataset")
            for dirpath, dirnames, filenames in walk:
                if filenames:
                    firstraster =  ALClasses.Raster(dirpath,filenames[0])
                    break
            if firstraster == None:
                arcpy.AddError('Cannot find rasters in folder {}'.format(inFolder))
                raise arcpy.ExecuteError
            spref = firstraster.spatialreference()
            refMosaic = ALClasses.TempMosaicDataset()
            refMosaic.createmosaic(proj=spref)
            refMosaic.addrasters(inFolder)
        else:
            # Make reference by copying from original so that we can apply clip
            origMosaic = ALClasses.MosaicDataset(inMosaic)
            spref = origMosaic.spatialreference()
            refMosaic = ALClasses.TempMosaicDataset()
            refMosaic.createmosaic(proj=spref)
            refMosaic.addtable(origMosaic,exclude_overviews=True)


        # Now create a temporary feature class from the footprints
        fps = ALClasses.TempFeatureClass("TempFootprints")
        refMosaic.exportgeometry(fps, where_clause="Category = 1", geometry_type="FOOTPRINT")

        # If we do not have an AOI supplied use the boundary of the temp mosaic
        refAOI = None
        clip_bound = False
        if not aoiFC:
            refAOI = ALClasses.TempFeatureClass("TempAOI")
            refMosaic.exportgeometry(refAOI, geometry_type="BOUNDARY")
        else:
            origAOI = ALClasses.FeatureClass(aoiFC)
            refAOI  = ALClasses.TempFeatureClass("TempAOI")
            origAOI.copyfeatures(refAOI)

            # Now clip the footprints to the AOI
            tempfps = fps
            fps = ALClasses.TempFeatureClass("TempFootprintClip")
            tempfps.clip(refAOI, fps)

            # Also clip the mosaic footprints to the AOI
            refMosaic.deleteexternalrasters(refAOI)
            refMosaic.importgeometry (fps,'UriHash',geometry_type="FOOTPRINT")


        # Make sure that the other reference mosaic does not exist
        # Defined as a temporary mosaic will ensure that it gets deleted when out of scope
        with ALClasses.MosaicDataset('{}_Cprep'.format(refMosaic.fname)) as derivedMosaic:
            if derivedMosaic.delete() == False:
                raise arcpy.ExecuteError

            # Add call to generate contours tool here!
            arcpy.AddMessage('Parameters:')
            arcpy.AddMessage('    Mosaic          = {}'.format(refMosaic.fname))
            arcpy.AddMessage('    Mosaic _Cprep   = {}'.format(derivedMosaic.fname))
            arcpy.AddMessage('    Footprints      = {}'.format(fps.fname))
            arcpy.AddMessage('    Output folder   = {}'.format(outFolder))
            arcpy.AddMessage('    Contour spacing = {}'.format(inSpace))
            arcpy.AddMessage('    Vertical units  = {}'.format(inUnitCode))
            arcpy.AddMessage('    AOI             = {}'.format(refAOI.fname))
            arcpy.AddMessage('    Create labels   = {}'.format(outLabels))
            PATH = r'ngce\pmdm\c\C01ProcessContoursFromMDParallel.py'
            args = [refMosaic.fname, fps.fname, outFolder, inSpace, inUnitCode]
            ngce.pmdm.RunUtil.runTool(PATH, args, log_path=outFolder)


    except arcpy.ExecuteError:
        # Get the tool error messages
        msgs = arcpy.GetMessages(2)

        # Return tool error messages for use with a script tool
        arcpy.AddError(msgs)

        # Print tool error messages for use in Python/PythonWin
        print msgs

    except:
        # Get the traceback object
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]

        # Concatenate information together concerning the error into a message string
        pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
        msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"

        # Return python error messages for use in script tool or Python Window
        arcpy.AddError(pymsg)
        arcpy.AddError(msgs)

        # Print Python error messages for use in Python / Python Window
        print pymsg + "\n"
        print msgs


if __name__ == '__main__':
    main()

