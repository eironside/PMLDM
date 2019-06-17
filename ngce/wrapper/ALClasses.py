# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# ALClasses.py
# Created on: 2016-04-15
# Description: Utility classes for handling mosaic datasets
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#
# THIS CODE CANNOT RUN IN MULTITHREADED MODE AS ANY TEMPORARY TABLES OR
# FEATURES CLASSES WILL CONFLICT.
#
# ---------------------------------------------------------------------------

# Set the necessary product code
import arceditor

# Import arcpy module
import arcpy
import os
import sys
import traceback


# Global defaults
Verbose = False
WMAS_ProjCS = "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]];-20037700 -30241100 10000;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision"



def DisplayMessages(vprint=False):
    if vprint == True or Verbose == True:
        arcpy.AddMessage(arcpy.GetMessages())



def DecodeNames (gname, tname=None, replacechars=None):
    if tname == None:
        local_gname = os.path.dirname(gname)
        local_tname = os.path.basename(gname)
    else:
        local_gname = gname
        local_tname = tname

    if local_gname == "":
        local_fname = local_tname
    else:
        local_fname = os.path.join(local_gname,local_tname)

    return (local_gname, local_tname, local_fname)



def ParseTableName(tname, replacechars=None):
    if replacechars == None:
        return tname
    else:
        return "".join([replacechars.get(ch, ch) for ch in tname])



def InitializeNames(gname, tname=None, temptable=False, scratch=None, replacechars=None):
        # Was this just a table name or a GDB and table name
        if temptable == True:
            local_gname, local_tname, local_fname = DecodeNames(gname, tname)

            if local_gname == "":
                local_gname = arcpy.env.scratchGDB if scratch==None else scratch

            local_tname = ParseTableName(local_tname,replacechars)

            return DecodeNames(arcpy.CreateUniqueName(ParseTableName(local_tname, replacechars), local_gname))
        else:
            local_tname = ParseTableName(tname,replacechars) if tname else None
            return DecodeNames(gname, local_tname)



class Table (object):
    def __init__(self, gname, tname=None, temptable=False):
        replace_list = {
            " ": "_",
            ".": ""
            }
        self.gname = None
        self.tname = None
        self.fname = None
        self.temptable = temptable

        # Was this just a table name or a GDB and table name
        (self.gname, self.tname, self.fname) = InitializeNames (gname, tname, temptable, arcpy.env.scratchGDB, replace_list)
        if self.temptable == True:
            if self.delete() == False:
                raise arcpy.ExecuteError

    def parsetablename(self, tname):
        return tname.replace(" ", "_").replace(".","")

    def create(self,template=None):
        retval = arcpy.CreateTable_management(self.gname, self.tname, template)
        DisplayMessages()
        return retval

    def describe(self):
        return arcpy.Describe(self.fname)

    # Get size of selection
    def getcount(self):
        return long(arcpy.GetCount_management(self.fname).getOutput(0))

    # Get list of fields
    def getfields(self):
        return arcpy.ListFields(self.fname)

    # Find a field from a feature class
    def findfield (self, fieldname):
        fieldList = self.getfields()
        for field in fieldList:
            if field.name == fieldname:
                return True
        return False

    # Set the value using the field calculator
    def calculatefield (self, fname, cexp, clang="PYTHON", cblock="", where_clause=None):
        if where_clause:
            self.selectbyattribute("NEW_SELECTION", where_clause)
        arcpy.CalculateField_management(self.fname, fname, cexp, clang, cblock)
        DisplayMessages()

    # Create a field
    def createfield (self, fname, ftype, flength=""):
        arcpy.AddField_management(self.fname, fname, ftype, "", "", flength, "", "NULLABLE", "NON_REQUIRED", "")
        DisplayMessages()

    # Create a field and then set the value using the field calculator
    def createandcalculatefield (self, fname, ftype, cexp, clang="PYTHON", flength="", cblock=""):
        self.createfield(fname, ftype, flength)
        self.calculatefield(fname, cexp, clang, cblock)

    # Select something from the table and return the number selected
    def selectbyattribute(self, stype="NEW_SELECTION", swhere=""):
        arcpy.SelectLayerByAttribute_management(self.fname, stype, swhere)
        DisplayMessages()
        return long(self.getcount())

    # Join another table
    def jointable (self,sname,stable,sjfield=None,sfields=None):
        if sjfield == None:
            sjfield = sname
        arcpy.JoinField_management(self.fname, sname, stable.fname, sjfield, sfields)
        DisplayMessages()

    # Copy features
    def copyfeatures (self, ofile):
        arcpy.CopyFeatures_management (self.fname, ofile.fname)
        DisplayMessages()

    # Sort features
    def sort(self,ofile,sfield,sorder="ASCENDING"):
        arcpy.Sort_management(self.fname, ofile.fname, [[sfield,sorder]])

    # Delete entries with idential values for
    def deleteidentical(self, sfields):
        arcpy.DeleteIdentical_management(TmpTable.fname, sfields)
        DisplayMessages()

    # Add index to table
    def addindex(self, ifields, iname, unique="NON_UNIQUE", ascending="NON_ASCENDING"):
        arcpy.AddIndex_management(self.fname, ifields, iname, unique, ascending)

    # Delete a field
    def deletefield(self,sfield):
        arcpy.DeleteField_management(self.fname, sfield)
        DisplayMessages()

    # Get InsertCursor
    def insertcursor(self,ifields=None):
        return arcpy.da.InsertCursor(self.fname, ifields)

    # Get SearchCursor
    def searchcursor(self,sfields,where_clause=None,spatial_reference=None,explode_to_points=False,sql_clause=(None,None)):
        return arcpy.da.SearchCursor(self.fname, sfields, where_clause, spatial_reference, explode_to_points, sql_clause)

    # Get UpdateCursor
    def updatecursor(self,ufields,where_clause=None,spatial_reference=None,explode_to_points=False,sql_clause=(None,None)):
        return arcpy.da.UpdateCursor(self.fname, ufields, sfields, where_clause, spatial_reference, explode_to_points, sql_clause)

    def exists(self):
        return arcpy.Exists(self.fname)

    def delete(self):
        if self.exists():
            arcpy.Delete_management(self.fname)
            if self.exists():
                arcpy.AddError("Table {0} cannot be deleted".format(self.fname))
                return False
        return True

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        if self.temptable == True:
            self.delete()

    def __del__(self):
        if self.temptable == True:
            self.delete()



# Class for temporary tables
class TempTable(Table):
    def __init__(self, tname=None):
        super(TempTable,self).__init__("TempTable" if tname == None else tname, temptable=True)

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        super(TempTable,self).__exit__(type,value,traceback)

    def __del__(self):
        super(TempTable,self).__del__()



class FeatureClass(Table):
    def __init__(self, gname, tname=None, temptable=False):
        super(FeatureClass,self).__init__(gname,tname,temptable)

    # Select something from the footprints and return the number selected
    def selectbylocation(self, sovrtype, sfeatures, sdist, stype):
        arcpy.SelectLayerByLocation_management(self.fname, sovrtype, sfeatures.fname, sdist, stype)
        DisplayMessages()
        return long(self.getcount())

    def extent(self):
        return arcpy.Describe(self.fname).extent

    def create(self, template=None, spatial_reference="#"):
        retval = arcpy.CreateFeatureclass_management(self.gname, self.tname, template=template, spatial_reference=spatial_reference)
        DisplayMessages()
        return retval

    def spatialreference(self):
        return self.describe().spatialReference

    def spatialreferenceasstring(self):
        return self.spatialreference.exporttostring()

    def buffer(self,ofile,dist):
        arcpy.Buffer_analysis (self.fname, ofile.fname, dist, "FULL", "ROUND", "NONE")
        DisplayMessages()

    def project(self, sout, soutcs, transform_method=None, in_coor_system=None, preserve_shape=None, max_deviation=None):
        arcpy.Project_management (self.fname, sout, soutcs, transform_method, in_coor_system, preserve_shape, max_deviation)
        DisplayMessages()

    def clip(self,cfile,ofile):
        arcpy.Clip_analysis(self.fname, cfile.fname, ofile.fname)
        DisplayMessages()

    def erase(self,efile,ofile):
        arcpy.Erase_analysis (self.fname, efile.fname, ofile.fname)
        DisplayMessages()

    def simplify(self,ofile,sdist):
        arcpy.SimplifyPolygon_cartography(self.fname, ofile.fname, "POINT_REMOVE", sdist, "0 SquareMeters", "NO_CHECK", "NO_KEEP")
        DisplayMessages()

    # Get geometries
    def readgeometries (self):
        geometries = arcpy.CopyFeatures_management(self.fname, arcpy.Geometry())
        DisplayMessages()
        return geometries

    def writegeometries (self,geometries):
        arcpy.CopyFeatures_management(geometries, self.fname)
        DisplayMessages()

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        if self.temptable == True:
            self.delete()

    def __del__(self):
        if self.temptable == True:
            self.delete()



# Class for temporary feature classes
class TempFeatureClass(FeatureClass):
    def __init__(self, tname=None):
        super(TempFeatureClass,self).__init__("TempFeatureClass" if tname == None else tname, temptable=True)

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        super(TempFeatureClass,self).__exit__(type,value,traceback)

    def __del__(self):
        super(TempFeatureClass,self).__del__()



class Raster(object):
    def __init__(self, gname, rname=None,tempraster=False):
        self.tempraster = tempraster
        # Was this just a table name or a GDB and table name
        self.gname, self.rname, self.fname = InitializeNames (gname, rname, tempraster, arcpy.env.scratchFolder)
        if self.tempraster == True:
            self.delete()

    def describe(self):
        return arcpy.Describe(self.fname)

    def spatialreference(self):
        return self.describe().spatialReference

    def exists(self):
        return arcpy.Exists(self.fname)

    def delete(self):
        if self.exists():
            arcpy.Delete_management(self.fname)
            if self.exists():
                arcpy.AddError("Raster {0} cannot be deleted".format(self.fname))
                return False
        return True

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        if self.tempraster == True:
            self.delete()

    def __del__(self):
        if self.tempraster == True:
            self.delete()



class TempRaster(Raster):
    def __init__(self, rname=None):
        super(TempRaster,self).__init__("TempRaster" if rname == None else rname,tempraster=True)

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        super(TempRaster,self).__exit__(type,value,traceback)

    def __del__(self):
        super(TempRaster,self).__del__()



# Class for feature layers
class FeatureLayer(FeatureClass):
    def __init__(self,lname,fclass="",wclause=""):
        self.lname = lname
        super(FeatureLayer,self).__init__(self.lname, temptable=False)
        if fclass == "":
            if not self.exists():
                arcpy.AddError("Layer {0} does not exist".format(self.lname))
                raise arcpy.ExecuteError
        else:
            arcpy.MakeFeatureLayer_management(fclass, self.lname, wclause, "", "")

    def __del__(self):
        super(FeatureLayer,self).__del__()

    def getcount(self):
        return int(arcpy.GetCount_management(self.lname).getOutput(0))

    def buffer(self,oname,dist):
        arcpy.Buffer_analysis (self.lname, oname, DistanceString(dist), "FULL", "ROUND", "NONE")
        DisplayMessages()

    def buffer(self,oname,dist):
        arcpy.Buffer_analysis (self.lname, oname, DistanceString(dist), "FULL", "ROUND", "NONE")
        DisplayMessages()



class MosaicLayer(object):
    def __init__(self,lname,mname):
        self.lname = lname
        arcpy.MakeMosaicLayer_management(mname, self.lname)
        self.flayer = FeatureLayer("{0}\Footprint".format(self.lname))

    def __del__(self):
        if arcpy.Exists(self.lname):
            arcpy.Delete_management(self.lname)

    # Select something from the footprints and return the number selected
    def selectbylocation(self, sovrtype, sfeatures, sdist, stype):
        return int(self.flayer.selectbylocation(sovrtype, sfeatures, sdist, stype))

    # Select something from the footprints and return the number selected
    def selectbyattribute(self, stype="NEW_SELECTION", swhere=""):
        return int(self.flayer.selectbyattribute(stype, swhere))

    # Create a field and then set the value using the field calculator
    def calculatefield (self, fname, cexp, clang="PYTHON", cblock="",where_clause=None):
        self.flayer.calculatefield(fname, cexp, clang, cblock, where_clause)

    # Create a field and then set the value using the field calculator
    def createandcalculatefield (self, fname, ftype, cexp, clang="PYTHON", flength="", cblock=""):
        self.flayer.createandcalculatefield(fname, ftype, cexp, clang, flength, cblock)

    # Create a field
    def createfield (self, fname, ftype, flength=""):
        self.flayer.createfield(fname,ftype,flength)

    # Delete a field
    def deletefield(self,sfield):
        self.flayer.deletefield(sfield)

    # Copy footprints
    def copyfootprints (self, ofile):
        self.flayer.copyfeatures(ofile)

    # Find a given field in the footprint layer
    def findfield(self,fieldname):
        return self.flayer.findfield(fieldname)

    # Join another table
    def jointable (self,sname,stable,sjfield=None,sfields=None):
        self.flayer.jointable(sname, stable, sjfield, sfields)

    def removerasters(self, **kwargs):
        arcpy.RemoveRastersFromMosaicDataset_management(self.lname, **kwargs)
        DisplayMessages()


# Mosaic dataset class
class MosaicDataset(object):
    def __init__(self,gname,mname=None,tempmosaic=False):
        self.tempmosaic = tempmosaic
        self.mlayer     = None

        # Was this just an image name or a workspace and table name
        self.gname, self.mname, self.fname = InitializeNames (gname, mname, tempmosaic, arcpy.env.scratchGDB)

        # Needs to exist for mosaic layer functions to work so make sure to create it
        if self.tempmosaic == True:
            if self.delete() == False:
                raise arcpy.ExecuteError

        if self.exists():
            self.createlayer()

    # Delete mosaic
    def delete(self):
        if self.exists():
            arcpy.Delete_management(self.fname)
            if self.exists():
                arcpy.AddError("Mosaic {0} cannot be deleted".format(self.fname))
                return False
        return True

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        if self.tempmosaic == True:
            self.delete()

    def __del__(self):
        if self.tempmosaic == True:
            self.delete()

    def exists(self):
        return arcpy.Exists(self.fname)

    def createlayer(self):
        self.mlayer = MosaicLayer("{}_Layer".format(self.mname),self.fname)
        DisplayMessages()

    def createmosaic(self,proj=WMAS_ProjCS):
        if not self.exists():
            arcpy.CreateMosaicDataset_management(self.gname, self.mname, proj)
            self.createlayer()
            DisplayMessages()

    def createreference(self,omosaic):
        arcpy.CreateReferencedMosaicDataset_management(self.fname, omosaic.fname)
        DisplayMessages()

    def createtempreference(self,mname=None):
        omosaic = MosaicDataset("TempMosaic" if mname == None else mname, tempmosaic=True)
        omosaic.createreference(self)
        return omosaic

    def describe(self):
        return arcpy.Describe(self.fname)

    def spatialreference(self):
        return self.describe().spatialReference

    # Find a given field in the footprint layer
    def findfield(self,fieldname):
        return self.mlayer.findfield(fieldname)

    # Join another table
    def jointable (self,sname,stable,sjfield=None,sfields=None):
        self.mlayer.jointable(sname, stable, sjfield, sfields)

    def addtable(self,stable,exclude_overviews=False, **kwargs):
        tablename = stable.fname
        if type(stable) is MosaicDataset:
            if exclude_overviews == True:
                stable.selectbyattribute("NEW_SELECTION", "Category = 1")
                tablename = stable.mlayer.lname
        arcpy.AddRastersToMosaicDataset_management(self.fname, "Table", tablename, **kwargs)
        DisplayMessages()

    def addtables(self,slist,**kwargs):
        arcpy.AddRastersToMosaicDataset_management(self.fname, "Table", slist, **kwargs)
        DisplayMessages()

    def addrasters(self, nimages, **kwargs):
        arcpy.AddRastersToMosaicDataset_management(self.fname, "Raster Dataset", nimages, **kwargs)
        DisplayMessages()

    def setproperties(self, **kwargs):
        arcpy.SetMosaicDatasetProperties_management(self.fname, **kwargs)
        DisplayMessages()

    def buildfootprints(self, **kwargs):
        arcpy.BuildFootprints_management(self.fname,**kwargs)
        DisplayMessages()

    def buildboundary(self, **kwargs):
        arcpy.BuildBoundary_management(self.fname, **kwargs)
        DisplayMessages()

    def selectbyattribute(self, stype="NEW_SELECTION", swhere=""):
        return self.mlayer.selectbyattribute(stype, swhere)

    def calculatefield(self, fname, cexp, clang="PYTHON", cblock="", where_clause=None):
        self.mlayer.calculatefield(fname, cexp, clang, cblock, where_clause)

    def createandcalculatefield (self, fname, ftype, cexp, clang="PYTHON", flength="", cblock=""):
        self.mlayer.createandcalculatefield(fname, ftype, cexp, clang, flength, cblock)

    def createfield (self, fname, ftype, flength=""):
        self.mlayer.createfield(fname,ftype,flength)

    def deletefield(self,sfield):
        self.mlayer.deletefield(sfield)

    def exportgeometry(self,ofile, **kwargs):
        arcpy.ExportMosaicDatasetGeometry_management(self.fname, ofile.fname, **kwargs)
        DisplayMessages()

    def importgeometry(self,ifile,sfield="Name",ifield=None,geometry_type="FOOTPRINT",set_clip=True, set_boundary=True):
        if ifield == None:
            ifield = sfield
        arcpy.ImportMosaicDatasetGeometry_management(self.fname, geometry_type, sfield, ifile.fname, ifield)
        DisplayMessages()
        if set_clip == True:
            if geometry_type == "FOOTPRINT":
                self.setproperties (clip_to_footprints="CLIP")
                if set_boundary == True:
                    self.buildboundary()
            elif geometry_type == "BOUNDARY":
                self.setproperties (clip_to_boundary="CLIP")

    def calculatecellsizes(self, **kwargs):
        arcpy.CalculateCellSizeRanges_management(self.fname, **kwargs)
        DisplayMessages()

    def setmaxps(self,maxps,where_clause="Category = 1"):
        self.mlayer.calculatefield("MaxPS", maxps, where_clause=where_clause)

    def defineoverviews(self,ovdir,startps):
        arcpy.DefineOverviews_management(self.fname, ovdir, "", "", startps, "4", "5120", "5120", "2", "FORCE_OVERVIEW_TILES", "BILINEAR", "JPEG", "75")
        DisplayMessages()

    def exportpaths(self,otable,where_clause="#",smode="ALL",stype="RASTER"):
        arcpy.ExportMosaicDatasetPaths_management(self.fname, otable.fname, where_clause, smode, stype)
        DisplayMessages()

    def buildoverviews(self):
        # Calculate how many service overviews need to be created
        BuildFootprintCount = self.mlayer.selectbyattribute("NEW_SELECTION", "Category > 2")

        # Build Overviews
        arcpy.AddMessage ("Building {0} overviews for {1}".format(BuildFootprintCount, self.fname))
        arcpy.BuildOverviews_management(self.fname, "", "NO_DEFINE_MISSING_TILES", "GENERATE_OVERVIEWS", "GENERATE_MISSING_IMAGES", "REGENERATE_STALE_IMAGES")
        DisplayMessages()

        # Count any incomplete or partial overviews
        BuildFootprintCount = self.mlayer.selectbyattribute("NEW_SELECTION", "Category > 2")
        if BuildFootprintCount > 0:
            arcpy.AddWarning ("{0} overviews failed to build correctly".format(BuildFootprintCount))

        return BuildFootprintCount

    def deletebadoverviews(self):
        with TempFeatureClass ("BadOverviews") as TmpTable:

            # Put list of bad overviews in table
            self.exportpaths(TmpTable, "Category > 2", "ALL", "RASTER")
            TmpTable.deleteidentical("SourceID")

            with arcpy.da.SearchCursor(TmpTable.fname, "Path") as bovers:
                for bover in bovers:
                    if arcpy.Exists (bover[0]):
                        arcpy.AddWarning ("Deleting {0}".format(bover[0]))
                        arcpy.Delete_management(bover[0])
                        DisplayMessages()
                    else:
                        arcpy.AddWarning ("Overview {0} not found".format(bover[0]))

    def buildoverviews_robust(self,max_retries=5):
        UnbuiltOverviewCount = self.buildoverviews()

        RetryCount = 1
        while UnbuiltOverviewCount > 0 and RetryCount < max_retries:
            self.deletebadoverviews()
            UnbuiltOverviewCount = self.buildoverviews()
            RetryCount += 1

        if UnbuiltOverviewCount > 0:
            arcpy.AddError ("Overviews failed to build correctly. Exiting...")
            raise arcpy.ExecuteError

    def deleteexternalrasters(self,sbound,ezone=None,bdist=None):
        tempbnd = TempFeatureClass("TempBnd")
        tempezn = TempFeatureClass("TempExclZone")

        # Select all rasters in the mosaic
        DeleteFootprintCount = self.mlayer.selectbyattribute("NEW_SELECTION", "")

        # Remove rasters that intersect the boundary after a buffer
        if bdist:
            sbound.buffer(tempbnd, bdist)
        else:
            sbound.copyfeatures(tempbnd)
        DeleteFootprintCount = self.mlayer.selectbylocation("INTERSECT", tempbnd, "", "REMOVE_FROM_SELECTION")

        # Add rasters that are completely within the exclusion zone to the selection after a buffer
        if ezone != None:
            if bdist:
                ezone.buffer(tempezn, bdist)
            else:
                tempezn = ezone
            DeleteFootprintCount = self.mlayer.selectbylocation("COMPLETELY_WITHIN", tempezn, "", "ADD_TO_SELECTION")

        # Remove Selected Rasters From Mosaic Dataset
        if DeleteFootprintCount > 0:
            self.mlayer.removerasters()



class TempMosaicDataset(MosaicDataset):
    def __init__(self,mname=None):
        super(TempMosaicDataset,self).__init__("TempMosaic" if mname == None else mname,tempmosaic=True)

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        super(TempMosaicDataset,self).__exit__(type,value,traceback)

    def __del__(self):
        super(TempMosaicDataset,self).__del__()



class Progressor(object):
    def __init__(self,type="default",label=None,min_range=0,max_range=100,step_value=1):
        self.type        = type
        self.label       = label
        self.min_range   = min_range
        self.max_range   = max_range
        self.step_value  = step_value
        self.position    = 0
        self.initialized = False

    def initialize(self):
        if not self.initialized:
            arcpy.SetProgressor(self.type, self.label, self.min_range, self.max_range, self.step_value)
            self.initialized = True

    def setdefaulttype(self):
        self.type = "default"
        if self.initialized:
            arcpy.SetProgressor(self.type, self.label, self.min_range, self.max_range, self.step_value)

    def setrange(self,min_range=0,max_range=100,step_value=1):
        self.type        = "step"
        self.min_range   = min_range
        self.max_range   = max_range
        self.step_value  = step_value
        if self.initialized:
            arcpy.SetProgressor(self.type, self.label, self.min_range, self.max_range, self.step_value)

    def setlabel(self,label):
        if self.label != label:
            self.label = label
            if not self.initialized:
                self.initialize()
            else:
                arcpy.SetProgressorLabel(self.label)

    def setposition(self,position):
        if self.position != position:
            self.position = position
            if not self.initialized:
                self.initialize()
            arcpy.SetProgressorPosition(self.position)

    def reset(self):
        if self.initialized:
            arcpy.ResetProgressor()

    def __enter__(self):
        return self

    def __exit__(self,type,value,traceback):
        self.reset()

    def __del__(self):
        self.reset()


