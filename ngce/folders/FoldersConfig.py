'''
Created on Dec 21, 2015

@author: eric5946
'''
chars = ["'"," ", "\\", "/", ":", "*", "?", "<", ">", "|", "_"]
repls = ["" ,"" , ""  , "" , "" , "" , "" , "" , "" , "" , "-"]
    



# Main (top level) folders
original_dir = "ORIGINAL"
delivered_dir = "DELIVERED"
metadata_dir = "METADATA"
derived_dir = "DERIVED"
published_dir = "PUBLISHED"
qa_dir = "QAQC"

# Second level directories
breaks_dir = "BREAKS"
control_dir = "CONTROL"

LAS = "LAS"
RASTER = "RASTER"
las_dir = LAS
raster_dir = RASTER
lasd_dir = LAS + "D"

lasUnclassified_dir = LAS + "_UNCLASSIFIED"
zlasUnclassified_dir = "Z" + LAS + "_UNCLASSIFIED"

lasInvalid_dir = LAS + "_INVALID"

lasClassified_dir = LAS + "_CLASSIFIED"
zlasClassified_dir = "Z" + LAS + "_CLASSIFIED"

contour_dir = "CONTOUR"
SCRATCH = "SCRATCH"

#@TODO Move text to other config file..
DTM = "DTM"
DSM = "DSM"
DHM = "DHM"
DLM = "DLM"
DCM = "DCHM"
INT = "INTENSITY"
ELEVATION = "ELEVATION"

FIRST = "FIRST"
LAST = "LAST"
ALAST = "ALAST"
ALL = "ALL"

elev_dir = ELEVATION
demFirst_dir = DSM
demFirstTiff_dir = "TIF_{}".format(DSM)
demLast_dir = DTM
demLastTiff_dir = "TIF_{}".format(DTM)
demHeight_dir = DHM
demHeightTiff_dir = "TIF_{}".format(DHM)
demLAll_dir = DLM


intensity_dir = INT
intensityFirst_dir = INT + "_" + FIRST
intensityLast_dir = INT + "_" + LAST

tileIndex_dir = "TILE_INDEX"
tiles_dir = "TILES"
boundary_dir = "BOUNDARY"

stats_dir = "STATS"
counts_dir = "COUNTS"
iRanges_dir = "I_RANGES"
zRanges_dir = "Z_RANGES"
predominant_dir = "PREDOMINANT"
stats_dir = "STATS"

pulse_count_dir = "PULSE_COUNT"
point_count_dir = "POINT_COUNT"
predominant_last_return_dir = "PREDOMINANT_LAST_RETURN"
predominant_class_dir = "PREDOMINANT_CLASS"
intensity_range_dir = "INTENSITY_RANGE"
z_range_dir = "Z_RANGE"


STATS_METHODS = [pulse_count_dir, point_count_dir, predominant_last_return_dir, predominant_class_dir, intensity_range_dir, z_range_dir]
DATASET_NAMES = ['_' + ALL, "_" + FIRST, "_" + LAST]

