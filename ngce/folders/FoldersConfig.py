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

las_dir = "LAS"

lasUnclassified_dir = "LAS_UNCLASSIFIED"
zlasUnclassified_dir = "ZLAS_UNCLASSIFIED"

lasInvalid_dir = "LAS_INVALID"

lasClassified_dir = "LAS_CLASSIFIED"
zlasClassified_dir = "ZLAS_CLASSIFIED"

contour_dir = "CONTOUR"

#@TODO Move text to other config file..
DTM = "DTM"
DSM = "DSM"
DHM = "DHM"

demFirst_dir = DSM
demFirstTiff_dir = "TIF_{}".format(DSM)
demLast_dir = DTM
demLastTiff_dir = "TIF_{}".format(DTM)
demHeight_dir = DHM
demHeightTiff_dir = "TIF_{}".format(DHM)

intensity_dir = "INTENSITY"
intensityFirst_dir = "INTENSITY_FIRST"
intensityLast_dir = "INTENSITY_LAST"

tileIndex_dir = "TILE_INDEX"
tiles_dir = "TILES"
boundary_dir = "BOUNDARY"

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




