import os, csv, time
import arcpy
arcpy.env.overwriteOutput = True


# Read the shapefiles
zones = r'C:\Users\joseph.benjamin\OneDrive - University of Florida\GIS\street_network\Lesson1_Assignment\Zones_FC\Elem_Zones_NAD.shp'
# zones = arcpy.GetParametersAsText(1)

zone_name_field = 'code_elem'
# The field to be used as the name for each zone
# zone_name = arcpy.GetParametersAsText(2)

streets = r'C:\Users\joseph.benjamin\OneDrive - University of Florida\GIS\street_network\Lesson1_Assignment\GNV_Roads_FC\rciroads_jul23\rciroads_jul23.shp'
# streets = arcpy.GetParametersAsText(3)

output_folder = r'C:\Users\joseph.benjamin\OneDrive - University of Florida\GIS\street_network\Lesson1_Assignment\TEST1'
clipped_streets_output = os.path.join(output_folder, r'Clipped_Streets')

# Create Final Output Layer
line_bearing_output = os.path.join(output_folder, 'line_bearing_output.shp')
arcpy.management.CopyFeatures(zones, line_bearing_output)


# Clip Streets to each Zone
with arcpy.da.SearchCursor(zones, ['SHAPE@', zone_name_field]) as cursor:
    for row in cursor:

        # New Feature Layer for each Zone
        zone_name = str(row[1])
        single_zone_output = f'memory\\{zone_name}'
        single_zone_where_clause = f"{zone_name_field} = '{zone_name}'"
        arcpy.management.MakeFeatureLayer(zones, single_zone_output, single_zone_where_clause)

        # Clip Streets to that Feature Layer
        arcpy.analysis.Clip(streets, single_zone_output,
                            os.path.join(clipped_streets_output, r'Clipped_Streets_' + zone_name + r'.shp'))

