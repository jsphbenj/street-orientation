# Created by Joseph Benjamin

import os, csv, time
import arcpy
import geopandas as gpd
import plotly.graph_objects as go
import plotly.io as pio

start_time = time.time()

arcpy.CheckOutExtension("3D")
arcpy.env.overwriteOutput = True


def line_bearing(roads_shp):
    bearing_fixer = '''def bearing_fixer(fieldname):
        if fieldname < -5:
            return fieldname + 360
        elif fieldname > 355:
            return fieldname - 360
        else:
            return fieldname'''

    # Add Fields
    arcpy.management.AddFields(roads_shp, [['fwd_bear', 'DOUBLE'],
                                           ['back_bear', 'DOUBLE']])
    print(arcpy.AddMessage("New Fields Created."))

    # Create Forward Bearing
    arcpy.management.CalculateGeometryAttributes(roads_shp, [["fwd_bear", "LINE_BEARING"]])

    # Create Backward Bearing
    arcpy.management.CalculateField(roads_shp, 'back_bear', 'bearing_fixer(!fwd_bear! - 180)', "PYTHON3",
                                    bearing_fixer)
    print(arcpy.AddMessage("Backward Bearing Calculated."))

    # Make Forward Bearing in the correct range for bin creation
    # -5 <= x <= 355
    arcpy.management.CalculateField(roads_shp, 'fwd_bear', 'bearing_fixer(!fwd_bear!)', "PYTHON3", bearing_fixer)
    print(arcpy.AddMessage("Forward Bearing Calculated."))


# Set the working directory to the correct folder
original_workspace = r'C:\Users\Public\Documents\st_orientation_sample_data'
# arcpy.env.workspace = arcpy.GetParametersAsText(0)
arcpy.env.workspace = original_workspace

# Read the shapefiles
zones = r'Zones\Tampa_Planning_Districts.shp'
# zones = arcpy.GetParametersAsText(1)

zone_name_field = 'NAME'
# The field to be used as the name for each zone
# zone_name = arcpy.GetParametersAsText(2)

streets = r'Streets\Road_Centerline.shp'
# streets = arcpy.GetParametersAsText(3)

output_folder = os.path.join(arcpy.env.workspace, r'Line_Bearings')
if not os.path.exists(output_folder):
    os.mkdir(output_folder)
# output_folder = arcpy.GetParametersAsText(5)

clipped_streets_output = os.path.join(output_folder, r'Clipped_Streets')
# clipped_streets_output = arcpy.GetParametersAsText(4)

# Create Final Output Layer
line_bearing_output = os.path.join(output_folder, 'line_bearing_output.shp')
arcpy.management.CopyFeatures(zones, line_bearing_output)

# Clip Streets to each Zone
with arcpy.da.SearchCursor(zones, ['SHAPE@', zone_name_field]) as cursor:
    for row in cursor:
        try:
            # New Feature Layer for each Zone
            zone_name = str(row[1])
            single_zone_output = f'memory\\{zone_name}'
            single_zone_where_clause = f"{zone_name_field} = '{zone_name}'"
            arcpy.management.MakeFeatureLayer(zones, single_zone_output, single_zone_where_clause)

            # Clip Streets to that Feature Layer
            if not os.path.exists(clipped_streets_output):
                os.makedirs(clipped_streets_output)
            arcpy.analysis.Clip(streets, single_zone_output, clipped_streets_output + r'\Clipped_Streets_' + zone_name +
                                r'.shp')

            # Add message indicating success
            print(arcpy.AddMessage("Zone: " + zone_name + "Streets Clipped Successfully."))
            #todo why does it print 'None' after??? (same w line 102)
        except:
            # Add message indicating failure
            print(arcpy.AddError("Zone: " + zone_name + "Streets NOT Clipped Successfully."))

# Calculate Line Bearings
for root, directories, files in os.walk(clipped_streets_output):
    for file in files:
        if file.endswith('.shp'):
            line_bearing(os.path.join(clipped_streets_output, file))

            # Delete Zone Feature Layer
            arcpy.management.Delete(single_zone_output)
            print(f'{file}: Line bearings calculated!')

# Takes the key csv and reads it into a dictionary with the bin name, degree range, and the bearings that fall into
# that classification
bins_dict = {}
bin_key = r'bin_key.csv'

with open(bin_key, 'r') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # skip header row
    for row in reader:
        bin_name = row[0]
        lower_bound = int(row[1])
        upper_bound = int(row[2])
        row_count = 0
        bins_dict[bin_name] = [lower_bound, upper_bound, row_count]
del bin_name, lower_bound, upper_bound, row_count
print('Bins created.')

# Calculate Bins for Each Clipped_Streets Shapefile
for root, directories, files in os.walk(clipped_streets_output):
    for file in files:
        if file.endswith('.shp'):
            try:
                arcpy.env.workspace = original_workspace
                print('Placing ' + file + ' into bins')

                # Extract Zone Name
                start_str = "Clipped_Streets_"
                end_str = ".shp"
                start_index = file.index(start_str) + len(start_str)
                end_index = file.index(end_str)
                zone_name = file[start_index:end_index]

                # Count Number of Rows for Each Bin
                for key, value in bins_dict.items():
                    # SQL Expressions
                    select_expression = f'("fwd_bear" >= {value[0]} AND "fwd_bear" < {value[1]}) OR ("back_bear" >= {value[0]} AND "back_bear" < {value[1]})'

                    # Make Row Selections
                    selection = arcpy.management.SelectLayerByAttribute(os.path.join(clipped_streets_output, file),
                                                                        "NEW_SELECTION", select_expression)

                    # Write number of selected rows to dictionary
                    selection_count = int(arcpy.management.GetCount(selection).getOutput(0))
                    bins_dict[key][2] = selection_count

                # Create New Field in the Output Shapefile
                bin_field_list = ['St_Bin_' + str(x) for x in range(1, 37)]

                add_bin_fields_list = [[field, 'DOUBLE'] for field in bin_field_list]
                add_bin_fields_list.append(['Total', 'DOUBLE'])

                arcpy.management.AddFields(line_bearing_output, add_bin_fields_list)

                where_clause = f"{zone_name_field} = '{zone_name}'"
                cursor_fields = bin_field_list + ['Total']
                with arcpy.da.UpdateCursor(line_bearing_output, cursor_fields, where_clause) as cursor:
                    for row in cursor:
                        total = 0
                        for field in bin_field_list:
                            bin_num = field.replace('St_Bin_', '')
                            # Sets the field of that row to the
                            row[int(bin_num) - 1] = bins_dict[bin_num][2]
                            total += int(bins_dict[bin_num][2])
                        row[36] = total
                        cursor.updateRow(row)
            finally:
                arcpy.AddMessage("On to the next step!")
# Create Polar Histogram
output_gdf = gpd.read_file(os.path.join(original_workspace, line_bearing_output))
for index, row in output_gdf.iterrows():
    counter = 1
    radii = []
    theta = []

    while counter <= 36:
        bin_value = output_gdf['St_Bin_' + str(counter)][index]
        bin_proportion = bin_value / output_gdf['Total'][index]
        radii.append(bin_proportion)
        counter += 1
    fig = go.Figure(go.Barpolar(
        r=radii,
        theta0=0,
        dtheta=10,
        marker_color="black",
        opacity=0.6
    ))

    fig.update_layout(
        template=None,
        title=output_gdf[zone_name_field][index],
        font_size=30,
        polar=dict(
            radialaxis=dict(range=[0, max(radii)], showticklabels=False, ticks=''),
            angularaxis=dict(tickmode='array',
                             tickvals=[0, 90, 180, 270],
                             ticktext=['N', 'E', 'S', 'W'],
                             direction='clockwise',
                             gridwidth=0
                             )
        )
    )

    file_name = output_folder + 'p_hist_' + output_gdf[zone_name_field][index].replace(' ', '_') + '.png'
    pio.write_image(fig, file_name)
    print('File Created:' + output_gdf[zone_name_field][index])

print("--- %s seconds ---" % (time.time() - start_time))
