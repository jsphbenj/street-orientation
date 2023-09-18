# Created 11 July 2023 by Joseph Benjamin

import os, pyproj, csv, math
import geopandas as gpd
import plotly.graph_objects as go
import plotly.io as pio

def line_bearing(gdf):
    index = 0
    for geometry in gdf.geometry:
        # Treats MLSs different from typical LineStrings
        if geometry.geom_type == 'MultiLineString':
            gdf.at[index, 'fwd_bear'] = ''
            gdf.at[index, 'back_bear'] = ''
        else:
            x, y = geometry.coords.xy
            long1, lat1 = x[0], y[0]
            long2, lat2 = x[-1], y[-1]
            geodesic = pyproj.Geod(ellps='WGS84')
            fwd_bearing, back_bearing, distance = geodesic.inv(long1, lat1, long2, lat2)

            # Make it so they are both positive
            if fwd_bearing < -5:
                fwd_bearing += 360
            elif fwd_bearing > 355:
                fwd_bearing -= 360
            if back_bearing < -5:
                back_bearing += 360
            elif back_bearing > 355:
                back_bearing -= 360

            # Create new columns in the GeoDataFrame
            gdf.at[index, 'fwd_bear'] = fwd_bearing
            gdf.at[index, 'back_bear'] = back_bearing

        index += 1

# Set the working directory to the street_orientation folder
os.chdir(r'C:\Users\joeyb\OneDrive\Public\Documents\GitHub\wsud-alexandria\street_orientation')

# Read the shapefiles
basin_bounds = gpd.read_file(r'downloads\basins\basins.shp')
all_roads = gpd.read_file(r'downloads\Roads_all\Roads_all.shp')

# 4. CLIP STREETS TO EACH MICROBASIN
for index, row in basin_bounds.iterrows():
    # perform the clip function
    clipped = gpd.clip(all_roads, row.geometry)
    print('Clip success!')
    clipped = clipped[clipped.geometry.type != 'Point']

    # put that clip into a new file
    mb_number = row['Name'].replace("MB ", "")
    file_name = 'C:\\Users\\joeyb\\OneDrive\\Public\\Documents\\GitHub\\wsud-alexandria\\street_orientation\\outputs\\' \
                'roads_clipped_by_mb\\clipped_roads_' + mb_number + '.shp'

    clipped.to_file(file_name)
    print('Created: ' + file_name)
print('Streets Clipped!')
print()


# 5. Calculate Line Bearings
for root, directories, files in os.walk(r'C:\Users\joeyb\OneDrive\Public\Documents\GitHub\wsud-alexandria\street_orientation\outputs\roads_clipped_by_mb'):
        for file in files:
            if file.endswith('.shp'):
                # Removing irrelevant columns
                gdf = gpd.read_file('C:\\Users\\joeyb\\OneDrive\\Public\\Documents\\GitHub\\wsud-alexandria\\street_orientation\\outputs\\roads_clipped_by_mb\\' + file)
                print('Processing ' + file + ':')
                columns_list = gdf.columns.tolist()
                removefromlist = ["full_id", "osm_id", "osm_type", "alt_name_e", "name_en", "oneway", "geometry",
                                  "highway"]
                updated_list = [x for x in columns_list if x not in removefromlist]
                gdf = gdf.drop(columns=updated_list)
                print('Irrelevant columns dropped.')

                # Line bearings calculated with function above
                line_bearing(gdf)
                print('Line bearing calculated.')

                # After the line bearings have been calculated, save the gdf to file
                mb_number = file.replace('clipped_roads_', '')
                new_file_path = r'C:\Users\joeyb\OneDrive\Public\Documents\GitHub\wsud-alexandria\street_orientation' \
                                r'\outputs\shp_w_line_bearings\line_bearing_' + mb_number
                gdf.to_file(new_file_path)
                print('New file created with line bearing.')
                print('')

# 6. Calculate Bins in MB Layers
# 6.1. Read csv and create dictionary based off of it with the bin name, degree range, and the bearings that fall into
#      that classification. The bearings_list was kept empty so this dictionary can be reused.
bins_dict = {}
bin_key_path = r'C:\Users\joeyb\OneDrive\Public\Documents\GitHub\wsud-alexandria\street_orientation\outputs\bin_key.csv'

library = 'C:\\Users\\joeyb\\OneDrive\\Public\\Documents\\GitHub\\wsud-alexandria\\street_orientation\\outputs' \
          '\\shp_w_line_bearings\\'
for root, directories, files in os.walk(library):
    for file in files:
        if file.endswith('.shp'):
            bearings_shp = gpd.read_file(library + file)
            print('Placing ' + file + ' into bins')

            # extract the basin number
            start_str = "line_bearing_"
            end_str = ".shp"

            start_index = file.index(start_str) + len(start_str)
            end_index = file.index(end_str)

            mb_number = file[start_index:end_index]

            # 6.2. Takes the key csv and reads it into a dictionary
            with open(bin_key_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # skip header row
                for row in reader:
                    bin_name = row[0]
                    mini = int(row[1])
                    maxi = int(row[2])
                    bearings_list = []
                    bins_dict[bin_name] = (mini, maxi, bearings_list)
            del bin_name, mini, maxi, bearings_list
            print('Bins created.')

            for index, row in bearings_shp.iterrows():
                done_check = 0
                # place the fwd_bearing into the appropriate bearing_list in the dictionary

                bearing1 = float(bearings_shp.at[index, 'fwd_bear'])
                bearing2 = float(bearings_shp.at[index, 'back_bear'])
                for bin in bins_dict:

                    mini = bins_dict[bin][0]
                    maxi = bins_dict[bin][1]

                    if math.isnan(bearing1):
                        print('MultiLineString @ ' + str(index))
                        done_check += 2
                    elif mini <= int(round(bearing1)) < maxi:
                        bins_dict[bin][2].append(bearing1)
                        done_check += 1
                    elif mini <= int(round(bearing2)) < maxi:
                        bins_dict[bin][2].append(bearing2)
                        done_check += 1

                    if done_check == 2:
                        break

            # Create new columns in basins boundary shp for each bin.
            mb_name = 'MB ' + mb_number
            mb_index = basin_bounds.index[basin_bounds['Name'] == mb_name][0]
            dict_length_total = 0
            for bin in bins_dict:
                dict_length_total += int(len(bins_dict[bin][2]))
                basin_bounds.at[mb_index, bin] = int(len(bins_dict[bin][2]))
            basin_bounds.at[mb_index, 'Total'] = dict_length_total
            print('Bearings placed in bins for ' + file)
            print(str(dict_length_total) + ' ~ ' + str(index) + 'x 2  ?')
            print('')

#       Save to new csv, shp
basin_bins_name = r'C:\Users\joeyb\OneDrive\Public\Documents\GitHub\wsud-alexandria\street_orientation' \
                  r'\outputs\basin_bins\basin_bins'
basin_bounds.to_file(basin_bins_name + '.shp')
basin_bounds.to_csv(basin_bins_name + '.csv')

# 7. Use Bins to Create Polar Histogram

basin_bins = gpd.read_file(r'C:\Users\joeyb\OneDrive\Public\Documents\GitHub\wsud-alexandria\street_orientation'
                           r'\outputs\basin_bins\basin_bins.shp')
for index, row in basin_bins.iterrows():
    counter = 1
    radii = []
    theta = []

    while counter <= 36:
        bin_value = basin_bins[str(counter)][index]
        bin_proportion = bin_value / basin_bins['Total'][index]
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
        title = basin_bins['Name'][index],
        font_size=30,
        polar = dict(
            radialaxis = dict(range=[0, max(radii)], showticklabels=False, ticks=''),
            angularaxis = dict(tickmode='array',
                               tickvals=[0, 90, 180, 270],
                               ticktext=['N', 'E', 'S', 'W'],
                               direction='clockwise',
                               gridwidth=0
                               )
        )
    )

    file_name = r'C:\Users\joeyb\OneDrive\Public\Documents\GitHub\wsud-alexandria\street_orientation\outputs' \
                r'\polar_histograms\p_hist_' + basin_bins['Name'][index].replace(' ', '_') + '.png'
    pio.write_image(fig, file_name)
    print('File Created:' + basin_bins['Name'][index])
