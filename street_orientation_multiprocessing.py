# Created 11 July 2023 by Joseph Benjamin

import os, csv, time
import arcpy
import geopandas as gpd
import plotly.graph_objects as go
import plotly.io as pio
import multiprocessing
from mp_worker import worker

start_time = time.time()

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


def mp_handler():
    # Read the shapefiles
    zones = arcpy.GetParameterAsText(0)

    zone_name_field = arcpy.GetParameterAsText(1)

    streets = arcpy.GetParameterAsText(2)

    # Create Output Folders
    output_folder = arcpy.GetParameterAsText(3)
    clipped_streets_output = os.path.join(output_folder, r'Clipped_Streets')

    line_bearing_output = os.path.join(output_folder, 'line_bearing_output.shp')
    arcpy.management.CopyFeatures(zones, line_bearing_output)

    try:
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
                    arcpy.analysis.Clip(streets, single_zone_output,
                                        os.path.join(clipped_streets_output, r'Clipped_Streets_' + zone_name + r'.shp'))
                    # todo I think what i'll have to do here is fully CREATE a new folder within the output folder

                    # Delete Zone Feature Layer from Memory
                    arcpy.management.Delete(single_zone_output)

                    # Add message indicating success
                    print(arcpy.AddMessage("Zone: " + zone_name + "Streets Clipped Successfully."))

                except:
                    # Add message indicating failure
                    print(arcpy.AddError("Zone: " + zone_name + "Streets NOT Clipped Successfully."))

        # Calculate Line Bearings
        for root, directories, files in os.walk(clipped_streets_output):
            for file in files:
                if file.endswith('.shp'):
                    line_bearing(os.path.join(clipped_streets_output, file))
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

        # Calculate Bins for Each Clipped_Streets Shapefile Using Multiprocessing
        print("")
        arcpy.AddMessage("Beginning multiprocessing stage: counting line bearing results for each zone"
                         " and making associated calculations.")

        # 1. Create task list with parameter tuples
        jobs = []
        for root, directories, files in os.walk(clipped_streets_output):
            for file in files:
                if file.endswith('.shp'):
                    jobs.append(file)
        arcpy.AddMessage("Job list has " + str(len(jobs)) + " elements.")

        # 2. Create and Run Multiprocessing Pool
        arcpy.AddMessage("Sending to pool")
        cpu_num = multiprocessing.cpu_count()  # determine number of cores to use
        print("there are: " + str(cpu_num) + " cpu cores on this machine")

        with multiprocessing.Pool(processes=cpu_num) as pool:
            res = pool.map(worker, jobs)

        failed = res.count(False)  # count how many times False appears in the list with the return values
        if failed > 0:
            arcpy.AddError("{} workers failed!".format(failed))
            print("{} workers failed!".format(failed))

        arcpy.AddMessage("Finished multiprocessing!")
        print("Finished multiprocessing!")

    except arcpy.ExecuteError:
        # Geoprocessor threw an error
        arcpy.AddError(arcpy.GetMessages(2))
        print("Execute Error:", arcpy.ExecuteError)
    except Exception as e:
        # Capture all other errors
        arcpy.AddError(str(e))
        print("Exception:", e)

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

        file_name = output_gdf[zone_name_field][index].replace(' ', '_') + '_p_hist.png'
        file_loc = os.path.join(output_folder, 'polar_histograms', file_name)
        pio.write_image(fig, file_loc)
        print('File Created:' + output_gdf[zone_name_field][index])


if __name__ == '__main__':
    mp_handler()
    print("--- %s seconds ---" % (time.time() - start_time))
