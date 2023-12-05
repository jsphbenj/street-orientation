# Created 11 July 2023 by Joseph Benjamin
# todo make the messages make more sense

import os, csv, time
import sys
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
                    if not os.path.exists(clipped_streets_output):
                        os.makedirs(clipped_streets_output)
                    arcpy.analysis.Clip(streets, single_zone_output,
                                        os.path.join(clipped_streets_output, r'Clipped_Streets_' + zone_name + r'.shp'))

                    # Add message indicating success
                    print(arcpy.AddMessage("Zone: " + zone_name + " Streets Clipped Successfully."))

                except:
                    # Add message indicating failure
                    print(arcpy.AddError("Zone: " + zone_name + " Streets NOT Clipped Successfully."))

        # Calculate Line Bearings
        for root, directories, files in os.walk(clipped_streets_output):
            for file in files:
                if file.endswith('.shp'):
                    line_bearing(os.path.join(clipped_streets_output, file))

                    # Delete Zone Feature Layer from Memory
                    arcpy.management.Delete(single_zone_output)
                    print(f'{file}: Line bearings calculated!')

        # Create New Field in the Output Shapefile
        bin_field_list = ['St_Bin_' + str(x) for x in range(1, 37)]

        add_bin_fields_list = [[field, 'DOUBLE'] for field in bin_field_list]
        add_bin_fields_list.append(['Total', 'DOUBLE'])

        arcpy.management.AddFields(line_bearing_output, add_bin_fields_list)

        # Calculate Bins for Each Clipped_Streets Shapefile Using Multiprocessing
        print("")
        arcpy.AddMessage("Beginning multiprocessing stage: counting line bearing results for each zone"
                         " and making associated calculations.")

        # 1. Create task list with parameter tuples
        jobs = []
        for root, directories, files in os.walk(clipped_streets_output):
            for file in files:
                if file.endswith('.shp'):
                    full_path = os.path.join(root, file)
                    jobs.append(full_path)
        # jobs = [(x, str(zone_name_field), str(line_bearing_output)) for x in jobs]

        arcpy.AddMessage("Job list has " + str(len(jobs)) + " elements.")

        # 2. Create and Run Multiprocessing Pool
        multiprocessing.set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))

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
    output_gdf = gpd.read_file(os.path.join(line_bearing_output))
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
        file_loc = os.path.join(output_folder, 'polar_histograms')
        if not os.path.exists(file_loc):
            os.makedirs(file_loc)

        pio.write_image(fig, os.path.join(file_loc, file_name))
        print('File Created:' + output_gdf[zone_name_field][index])


if __name__ == '__main__':
    mp_handler()
    print(arcpy.AddMessage("--- %s seconds ---" % (time.time() - start_time)))
