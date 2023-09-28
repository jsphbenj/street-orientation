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
    # = r"C:\Users\joseph.benjamin\OneDrive - University of Florida\GIS\street_network\Lesson1_Assignment\Zones_FC\Elem_Zones_NAD.shp"
    zones = arcpy.GetParameterAsText(0)

    # zone_name_field = "code_elem"
    zone_name_field = arcpy.GetParameterAsText(1)

    # streets = r"C:\Users\joseph.benjamin\OneDrive - University of Florida\GIS\street_network\Lesson1_Assignment\GNV_Roads_FC\rciroads_jul23\rciroads_jul23.shp"
    streets = arcpy.GetParameterAsText(2)

    # Create Output Folders
    # output_folder = r"C:\Users\joseph.benjamin\Documents\GeoPlan\GEOG489\Lesson1\L1_Proj\TEST1"
    output_folder = arcpy.GetParameterAsText(3)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
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
                    if not os.path.exists(clipped_streets_output):
                        os.makedirs(clipped_streets_output)
                    arcpy.analysis.Clip(streets, single_zone_output,
                                        os.path.join(clipped_streets_output, r'Clipped_Streets_' + zone_name + r'.shp'))

                    # Delete Zone Feature Layer from Memory
                    arcpy.management.Delete(single_zone_output)

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
                    print(f'{file}: Line bearings calculated!')

        # Takes the key csv and reads it into a dictionary with the bin name, degree range, and the bearings
        # that fall into that classification

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
        jobs = [(x, str(zone_name_field), str(line_bearing_output)) for x in jobs]

        arcpy.AddMessage("Job list has " + str(len(jobs)) + " elements.")

        # 2. Create and Run Multiprocessing Pool
        multiprocessing.set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))

        arcpy.AddMessage("Sending to pool")
        cpu_num = multiprocessing.cpu_count()  # determine number of cores to use
        print("there are: " + str(cpu_num) + " cpu cores on this machine")

        with multiprocessing.Pool(processes=cpu_num) as pool:
            res = pool.starmap(worker, jobs)

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
