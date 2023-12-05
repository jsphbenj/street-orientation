import arcpy
import csv
import os

def worker(file):
    try:
        bins_dict = {}
        bin_key = r'C:\Users\Public\Documents\st_orientation_sample_data\bin_key.csv'
        zone_name_field = 'NAME'
        output_folder = r'C:\Users\Public\Documents\st_orientation_sample_data\Line_Bearings'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        line_bearing_output = os.path.join(output_folder, 'line_bearing_output.shp')
        if not os.path.exists(line_bearing_output):
            arcpy.CreateFeatureclass_management(output_folder, line_bearing_output)

        # Takes the key csv and reads it into a dictionary with the bin name, degree range, and the bearings
        # that fall into that classification

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
            select_expression = f'("fwd_bear" >= {value[0]} AND "fwd_bear" < {value[1]}) OR ' \
                                f'("back_bear" >= {value[0]} AND "back_bear" < {value[1]})'

            # Make Row Selections
            selection = arcpy.management.SelectLayerByAttribute(file, "NEW_SELECTION", select_expression)

            # Write number of selected rows to dictionary
            selection_count = int(arcpy.management.GetCount(selection).getOutput(0))
            bins_dict[key][2] = selection_count

        # Create New Fields in the Output Shapefile
        #todo add in HO and PHI (entropy indicator)
        bin_field_list = ['St_Bin_' + str(x) for x in range(1, 37)]

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
    except arcpy.ExecuteError:
        # Geoprocessor threw an error
        arcpy.AddError(arcpy.GetMessages(2))
        print("Execute Error:", arcpy.ExecuteError)