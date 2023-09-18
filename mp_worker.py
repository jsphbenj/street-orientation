import arcpy


def worker(file):
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
        select_expression = f'("fwd_bear" >= {value[0]} AND "fwd_bear" < {value[1]}) OR ' \
                            f'("back_bear" >= {value[0]} AND "back_bear" < {value[1]})'

        # Make Row Selections
        selection = arcpy.management.SelectLayerByAttribute(file, "NEW_SELECTION", select_expression)

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