# Creating Polar Histograms of City Street Orientation
## Description
This python script and ArcGIS Pro script tool takes the line bearing of every street and visualizes the proportion going in certain directions via a polar histogram and shapefile outputs. This is applicable for analyzing street network orientations. A city with a perfectgridiron street form will be represented by a polar histogram with a cross shape (all streets go in one of two directional pairs). The opposite, a city with streets' endpoints' bearing going in all different directions would be represented by a polar histogram with bars creating a rounder shape. The closer to a circular form the polar histogram's bars are, the close the city is to equal proportions of streets going in every direction. 

## How it Works
x

### Parameters
The format is as follows:
Python Object(Script Tool Parameter): Description

zones (Zones Feature Class): The polygon boundaries that will be used to delineate the area that each polar histogram is describing. The test data uses the Elementary School Zones in Gainesville, Florida, but zones could be on a neighborhood- or city-wide scale as well. 

zone_name_field (Zone Name Field): This is the name of the field that describes each zone, as found in the feature class's attribute table. The test data will use the 'code_elem' field as a descriptor, but city/neighborhood names could also be used depending on the zone feature class. 

streets (Streets Feature Class): The streets to be processed for their orientation. The test data (for the sake of run-time speed), uses the FDOT Roads Characteristics inventory (RCI) dataset already clipped to Gainesville, FL and will therefore not result in a fully accurate output polar histogram, as non-major local roads are excluded. 

output_folder_name (Output Folder Name): Where the outputs will be stored. If there is not an existing folder already, a new one will be created in the location specified. 

### Outputs
Output Path > polar_histograms >
+ Polar Histogram JPGs will be created inside this folder.
 

Output Path > Clipped_Streets >
+ Individual shapefiles of the streets layer clipped to each zone. 

## Skills Used
+ Python Modules: arcpy (to run geoprocessing), plotly (to create the polar histograms), and geopandas (here, solely to read the shapefile as a GDF before creating the polar histogram with plotly)
+ Multiprocessing: each "zone" (city, neighborhood, zip code, etc) is sent to a CPU as a 'task'
+ Creating a ArcGIS Toolbox
 
## How to use
Either download and run the arcpy, hardcoding the input variables, or use the ArcGIS script tool. This achieves the same output within the ArcGIS Pro application. 
Ensure that the bin_key.csv is in the local drive to correctly run. 

## Testing
