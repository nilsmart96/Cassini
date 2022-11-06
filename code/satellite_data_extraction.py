# Import of necessary libraries.
import ee
import math
import numpy as np
import pandas as pd

# Connecting to earth engine API to retrieve satellite data.
ee.Authenticate()
ee.Initialize()

def air_info_collection(lat, lng, date_start, date_stop, square):

    """
    Obtain NO2 values for square around given location starting
    mid 2018 and reaching to mid 2022. An inner sqaure shows the 
    pollution source, while an outer one is used as a reference.

    :param lat: Latitude of the location
    :param lng: Longitude of the location
    :param date_start: Start month/year of image collection
    :param date_end: End month/year of image collection
    :param square: Choice of outer or inner square

    :return float: Sum of pixel values in the square and of
                   all images that are inside the time frame.
    """
    
    if square == "inner":
        shape = ee.Geometry.BBox(lng - 0.15, lat - 0.1, lng + 0.15, lat + 0.1)
    if square == "outer":
        shape = ee.Geometry.BBox(lng - 0.15 * math.sqrt(2), lat - 0.1 * math.sqrt(2
        ), lng + 0.15 * math.sqrt(2), lat + 0.1 * math.sqrt(2))
    
    dataset = ee.ImageCollection("COPERNICUS/S5P/NRTI/L3_NO2"
    ).select("tropospheric_NO2_column_number_density"
    ).filterDate(str(date_start) + "-01", str(date_stop) + "-01"
    ).filterBounds(shape)

    def setProperty(image):
        dict = image.reduceRegion(ee.Reducer.sum(), shape)
        return image.set(dict)

    pixel_values = dataset.map(setProperty)
    aggregate_pixel_values = (pixel_values.aggregate_array("tropospheric_NO2_column_number_density"
    ).getInfo())
    sum_pixel_value = np.sum(aggregate_pixel_values)

    return sum_pixel_value

# Dictionary of companies/locations that are checked for this example. 
# This can easily be extended by using databases on this kind of information.
exmpl_dict = [
    {"Name": "BASF", "lat": 49.5142, "lng": 8.4250},
    {"Name": "Thyssen", "lat": 51.4871, "lng": 6.7185},
    {"Name": "MiRO", "lat": 49.0576, "lng": 8.3250},
    {"Name": "Boxberg", "lat": 51.4213, "lng": 14.5802},
    {"Name": "Belchatow", "lat": 51.2669, "lng": 19.3266},
    {"Name": "Baymina", "lat": 39.7769, "lng": 32.4022},
    {"Name": "Formosa", "lat": 23.7829, "lng": 120.1929},
    {"Name": "NTPC", "lat": 24.0273, "lng": 82.7901}
    ]

# Dates that Sentinel 5P has data on, split in half-year intervals to reduce
# seasonality.
date_list_1 = [
    "2019-01", "2019-07", "2020-01", "2020-07", 
    "2021-01", "2021-07", "2022-01", "2022-07"
    ]

date_list_2 = [
    "2018-07", "2019-01", "2019-07", "2020-01",
    "2020-07", "2021-01", "2021-07", "2022-01"
    ]

square_list = ["outer", "inner"]

# Loop that iterates through the dictionary and uses the function above
# to retrieve data. In a final product the dictionary is searched for
# the firm in the user input and just the data on that firm is retrieved.
j = 0
while j < len(square_list):

    for entity in exmpl_dict:

        i = 0
        while i < len(date_list_1) - 1:
            result_1 = air_info_collection(lat=entity["lat"], lng=entity["lng"], 
            date_start=date_list_1[i], date_stop=date_list_1[i + 1], square=square_list[j])
            if i == 0 and j == 0:
                entity["2019_outer_1"] = result_1
            if i == 2 and j == 0:
                entity["2020_outer_1"] = result_1
            if i == 4 and j == 0:
                entity["2021_outer_1"] = result_1
            if i == 6 and j == 0:
                entity["2022_outer_1"] = result_1
            if i == 0 and j == 1:
                entity["2019_inner_1"] = result_1
            if i == 2 and j == 1:
                entity["2020_inner_1"] = result_1
            if i == 4 and j == 1:
                entity["2021_inner_1"] = result_1
            if i == 6 and j == 1:
                entity["2022_inner_1"] = result_1

            result_2 = air_info_collection(lat=entity["lat"], lng=entity["lng"], 
            date_start=date_list_2[i], date_stop=date_list_2[i + 1], square=square_list[j])
            if i == 0 and j == 0:
                entity["2018_outer_2"] = result_2
            if i == 2 and j == 0:
                entity["2019_outer_2"] = result_2
            if i == 4 and j == 0:
                entity["2020_outer_2"] = result_2
            if i == 6 and j == 0:
                entity["2021_outer_2"] = result_2
            if i == 0 and j == 1:
                entity["2018_inner_2"] = result_2
            if i == 2 and j == 1:
                entity["2019_inner_2"] = result_2
            if i == 4 and j == 1:
                entity["2020_inner_2"] = result_2
            if i == 6 and j == 1:
                entity["2021_inner_2"] = result_2

            i += 2
    j += 1

# Storing the results in a dataframe and some further processing.
df_res = pd.DataFrame(exmpl_dict)

df1 = ((df_res['2019_inner_1'].mul(2).sub(df_res['2019_outer_1'])
) / df_res['2019_outer_1'].sub(df_res['2019_inner_1'])).to_frame('2019_1')
df2 = ((df_res['2018_inner_2'].mul(2).sub(df_res['2018_outer_2'])
) / df_res['2018_outer_2'].sub(df_res['2018_inner_2'])).to_frame('2018_2')
df3 = ((df_res['2020_inner_1'].mul(2).sub(df_res['2020_outer_1'])
) / df_res['2020_outer_1'].sub(df_res['2020_inner_1'])).to_frame('2020_1')
df4 = ((df_res['2019_inner_2'].mul(2).sub(df_res['2019_outer_2'])
) / df_res['2019_outer_2'].sub(df_res['2019_inner_2'])).to_frame('2019_2')
df5 = ((df_res['2021_inner_1'].mul(2).sub(df_res['2021_outer_1'])
) / df_res['2021_outer_1'].sub(df_res['2021_inner_1'])).to_frame('2021_1')
df6 = ((df_res['2020_inner_2'].mul(2).sub(df_res['2020_outer_2'])
) / df_res['2020_outer_2'].sub(df_res['2020_inner_2'])).to_frame('2020_2')
df7 = ((df_res['2022_inner_1'].mul(2).sub(df_res['2022_outer_1'])
) / df_res['2022_outer_1'].sub(df_res['2022_inner_1'])).to_frame('2022_1')
df8 = ((df_res['2021_inner_2'].mul(2).sub(df_res['2021_outer_2'])
) / df_res['2021_outer_2'].sub(df_res['2021_inner_2'])).to_frame('2021_2')

df_first_half = df1.merge(df3,left_index=True, right_index=True
).merge(df5,left_index=True, right_index=True
).merge(df7,left_index=True, right_index=True)

df_second_half = df2.merge(df4,left_index=True, right_index=True
).merge(df6,left_index=True, right_index=True
).merge(df8,left_index=True, right_index=True)

# Specifying where to save the results.
path = r"C:\Users\nmart\OneDrive - fs-students.de\Dokumente\Office\Sonstiges\Hackathlon\Cassini\data"

df_first_half.to_csv(path + r"\first_half_data.csv")
df_second_half.to_csv(path + r"\second_half_data.csv")

print("Data extraction finished, saved files to the following path:")
print(path)