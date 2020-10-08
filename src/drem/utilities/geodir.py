import pandas as pd
import geopandas as gpd
from drem.filepaths import RAW_DIR
from drem.filepaths import PROCESSED_DIR
import shapely
import pyproj
import utm

df1 = pd.read_csv(RAW_DIR / 'DublinBuildingsData.csv', encoding= 'unicode_escape')
df1_dropped = df1.drop_duplicates()

df2 = pd.read_csv(RAW_DIR / 'GeodirSample.csv', encoding= 'unicode_escape')
df2_dropped = df2.drop_duplicates()

df3 = pd.merge(df1_dropped,df2_dropped,on='BUILDING_ID', how='right')
df3_dropped = df3.drop_duplicates()

buildings = gpd.read_file(RAW_DIR / 'prime2_no_z_2157.shp')
buildings_geo = gpd.GeoDataFrame(buildings)
buildings_final = buildings_geo.set_crs(epsg=2157)
buildings_final = buildings_final.to_crs(epsg=32629)
buildings_final = gpd.GeoDataFrame(geometry=gpd.GeoSeries(buildings_final['geometry']))
#buildings_flipped = buildings_final.geometry.map(lambda polygon: shapely.ops.transform(lambda x, y: (y, x), polygon))
buildings_cea = gpd.GeoDataFrame(buildings_final)

#Change floors to an integer, height can be a float
df_extracted = df3_dropped[['BUILDING_ID','BUILDING_HEIGHT', 'FLOORS', 'BUILDING_USE_y', 'ITM_EAST', 'ITM_NORTH']]
df_renamed = df_extracted.rename(columns={'BUILDING_ID': 'Name', 'BUILDING_HEIGHT' : 'height_ag', 'FLOORS': 'floors_ag', 'BUILDING_USE_y' : 'category'})
df_renamed['floors_bg'] = 0
df_renamed['height_bg'] = 0
df_ordered = df_renamed[['Name', 'height_ag', 'height_bg', 'floors_ag', 'floors_bg', 'category', 'ITM_EAST', 'ITM_NORTH']]
df_final = df_ordered.drop_duplicates()
gdf = gpd.GeoDataFrame(df_final, geometry=gpd.points_from_xy(df_final.ITM_EAST, df_final.ITM_NORTH))

#Setting at Irish grid and converting to UTM
gdf = gdf.set_crs(epsg=2157) 
gdf = gdf.to_crs(epsg=32629)

points_in_poly = gpd.sjoin(buildings_cea, gdf, op='contains') 
df_cea = points_in_poly[['Name','height_ag', 'height_bg', 'floors_ag', 'floors_bg', 'geometry', 'category']]
df_cea['Name'] = 'B' + df_cea['Name'].astype(str)
df_cea_height = df_cea.drop(df_cea[df_cea['height_ag']/df_cea['floors_ag'] < 1].index)
df_cea_height = df_cea_height.reset_index()

#Move index to the far right and create a new index on the left
df_cea_extracted = df_cea_height[['Name','height_ag', 'height_bg', 'floors_ag', 'floors_bg', 'geometry', 'category']]
df_cea_extracted[['floors_ag', 'floors_bg']] = df_cea_extracted[['floors_ag', 'floors_bg']].astype(int)

df_output = df_cea_extracted.drop_duplicates('Name', keep='last')
#Zone shapefile
df_output.to_file(driver = 'ESRI Shapefile', filename = PROCESSED_DIR / 'ZoneCEA')
site = points_in_poly['geometry']
#Site shapefile
site.to_file(driver = 'ESRI Shapefile', filename = PROCESSED_DIR / 'SiteCEA')
#Create a surroundings shapefile with Name/height_ag/floors_ag/description/category/geometry
surroundings = df_output[['Name','height_ag', 'floors_ag', 'geometry', 'category']]
surroundings['description'] = 'none'
surroundings_ordered = surroundings[['Name', 'height_ag', 'floors_ag', 'description', 'category', 'geometry']]
surroundings_ordered.to_file(driver = 'ESRI Shapefile', filename = PROCESSED_DIR / 'SurroundingsCEA')
