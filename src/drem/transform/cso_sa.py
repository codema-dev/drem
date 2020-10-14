Spatially linking BER dataset to Geodir on SA level
ber = pd.read_parquet('BER_Closed.parquet')
ber_dub = ber[ber['CountyName2'].str.contains("DUBLIN")]
ber_dub = ber_dub.drop_duplicates()
ber_extracted = ber_dub[["cso_small_area","CountyName2", "Year of construction", "Year of construction range","Dwelling type description","Energy Rating"]]

census = gpd.read_file('Census2011_Small_Areas_generalised20m.shp')
census_extracted = census[["SMALL_AREA","geometry"]]
census_renamed = census_extracted.rename(columns={"SMALL_AREA": "cso_small_area"})

ber_sa = pd.merge(ber_extracted, census_renamed, on="cso_small_area", how="inner")
ber_sa.sort_values('cso_small_area')

ber_sa['Dwelling Group'] = ber_sa['Dwelling type description'].map({
                        'Mid floor apt.' : 'Apartment',
                        'Top-floor apt.' : 'Apartment', 
                        'Apt.' : 'Apartment', 
                        'Maisonette' : 'Apartment',
                        'Grnd floor apt.' : 'Apartment',
                        'Semi-det. house' : 'Semi detatched house',
                        'House' : 'Semi detatched house',
                        'Det. house' : 'Detatched house',
                        'Mid terrc house' : 'Terraced house',
                        'End terrc house' : 'Terraced house',
                        'None' : 'Not stated',
                        }) 

ber_test = ber_sa[ber_sa['CountyName2'].str.contains("DUBLIN 14")]
ber_test2 = ber_test[ber_sa['Dwelling Group'].str.contains("Semi detatched house", na=False)]
ber_test2['Year of construction range'].value_counts()
ber_test2['Year of construction'].median()