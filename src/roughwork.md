# Fuzzywuzzy & idftf for Census & EDs

Was able to clean Census EDs using a regex so could do a direct pd merge

```python
def _fuzzywuzzy_ljoin_census_and_ed(
    census: pd.DataFrame,
    map_of_dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    census_with_new_ED_column = fjoin.fuzzywuzzy_ljoin_two_dataframes(
        df_left=census,
        df_right=map_of_dublin_eds,
        left_on='Electoral_Districts',
        right_on='Electoral_Districts',
    )

    log_dataframe(
        census_with_new_ED_column,
        logger=LOGGER,
        name='ED_ENGLISH column from gdf fuzzy matched to census',
    )
    return census_with_new_ED_column


def _merge_fjoined_census_and_ed(
    fjoined_census: pd.DataFrame,
    map_of_dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    census_eds_merged = pd.merge(
        left=fjoined_census,
        right=map_of_dublin_eds,
        left_on='ED_ENGLISH',
        right_on='ED_ENGLISH',
    )

    log_dataframe(
        census_eds_merged,
        logger=LOGGER,
        name='Census & EDs merged',
    )
    return census_eds_merged


def _get_tfidf_matches(
    census: pd.DataFrame,
    map_of_dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    return (
        fuzzy.tfidf_link_records(
            left=census.reset_index(),
            right=map_of_dublin_eds,
            left_on='Electoral_Districts',
            right_on='Electoral_Districts',
        )
    )

# def _reformat_census_data(census: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

#     census = census.set_index(().unstack().reset_index()

#    log_dataframe(
#         census,
#         logger=LOGGER,
#         name='Census transformed for fuzzy merging',
#     )

```

# Recordlinkage for MnR

```python
@pipes
def _recordlinkage_clean_mnr(
    mnr: pd.DataFrame
) -> pd.DataFrame:

    mnr['Address'] = (
        mnr['Address'].astype(str)
        >> recordlinkage.clean
    )

    return mnr


def _recordlinkage_deduplicate_mnr(
    mnr: pd.DataFrame,
) -> pd.DataFrame:

    # Indexation
    indexer = recordlinkage.Index()
    indexer.block('Location')
    candidate_links = indexer.index(mnr)

    # Comparison
    compare_cl = recordlinkage.Compare()
    compare_cl.string('Location', 'Location')
    features = compare_cl.compute(candidate_links, mnr)

    return features

```

# Geocoding M&R

```python

def _reformat_columns(mnr: pd.DataFrame) -> pd.DataFrame:

    mnr["Point"] = mnr["Location"].apply(
        lambda loc: tuple(loc.point) if loc else None,
    )
    mnr["Latitude"] = mnr["Point"].apply(
        lambda point: point[0] if point else None,
    )
    mnr["Longitude"] = mnr["Point"].apply(
        lambda point: point[1] if point else None,
    )

    log_dataframe(
        df=mnr_raw,
        logger=LOGGER,
        name='Reformat columns into Latitude and Longitude',
    )

    return mnr

```

# Tf-IDF fuzzy join adapted for Mnr & VO

```python

def ngrams(string, n=3):
    string = fix_text(string)  # fix text
    # remove non ascii chars
    string = string.encode("ascii", errors="ignore").decode()
    string = string.lower()
    chars_to_remove = [")", "(", ".", "|", "[", "]", "{", "}", "'"]
    rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
    string = re.sub(rx, '', string)
    string = string.replace('&', 'and')
    string = string.replace(',', ' ')
    string = string.replace('-', ' ')
    string = string.title()  # normalise case - capital at start of each word
    # get rid of multiple spaces and replace with a single
    string = re.sub(' +', ' ', string).strip()
    string = ' ' + string + ' '  # pad names for ngrams...
    string = re.sub(r'[,-./]|\sBD', r'', string)
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]


def awesome_cossim_top(A, B, ntop, lower_bound=0):
    # force A and B as a CSR matrix.
    # If they have already been CSR, there is no overhead
    A = A.tocsr()
    B = B.tocsr()
    M, _ = A.shape
    _, N = B.shape

    idx_dtype = np.int32

    nnz_max = M*ntop

    indptr = np.zeros(M+1, dtype=idx_dtype)
    indices = np.zeros(nnz_max, dtype=idx_dtype)
    data = np.zeros(nnz_max, dtype=A.dtype)

    ct.sparse_dot_topn(
        M, N, np.asarray(A.indptr, dtype=idx_dtype),
        np.asarray(A.indices, dtype=idx_dtype),
        A.data,
        np.asarray(B.indptr, dtype=idx_dtype),
        np.asarray(B.indices, dtype=idx_dtype),
        B.data,
        ntop,
        lower_bound,
        indptr, indices, data
    )

    return csr_matrix((data, indices, indptr), shape=(M, N))

# matching query:


def getNearestN(query, vectorizer, nbrs):
    queryTFIDF_ = vectorizer.transform(query)
    distances, indices = nbrs.kneighbors(queryTFIDF_)
    return distances, indices


def fast_fuzzymatch(vo: gpd.GeoDataFrame, mnr: gpd.GeoDataFrame) -> csr_matrix:

    from sklearn.metrics.pairwise import cosine_similarity
    from sklearn.feature_extraction.text import TfidfVectorizer
    import re
    from sklearn.neighbors import NearestNeighbors

    vo_addresses = vo['Address'].unique()

    print('Vecorizing the data - this could take a few minutes for large datasets...')
    vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams, lowercase=False)
    tfidf = vectorizer.fit_transform(vo_addresses)
    print('Vectorizing completed...')
    LOGGER.debug(f'tfidf: \n\n{tfidf})')

    nbrs = NearestNeighbors(n_neighbors=1, n_jobs=-1).fit(tfidf)
    LOGGER.debug(f'Nearest Neightbours: \n\n{nbrs}')

    # column to match against in the messy data
    mnr_addresses = mnr['Address'].values
    LOGGER.debug(f'mnr_addresses:  \n\n{mnr_addresses[:10]})')

    t1 = time.time()
    print('getting nearest n...')
    distances, indices = getNearestN(mnr_addresses, vectorizer, nbrs)
    LOGGER.debug(f'Dictances: \n\n{distances})')
    LOGGER.debug(f'Indices: \n\n{indices})')
    t = time.time()-t1
    print("COMPLETED IN:", t)

    # need to convert back to a list
    unique_mnr_addresses = list(mnr_addresses)
    print('Finding matches...')
    matches = []

    for i, j in enumerate(indices):
        temp = [
            round(distances[i][0], 2),
            vo_addresses[i],
            unique_mnr_addresses[i],
        ]
        matches.append(temp)

    LOGGER.debug(f'Vo Addresses: \n\n{vo_addresses[:10]}')
    LOGGER.debug(f'MnR Addresses: \n\n{unique_mnr_addresses[:10]}')
    LOGGER.debug(f'Matches: \n\n{matches}')

    print('Building data frame...')
    matches = pd.DataFrame(
        matches,
        columns=[
            'Match confidence (lower is better)',
            'Matched name',
            'Origional name'
        ]
    )
    print('Done')
    return matches

```

# Fuzzy joins for MnR & VO

```python

@log_durations(LOGGER.info)
def _fuzzywuzzy_match_addresses(
    gdf_in: gpd.GeoDataFrame,
    mnr_address_column: str = 'Address',
    vo_address_column: str = 'Address',
) -> gpd.GeoDataFrame:

    gdf_out = gpd.GeoDataFrame()

    mnr_addresses = gdf_in[mnr_address_column].unique().tolist()
    matching_vo_addresses: List = []

    for mnr_address in mnr_addresses:

        nearby_vo_addresses = gdf_in[
            gdf_in[mnr_address_column] == mnr_address
        ][vo_address_column].tolist()

        matching_vo_address = process.extract(
            mnr_address,
            nearby_vo_addresses,
            limit=1,
            scorer=fuzz.token_set_ratio,
        )

        matching_vo_addresses.append(*matching_vo_address)

    gdf_out['MnR Address'] = mnr_addresses
    vo_addresses, scores = zip(*matching_vo_addresses)
    gdf_out['Closest Address'] = vo_addresses
    gdf_out['Score'] = scores

    log_dataframe(gdf_out, LOGGER, name='M&R and VO matched using fuzzywuzzy')
    return gdf_out


def _sjoin_vo_and_mnr_and_fuzzymatch(
    vo: gpd.GeoDataFrame,
    mnr: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    mnr_buffered = _add_buffer_to_mnr(mnr)

    return pipe(
        _spatial_join_nearby_buildings(vo, mnr_buffered),
        _fuzzywuzzy_match_addresses,
    )


def fuzzywuzzy_ljoin_two_dataframes(
    df_left: Union[pd.DataFrame, gpd.GeoDataFrame],
    df_right: Union[pd.DataFrame, gpd.GeoDataFrame],
    left_on: str,
    right_on: str,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:

    df_out = pd.DataFrame()
    right_on_matches: List[str] = []

    for value_to_match in tqdm(df_left[left_on]):

        potential_matches = df_right[right_on].tolist()

        matching_values = process.extract(
            value_to_match,
            potential_matches,
            limit=1,
            scorer=fuzz.token_set_ratio,
        )

        right_on_matches.append(*matching_values)

    df_out[left_on + 'MnR'] = df_left[left_on]
    right_on_list, scores = zip(*right_on_matches)
    df_out[right_on + ' VO'] = right_on_list
    df_out['Similarity'] = scores

    log_dataframe(
        df=df_out,
        logger=LOGGER,
        name=f'{left_on} in MnR and {right_on} in VO matched using fuzzywuzzy',
    )
    return df_out

```


# Fuzzywuzzy join Census & EDs

```python

def fuzzywuzzy_A_add_matching_column_to_dataframe(
    census: pd.DataFrame,
    map_of_dublin_eds: gpd.GeoDataFrame,
    key1: str,
    key2: str,
    threshold: int = 90,
    limit: int = 1
) -> gpd.GeoDataFrame:
    """ Uses fuzzywuzzy to join to dfs on columns

        Arguments:
            census {pd.DataFrame} -- is the left table to join
            map_of_dublin_eds {gpd.GeoDataFrame} -- is the right table to join
            key1 {str} -- is the key column of the left table
            key2 {str} -- is the key column of the right table

        Keyword Arguments:
            threshold {int} -- is how close the matches should be to return a match,
            based on Levenshtein distance (default: {90})
            limit {int} -- is the amount of matches that will get returned,
            these are sorted high to low (default: {1})

        Returns:
            gpd.GeoDataFrame -- is a fuzzyjoined gdf

        """
    map_eds = map_of_dublin_eds[key2].tolist()

    census['Map_EDs'], census['Similarity'] = census[key1].apply(
        lambda row: process.extract(
            row, map_eds, limit=limit, scorer=fuzz.token_set_ratio,
        )
    )

    return census


def fuzzywuzzy_match(s, t, d, i):
    new_name, score = process.extractOne(s, t)
    d.loc[i] = [s, new_name, score]
    return 1
    return 0


```

# Fuzzy joins for Census & EDs

```python

def _fuzzywuzzy_join_census_and_ed(
    census: pd.DataFrame,
    map_of_dublin_eds: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    return fjoin.fuzzywuzzy_add_matching_column_to_dataframe(
        census=census,
        map_of_dublin_eds=map_of_dublin_eds,
        key1='Electoral_Districts',
        key2='ED_ENGLISH',
    )


def _fuzzypandas_join_census_and_eds(
    census: pd.DataFrame,
    map_of_dublin_eds: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    fuzzyjoined_df = fuzzy_pandas.fuzzy_merge(
        df1=census.reset_index(),
        df2=map_of_dublin_eds,
        left_on='Electoral_Districts',
        right_on='ED_ENGLISH',
        method='jaro',
    )

    log_dataframe(
        df=map_of_dublin_eds,
        logger=LOGGER,
        name='Join Census & EDs using fuzzy_pandas',
    )
    return fuzzyjoined_df

def _fuzzymatch_ed_names(
    census: pd.DataFrame,
    map_of_dublin: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    return fuzzywuzzy_match_columns_in_two_dataframes(
        df_left=census.reset_index(),
        df_right=map_of_dublin,
        left_on='Electoral_Districts',
        right_on='ED_ENGLISH',
    )


def _fuzzywuzzy_join_census_and_eds(census: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    map_of_dublin_eds = _load_map_of_dublin_eds()
    fuzzymatched_eds = _fuzzymatch_ed_names(census, map_of_dublin_eds)

    fuzzy_dublin = pd.merge(
        fuzzymatched_eds,
        map_of_dublin_eds,
        left_on='ED_ENGLISH',
        right_on='ED_ENGLISH',
    )
    return pd.merge(
        census,
        fuzzy_dublin,
        left_on='Electoral_Districts',
        right_on='Electoral_Districts',
    )

```

# VO Cleaning for Rowan Commercial Amalgamation.xlsx

```python 

INPUT_NAME = 'Rowan Commercial Amalgamation.xlsx'

def _load_vo() -> pd.DataFrame:

    vo_path = ROOT_DIR / 'data' / 'interim' / f'{INPUT_NAME}'
    vo = pd.read_excel(vo_path, engine='openpyxl')

    logfuncs.log_dataframe(df=vo, logger=LOGGER)
    return vo

def _merge_columns_into_address(vo_raw: pd.DataFrame) -> pd.DataFrame:

    vo_raw['Address'] = (
        vo_raw[[
            'Address 1',
            'Address 2',
            'Address 3',
            'Uses',
            'Floor Use',
        ]].fillna('').astype(str).apply(' '.join, axis=1)
    )

    logfuncs.log_dataframe(
        df=vo_raw,
        logger=LOGGER,
        name='Merge columns into Address',
    )
    return vo_raw

# Nearest building
https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe#222388

```python

from shapely.ops import nearest_points
from shapely.geometry import Point

def join_nearest_buildings(
    gdf1: gpd.GeoDataFrame,
    gdf2: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    # unary union of the gpd2 geomtries
    vo_pts = gdf2.geometry.unary_union

    def _near(point, pts=vo_pts):
        # find the nearest point and return the corresponding Place value
        nearest = gdf2.geometry == nearest_points(point, pts)[1]
        return gdf2[nearest].Place.get_values()[0]

    gdf_out = gpd.GeoDataFrame()
    gdf_out["MnR Address"] = gdf1["Address"]
    gdf_out['Nearest VO Address'] = gdf1.apply(
        lambda row: _near(row.geometry),
        axis=0,
    )
    return gdf_out

def ckdnearest(
    gdA: gpd.GeoDataFrame,
    gdB: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)))
    nB = np.array(list(zip(gdB.geometry.x, gdB.geometry.y)))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdf = pd.concat(
        [gdA.reset_index(drop=True),
         gdB.loc[idx, gdB.columns != 'geometry'].reset_index(drop=True),
         pd.Series(dist, name='dist')
         ], axis=1)

    return gdf

```

# Joining M&R and VO

```python

import fuzzymatcher
import pandas_dedupe

@log_durations(LOGGER.info)
def fuzzywuzzy_merge(df_1, df_2)
:
    s = df_2['Address'].tolist()

    match, score = df_1['Address'].apply(lambda x: process.extract(
        x, s, limit=1, scorer=fuzz.token_set_ratio))
    df_1['Closest Address'], df_1['Similarity'] = match, score

    return df_1


def fuzzymatcher_merge(mnr: gpd.GeoDataFrame, vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return fuzzymatcher.fuzzy_left_join(mnr, vo, left_on='Address', right_on='Address')

@log_durations(LOGGER.info)
def fuzzy_match_on_address(vo: gpd.GeoDataFrame, mnr: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    closest_vo_address = []
    similarity = []

    for address in mnr['Address']:
        # ratio = process.extract(address, vo['Address'], limit=1)
        ratio = process.extractOne(
            address, vo['Address'], scorer=fuzz.token_set_ratio)
        closest_vo_address.append(ratio[0][0])
        similarity.append(ratio[0][1])

    mnr_matched = mnr.copy()

    mnr_matched['Closest VO Address'] = pd.Series(closest_vo_address)
    mnr_matched['Similarity'] = pd.Series(similarity)

    return mnr_matched

def _create_geodf_from_crs(df: pd.DataFrame, x: pd.Series, y: pd.Series, crs: str) -> gpd.GeoDataFrame:

    locations = gpd.points_from_xy(x, y)
    geo_df = gpd.GeoDataFrame(df, geometry=locations)
    geo_df.crs = crs
    geo_df = geo_df.to_crs('epsg:4326')

    log_dataframe(geo_df, LOGGER)
    return geo_df


def convert_to_geodf(mnr: pd.DataFrame, vo: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):

    mnr_geo = _create_geodf_from_crs(
        mnr, y=mnr['Latitude'], x=mnr['Longitude'], crs='epsg:4326')

    vo_geo = _create_geodf_from_crs(
        vo, x=vo[' X ITM'], y=vo[' Y ITM'], crs='epsg:2157')

    return mnr_geo, vo_geo


def _drop_null_coordinates(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return gdf.dropna(axis=1, subset=[" X ITM", " Y ITM"])


def _convert_coordinates_to_floats(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    gdf[[" X ITM", " Y ITM"]] = gdf[[" X ITM", " Y ITM"]].astype(np.float32)

    return


def _remove_non_dublin_data_from_vo(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return vo[vo[' X ITM'] > 100000]


def _drop_duplicated_addresses(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return gdf.drop_duplicates(subset=["Address"],)


def clean_vo(vo: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    vo_clean = pipe(
        _drop_null_coordinates(vo),
        _convert_coordinates_to_floats,
        _remove_non_dublin_data_from_vo,
        _drop_duplicated_addresses)

    return vo_clean


def _remove_unnamed_columns_from_mnr(mnr: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return mnr.drop(columns=["Unnamed: 0", "Unnamed: 0.1"])


def _deduplicate_geodataframe(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    return pandas_dedupe.dedupe_dataframe(df=gdf, field_properties=['Address'])


def clean_mnr(mnr: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    mnr_clean = pipe(
        _remove_unnamed_columns_from_mnr(mnr),
        _deduplicate_geodataframe)

    return mnr
```