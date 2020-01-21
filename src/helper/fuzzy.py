from sklearn.feature_extraction.text import TfidfVectorizer
import sparse_dot_topn.sparse_dot_topn as ct
from scipy.sparse import csr_matrix
import numpy as np
from ftfy import fix_text  # amazing text cleaning for decode issues..
import re
from typing import List, Union, Tuple, Set
from pathlib import Path
from time import time

from sklearn.neighbors import NearestNeighbors
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

import geopandas as gpd
import pandas as pd
from fuzzywuzzy import fuzz, process

from pipeop import pipes
from tqdm import tqdm
import src
from src.helper.logging import create_logger, log_dataframe

from tqdm import tqdm
import os
from matplotlib import style
style.use('fivethirtyeight')

if __name__ == '__main__':
    ROOT_DIR = Path(src.__path__[0]).parent
    LOGGER = create_logger(root_dir=ROOT_DIR, caller=__name__)

'''
Fuzzy merging options:
- Fuzzywuzzy: works well but tough to adapt for a merge
- difflib's get_close_matches
- Fuzzy_pandas
- d6tjoin (i.e. jellyfish)
- fuzzymatcher
- Dedupe


see https://stackoverflow.com/questions/13636848/is-it-possible-to-do-fuzzy-match-merge-with-python-pandas
'''

'''
Sources:
- https://towardsdatascience.com/fuzzy-matching-at-scale-84f2bfd0c536
- https://colab.research.google.com/drive/1qhBwDRitrgapNhyaHGxCW8uKK5SWJblW#scrollTo=G69wMxaaDi-x
'''


def fuzzywuzzy_match_columns_in_dataframe(
    df_in: Union[pd.DataFrame, gpd.GeoDataFrame],
    column_left: str,
    column_right: str,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:

    df_out = gpd.GeoDataFrame()

    column_left_list = df_in[column_left].unique().tolist()
    column_right_matches: List[str] = []

    for value_to_match in column_left_list:

        potential_matches = df_in[
            df_in[column_left] == value_to_match
        ][column_right].tolist()

        matching_values = process.extract(
            value_to_match,
            potential_matches,
            limit=1,
            scorer=fuzz.token_set_ratio,
        )

        column_right_matches.append(*matching_values)

    df_out[column_left] = column_left

    column_right_names, scores = zip(*column_right_matches)
    df_out[column_right] = column_right_names
    df_out['Score'] = scores

    log_dataframe(
        df=df_out,
        logger=logger,
        name=f'{column_left} and {column_right} matched using fuzzywuzzy'
    )
    return df_out


def fuzzywuzzy_match_lists(
    left: List[str],
    right: List[str],
    name_of_list: str,
) -> pd.DataFrame:

    matches: List[str] = []
    df_of_matches = pd.DataFrame()

    for value_to_match in tqdm(left):

        matching_values = process.extract(
            value_to_match,
            right,
            limit=1,
            scorer=fuzz.token_set_ratio,
        )

        matches.append(*matching_values)

    df_of_matches[name_of_list] = left
    matching_names, scores = zip(*matches)
    df_of_matches['Matches'] = matching_names
    df_of_matches['Score'] = scores

    return df_of_matches


def fuzzywuzzy_ljoin_columns(
    gdf: gpd.GeoDataFrame,
    col1: str,
    col2: str,
) -> gpd.GeoDataFrame:

    fuzzy_matches = fuzzywuzzy_match_lists(
        gdf[col1].unique().tolist(),
        gdf[col2].tolist(),
        name_of_list=col1,
    )

    return fuzzy_matches.merge(
        gdf,
        how='left',
        on=col1,
    )

    return fuzzy_matches


def fuzzywuzzy_ljoin_dfs(
    left: Union[pd.DataFrame, gpd.GeoDataFrame],
    right: Union[pd.DataFrame, gpd.GeoDataFrame],
    left_on: str,
    right_on: str,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:

    fuzzy_matches = fuzzywuzzy_match_lists(
        left[left_on].tolist(),
        right[right_on].tolist(),
        name_of_list=left_on,
    )
    left['Matches'] = fuzzy_matches['Matches']

    return pd.merge(
        left=left,
        right=right,
        left_on='Matches',
        right_on=right_on,
    )


@pipes
def _ngrams(string, n=3):
    string = (
        fix_text(string)  # fix text encoding issues

        # remove non ascii chars
        >> string.encode("ascii", errors="ignore").decode()
        >> string.lower()  # make lower case
    )

    chars_to_remove = [")", "(", ".", "|", "[", "]", "{", "}", "'"]
    rx = '[' + re.escape(''.join(chars_to_remove)) + ']'

    string = (
        re.sub(rx, '', string)  # remove the list of chars defined above
        >> string.replace('&', 'and')
        >> string.replace(',', ' ')
        >> string.replace('-', ' ')
        >> string.title()  # normalise case - capital at start of each word

        # get rid of multiple spaces and replace with a single space
        >> re.sub(' +', ' ', string).strip()
        >> ' ' + string + ' '  # pad names for ngrams...
        >> re.sub(r'[,-./]|\sBD', r'', string)
    )
    ngrams = zip(*[string[i:] for i in range(n)])
    ngrams_out = [''.join(ngram) for ngram in ngrams]

    # LOGGER.debug(f'Ngrams_out, head: {ngrams_out[:5]}')

    return ngrams_out


def awesome_cossim_top(
    A: np.array,
    B: np.array,
    ntop: int,
    lower_bound: float = 0
) -> csr_matrix:

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


def _tfidf_get_column_duplicates(
    df: pd.DataFrame,
    column: str,
) -> csr_matrix:

    unique_values = df[column].unique()
    vectorizer = TfidfVectorizer(min_df=1, analyzer=_ngrams)
    tf_idf_matrix = vectorizer.fit_transform(unique_values)

    return awesome_cossim_top(
        tf_idf_matrix,
        tf_idf_matrix.transpose(),
        10,
        0.85,
    )


def _get_matches_df(
    sparse_matrix: csr_matrix,
    name_vector: List[str],
    top=100,
) -> pd.DataFrame:
    non_zeros = sparse_matrix.nonzero()

    sparserows = non_zeros[0]
    sparsecols = non_zeros[1]

    if top:
        nr_matches = top
    else:
        nr_matches = sparsecols.size

    left_side = np.empty([nr_matches], dtype=object)
    right_side = np.empty([nr_matches], dtype=object)
    similarity = np.zeros(nr_matches)

    for index in range(0, nr_matches):
        left_side[index] = name_vector[sparserows[index]]
        right_side[index] = name_vector[sparsecols[index]]
        similarity[index] = sparse_matrix.data[index]

    return pd.DataFrame({
        'left_side': left_side,
        'right_side': right_side,
        'similarity': similarity,
    })


def tfidf_get_df_of_column_duplicates(
    df: pd.DataFrame,
    column: str,
) -> pd.DataFrame:

    matches = _tfidf_get_column_duplicates(df, column)
    unique_values = df[column].unique()
    matches_df = _get_matches_df(matches, unique_values, top=1000)
    matches_df = matches_df[
        matches_df['similarity'] < 0.99999
    ]  # Remove all exact matches


def _getNearestN(
    left_values: List[str],
    right_values: Set[str],
) -> Tuple[List[float], List[float]]:

    # LOGGER.debug('Vectorizing the data ...')
    vectorizer = TfidfVectorizer(min_df=1, analyzer=_ngrams, lowercase=False)
    tfidf = vectorizer.fit_transform(left_values)
    # LOGGER.debug('Vectorizing completed...')

    nbrs = NearestNeighbors(n_neighbors=1, n_jobs=-1).fit(tfidf)
    queryTFIDF = vectorizer.transform(right_values)
    distances, indices = nbrs.kneighbors(queryTFIDF)
    return distances, indices


def tfidf_link_records(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_on: str,
    right_on: str,
) -> pd.DataFrame:
    """ Fits a term frequency-inverse document frequency (tf-idf)
        vectorizer to left[left_on] and uses this to find the nearest match
        in the right[right_on] column

        Arguments:
            left {pd.DataFrame} 
            right {pd.DataFrame}
            left_on {str} 
            right_on {str}

        Returns:
            pd.DataFrame 
        """
    left_values = left[left_on].unique()

    # use set for increased performance
    right_values = set(right[right_on].values)

    t1 = time()
    # LOGGER.debug('Getting nearest neighbours...')
    distances, indices = _getNearestN(left_values, right_values)
    t = time()-t1
    # LOGGER.debug("Completed in:", t)

    right_values = list(right_values)  # need to convert back to a list

    # LOGGER.debug('Finding matches...')
    matches = []
    for i, j in enumerate(indices):
        temp = [
            round(distances[i][0], 2),
            left_values.values[j][0][0],
            right_values[i]
        ]
        matches.append(temp)

    # LOGGER.debug('Building data frame...')
    return pd.DataFrame(
        matches,
        columns=[
            'Match confidence (lower is better)',
            'Matched name',
            'Original name',
        ]
    )
