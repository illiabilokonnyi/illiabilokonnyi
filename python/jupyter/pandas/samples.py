import pandas as pd
import numpy as np


key_categories = ['', 'mp', 'mn', 'p', 'u', 'n', 'pk', 'vp', 'vn']


def compile_sk_df(values: list, sk_mark: str):
    df = pd.DataFrame(values, columns=['attribute'])
    df['Key'] = sk_mark
    return df


def normalize_key_df(df: pd.DataFrame, inplace=False):
    if not inplace:
        df = df.copy()

    df['Upper'] = df['attribute'].str.upper()
    df['Key'].replace(np.nan, '', inplace=True)
    df['sortingWeight'] = df['Key'].replace(
        {v: k for k, v in enumerate(key_categories)})

    if not inplace:
        return df


def concat_sk_dfs(df_list: list[pd.DataFrame], normalized=False):
    if not normalized:
        df_list = map(normalize_key_df, df_list)

    df = pd.concat(
        df_list,
        ignore_index=True
    )

    df['nullable'] = df.groupby(
        'Upper')['nullable'].transform('min')

    df = remove_weighted_duplicates(df)
    return df


def remove_weighted_duplicates(df: pd.DataFrame):
    df = df.sort_values(
        by=['Upper', 'sortingWeight'],
        ascending=True
    ).drop_duplicates(
        keep='last',
        subset=['Upper']
    ).reset_index(
        drop=True
    )
    return df


def save_normalized_sk_df(df: pd.DataFrame, path: str):
    df_out = df.drop(columns=['Upper', 'sortingWeight'])
    df_out['nullable'].replace({0: False, 1: True}, inplace=True)
    df_out.to_excel(path, index=False)
