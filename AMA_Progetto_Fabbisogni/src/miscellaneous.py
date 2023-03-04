from pathlib import Path
from typing import Union

import time
import datetime
import functools

from icecream import ic
import logging

import re

import numpy as np
import pandas as pd

# from codicefiscale import codicefiscale
from dataprep.clean import clean_headers, clean_date, clean_it_codicefiscale, clean_it_iva

REGEX_D = "([0-2][1-9]|3[01])"
REGEX_M = "(0[1-9]|1[0-2])"
REGEX_Y4 = "(19[0-9][0-9]|20[0-2][0-9])"
REGEX_Y2 = "([01][0-9]|2[0-3])"
REGEX_DATES = {
    f"^{REGEX_Y4}-{REGEX_M}-{REGEX_D}$": "%Y-%m-%d",
    f"^{REGEX_D}-{REGEX_M}-{REGEX_Y4}$": "%d-%m-%Y",
    f"^{REGEX_Y4}_{REGEX_M}_{REGEX_D}$": "%Y_%m_%d",
    f"^{REGEX_D}_{REGEX_M}_{REGEX_Y4}$": "%d_%m_%Y",
    f"^{REGEX_Y4}/{REGEX_M}/{REGEX_D}$": "%Y/%m/%d",
    f"^{REGEX_D}/{REGEX_M}/{REGEX_Y4}$": "%d/%m/%Y",
    f"^{REGEX_Y2}-{REGEX_M}-{REGEX_D}$": "%y-%m-%d",
    f"^{REGEX_D}-{REGEX_M}-{REGEX_Y2}$": "%d-%m-%y",
    f"^{REGEX_Y2}_{REGEX_M}_{REGEX_D}$": "%y_%m_%d",
    f"^{REGEX_D}_{REGEX_M}_{REGEX_Y2}$": "%d_%m_%y",
    f"^{REGEX_Y2}/{REGEX_M}/{REGEX_D}$": "%y/%m/%d",
    f"^{REGEX_D}/{REGEX_M}/{REGEX_Y2}$": "%d/%m/%y",
    f"^{REGEX_Y4}{REGEX_M}{REGEX_D}$": "%Y%m%d",
    f"^{REGEX_D}{REGEX_M}{REGEX_Y4}$": "%d%m%Y",
    f"^{REGEX_Y2}{REGEX_M}{REGEX_D}$": "%y%m%d",
    f"^{REGEX_D}{REGEX_M}{REGEX_Y2}$": "%d%m%y",
}


def timefunc(func):
    """
    Time decorator.
    """

    @functools.wraps(func)
    def time_closure(*args, **kwargs):
        """
        Time wrapper.
        """
        start = time.perf_counter()
        result = func(*args, **kwargs)
        time_elapsed = time.perf_counter() - start
        logging.info(f"{func.__name__} - Time elapsed: {time_elapsed:.2f} s")
        return result

    return time_closure


def mem_usage(X: Union[pd.DataFrame, pd.Series]) -> float:
    """
    Compute memory usage.

    :param Union[pd.DataFrame, pd.Series] X: Pandas object
    :return: Usage in MB
    """
    if isinstance(X, pd.DataFrame):
        usage_b = X.memory_usage(deep=True).sum()
    elif isinstance(X, pd.Series):
        usage_b = X.memory_usage(deep=True)
    else:
        raise ValueError("Invalid type:", type(X))
    usage_mb = usage_b / 1024**2  # convert bytes to megabytes
    return usage_mb


def all_files(dirname: str, pattern: str = "*") -> list:
    """
    All files in directory.

    :param str dirname: Directory to scan
    :param str pattern: Pattern to filter
    :return: File paths
    """
    if pattern == "":
        pattern = "*"
    if pattern != "*":
        pattern = "*" + pattern + "*"
    content = sorted(Path(dirname).glob(pattern))
    files = list()
    for i in content:
        if i.is_file():
            files.append(i)
    return files


def latest_file(dirname: str, pattern: str = "*") -> str:
    """
    Latest file in directory.

    :param str dirname: Directory to scan
    :param str pattern: Pattern to filter
    :return: File path
    """
    files = all_files(dirname, pattern)
    return max(files, key=lambda x: x.stat().st_ctime)


def convert_data(data, to: str):
    """
    Convert data.

    :param obj data: Object to transform
    :param str to: Transform to
    :return: Converted data
    """
    converted = None
    if to == "array":
        if isinstance(data, np.ndarray):
            converted = data
        elif isinstance(data, list):
            converted = np.array(data)
        elif isinstance(data, pd.Series):
            # converted = data.values
            converted = data.to_numpy()
        elif isinstance(data, pd.DataFrame):
            # converted = data.as_matrix()
            converted = data.to_numpy()
    elif to == "list":
        if isinstance(data, list):
            converted = data
        elif isinstance(data, pd.Series):
            converted = data.values.to_list()
        elif isinstance(data, np.ndarray):
            converted = data.to_list()
    elif to == "dataframe":
        if isinstance(data, pd.DataFrame):
            converted = data
        elif isinstance(data, np.ndarray):
            converted = pd.DataFrame(data)
        elif isinstance(data, pd.Series):
            converted = data.to_frame()
    else:
        raise ValueError(f"Unknown data conversion: {to}")

    if converted is None:
        raise TypeError(f"Cannot handle data conversion of type: {type(data)} to {to}")

    return converted


def fit_transform(obj, X_train, y_train=None, X_test=None, y_test=None):
    """
    Fit & transform function.

    :param obj: Object
    :return: File path
    """
    X_train_transformed, X_test_transformed = None, None

    if obj is not None:
        if hasattr(obj, "fit_transform"):
            # print("fit_transform")
            X_train_transformed = obj.fit_transform(X_train, y_train)
        elif hasattr(obj, "fit") and hasattr(obj, "transform"):
            # print("fit")
            obj.fit(X_train, y_train)
            # print("transform")
            X_train_transformed = obj.transform(X_train)
        else:
            raise ValueError("Invalid obj (no fit & transform methods)")
        if X_test is not None and hasattr(obj, "transform"):
            # print("transform")
            X_test_transformed = obj.transform(X_test)

    return X_train_transformed, X_test_transformed


def parse_date(date: str):
    """
    Parse dates.

    :param str date: Date to parse
    :return: datetime object
    """
    for r, fmt in REGEX_DATES.items():
        match = re.search(r, date)
        print(fmt, match)
        if match:
            print(fmt, match[0], datetime.datetime.strptime(match[0], fmt))
            try:
                dt = datetime.datetime.strptime(match[0], fmt)
                return dt
            except ValueError:
                return None
    return None


def clean_columns(df: pd.DataFrame, case: str = "const") -> pd.DataFrame:
    """
    Clean column headers.

    .. seealso:: https://docs.dataprep.ai/user_guide/clean/clean_headers.html

    :param pd.DataFrame df: DataFrame
    :param str case: Desired case style of the column name (’snake’: ‘column_name’, ’kebab’: ‘column-name’, ’camel’: ‘columnName’, ’pascal’: ‘ColumnName’, ’const’: ‘COLUMN_NAME’, ’sentence’: ‘Column name’, ’title’: ‘Column Name’, ’lower’: ‘column name’, ’upper’: ‘COLUMN NAME’)
    :return: DataFrame
    """
    logging.info("Clean Headers")

    return clean_headers(df, case=case)


def clean_dates(df: pd.DataFrame, column: Union[str, list]) -> pd.DataFrame:
    """
    Clean dates.

    .. seealso:: https://docs.dataprep.ai/user_guide/clean/clean_date.html

    :param pd.DataFrame df: DataFrame
    :param Union[str, list] column: Column name
    :return: DataFrame
    """
    logging.info("Clean Dates")

    if not isinstance(column, list):
        column = [column]

    for c in column:
        df = clean_date(df, column=c, output_format="YYYY-MM-DD", inplace=True)
        df.rename(columns={f"{c}_clean": f"{c}"}, inplace=True)

    return df


# def decode_codice_fiscale(x: str) -> list:
#     """
#     Decode codice fiscale.
#
#     .. seealso:: https://github.com/fabiocaccamo/python-codicefiscale
#
#     :param str x: Codice fiscale
#     :return: [flag, sex, birthdate, birthplace]
#     """
#     try:
#         cf = codicefiscale.decode(x)
#         info = list()
#         if cf is not None:
#             info.append(1)
#             info.append(cf.get("sex", "-"))
#             info.append(cf.get("birthdate", "-"))
#             if cf.get("birthplace", "-") is not None:
#                 info.append(cf.get("birthplace", "-").get("name", "-"))
#             else:
#                 info.append("-")
#         else:
#             info = [0]
#             info.extend([""] * 3)
#     except Exception:
#         info = [0]
#         info.extend([""] * 3)
#     return info


# def process_codice_fiscale(df: pd.DataFrame, column: str) -> pd.DataFrame:
#     """
#     Process codice fiscale.
#
#     :param pd.DataFrame df: DataFrame
#     :param str column: Column name
#     :return: DataFrame
#     """
#     logging.info("")
#     logging.info("Process Codice Fiscale")
#
#     df_ = df[column].apply(decode_codice_fiscale).to_frame()
#     df_split = pd.DataFrame(df_[column].to_list(), columns=[column + "_ISVALID", "SEX", "BIRTHDATE", "BIRTHPLACE"])
#     df_split[column + "_ISVALID"] = df_split[column + "_ISVALID"].astype("category")
#     df_split["SEX"] = df_split["SEX"].astype("category")
#     df_split["BIRTHDATE"] = df_split["BIRTHDATE"].astype("datetime64[ns]")
#     df_split["BIRTHPLACE"] = df_split["BIRTHPLACE"].astype("category")
#     del df_
#
#     df = pd.concat([df, df_split], axis=1)
#     del df_split
#
#     return df


def clean_codice_fiscale(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Clean codice fiscale.

    .. seealso:: https://docs.dataprep.ai/api_reference/dataprep.clean.html#module-dataprep.clean.clean_it_codicefiscale
    .. seealso:: https://arthurdejong.org/python-stdnum/doc/1.17/stdnum.it.codicefiscale

    :param pd.DataFrame df: DataFrame
    :param str column: Column name
    :return: DataFrame
    """
    logging.info("")
    logging.info("Clean Codice Fiscale")

    df = clean_it_codicefiscale(df, column=column, output_format="standard")
    df.rename(columns={f"{column}_clean": f"{column}_CLEANED"}, inplace=True)
    df[f"{column}_CLEANED"] = df[f"{column}_CLEANED"].astype("object")
    # df = clean_it_codicefiscale(df, column=column, output_format="gender")
    # df.rename(columns={f"{column}_clean": f"{column}_SEX"}, inplace=True)
    # df[f"{column}_SEX"] = df[f"{column}_SEX"].astype("category")
    # df = clean_it_codicefiscale(df, column=column, output_format="birthdate")
    # df.rename(columns={f"{column}_clean": f"{column}_BIRTHDATE"}, inplace=True)
    # df[f"{column}_BIRTHDATE"] = df[f"{column}_BIRTHDATE"].astype("datetime64[ns]")

    return df


def clean_partita_iva(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Clean partita iva.

    .. seealso:: https://docs.dataprep.ai/api_reference/dataprep.clean.html#module-dataprep.clean.clean_it_iva
    .. seealso:: https://arthurdejong.org/python-stdnum/doc/1.17/stdnum.it.iva

    :param pd.DataFrame df: DataFrame
    :param str column: Column name
    :return: DataFrame
    """
    logging.info("")
    logging.info("Clean Partita IVA")

    df = clean_it_iva(df, column=column, output_format="standard")
    df.rename(columns={f"{column}_clean": f"{column}_CLEANED"}, inplace=True)
    df[f"{column}_CLEANED"] = df[f"{column}_CLEANED"].astype("object")

    return df
