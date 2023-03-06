from src.datafactory import DataFactory

import pytest

import numpy as np
import pandas as pd
import random
import string
import datetime


@pytest.fixture()
def input_DataFrame():
    k = 100

    # numeric
    df1 = pd.DataFrame(np.random.randint(0, 100, size=(k, 2)), columns=["int", "float"])
    df1["float"].replace(np.random.randint(0, 100, size=int(k / 10)), np.nan, inplace=True)

    # category
    df2 = pd.DataFrame(
        random.choices([f"CAT_{k}" for k in range(5)], k=k),
        columns=["category"],
        dtype="category",
    )

    # datetime
    df3 = pd.DataFrame(
        [datetime.date.today() + datetime.timedelta(random.randint(1, 365)) for k in range(k)],
        columns=["datetime"],
        dtype="datetime64[ns]",
    )

    # object
    df4 = pd.DataFrame(
        random.choices(["".join(random.sample(string.ascii_letters, 5)) for k in range(k)], k=k),
        columns=["object"],
    )

    df = pd.concat([df1, df2, df3, df4], axis=1)
    return df


def test_DataFactory(input_DataFrame):
    _ = DataFactory(input_DataFrame)


def test_DataFactory_set_types(input_DataFrame):
    df = DataFactory(input_DataFrame)
    df.set_types(["int"], "float")


def test_DataFactory_clean_data(input_DataFrame):
    df = DataFactory(input_DataFrame)
    df.clean_data(
        headers=True,
        empty=True,
        datetime=True,
        tolo=True,
        toup=True,
        spaces=True,
        chars=True,
        dropdup=True,
        dropna=True,
        fillna=True,
    )


def test_DataFactory_drop_columns(input_DataFrame):
    df = DataFactory(input_DataFrame)
    df.drop_columns(
        columns=[
            "int",
            "float",
        ],
    )


def test_DataFactory_drop_rows(input_DataFrame):
    df = DataFactory(input_DataFrame)
    df.drop_rows(rows=[0, 1])


def test_DataFactory_merge_duplicates(input_DataFrame):
    df = DataFactory(input_DataFrame)
    df.merge_duplicates("int", ["float"], "sum")


def test_DataFactory_query_apply(input_DataFrame):
    df = DataFactory(input_DataFrame)
    df.query_apply("int > 50", inplace=True)
