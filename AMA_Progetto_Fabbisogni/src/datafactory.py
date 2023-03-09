from src.miscellaneous import all_files, mem_usage, convert_data, fit_transform

from IPython.display import display

from icecream import ic
import logging
from importlib import reload

from abc import ABC, abstractmethod
from typing import Union
from pathlib import Path
import os
import itertools
import random
import re

import numpy as np
import pandas as pd

# from pandarallel import pandarallel

from sqlite3 import connect

import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

import sidetable
from dataprep.clean import clean_headers

from sklearn.utils import Bunch


class DataFactory(ABC):
    """
    Helper class to handle pandas.DataFrame.

    .. seealso:: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html

    :param Union[str, pd.DataFrame, sklearn.Bunch, sklearn.datasets] df: Input DataFrame
    :param str label: Label name
    :param bool dry: Dry mode
    :param bool pickled: Load from pickle
    :param bool empty: Empty DataFrame
    :param bool pre_process: Pre-process DataFrame
    :param bool process: Process DataFrame
    :param bool post_process: Post-process DataFrame
    :param bool silent: Silent mode
    :param bool verbose: Verbose mode
    :param bool debug: Debug mode
    :param dict kwargs: Load options

    :ivar pd.DataFrame df: Dataframe
    :ivar str file: File name
    :ivar list columns: Column names
    :ivar dict types: Column types
    :ivar list label: Label name
    :ivar dict kwargs: Load options
    :ivar dict options: DataFactory options
    """

    def __init__(
        self,
        df: Union[str, pd.DataFrame, Bunch],
        label: str = None,
        keys: list = None,
        dry: bool = False,
        pickled: bool = False,
        empty: bool = False,
        pre_process: bool = True,
        process: bool = True,
        post_process: bool = True,
        silent: bool = False,
        verbose: bool = True,
        debug: bool = False,
        **kwargs,
    ):
        self.df = df
        self.df_bkp = None
        self.file = None
        self.ext = "pkl"
        self.compression = "zip"

        self.columns = None
        self.types = None
        self.dbltypes = None
        self.dtypes = None
        self.keys = keys

        self.label = None
        self.classes = None
        self.is_binary = None
        self.is_continuous = None
        self.features = None

        self.silent = silent
        self.verbose = verbose
        self.debug = debug

        self.kwargs = kwargs
        self.options = dict()

        self.nans = None
        self.dups = None

        self.X = self.y = None
        self.X_train = self.y_train = None
        self.X_valid = self.y_valid = None
        self.X_test = self.y_test = None
        self.is_split_Xy = False
        self.is_split_tt = False
        self.is_split_tvt = False

        self.data = "df"
        self.data_Xy = None
        self.data_tt = None

        self.name = type(self).__name__
        self.logger = None

        self.set_options()
        self.set_kwargs()

        ic()

        self.logger.info("")
        self.logger.info(self.name)

        if empty:
            self.logger.info("")
            self.logger.info("Empty DataFactory")
            return

        if dry:
            self.set_kwargs(pop="na_values")
            self.set_kwargs(pop="dtype")
            self.set_kwargs(pop="parse_dates")
            pre_process = False
            process = False
            post_process = False

        if pickled and isinstance(self.df, str):
            ext = self.df.split(".")[-2] if self.df.split(".")[-1] in ["zip"] else self.df.split(".")[-1]
            self.df = self.df.replace("/*", f"/{self.name}") if "/*" in self.df else self.df
            self.df = self.df.replace(f".{self.df.split('.')[-1]}", f"_DataFactory.{self.ext}.{self.compression}")
            self.df = self.df.replace(f".{ext}", "") if len(ext) == 3 and ext != self.ext else self.df
            pre_process = False
            # process = False
            # post_process = False

        self.load()

        self.df.drop(columns=self.df.columns[self.df.columns.str.contains("Unnamed")])
        self.detect_columns()
        self.detect_types()
        self.set_label(label)

        if any([pre_process, process, post_process]) and not silent:
            self.print()
            self.check_data()
            self.check_keys()

        if self.name != "DataFactory":
            if pre_process:
                self.logger.info("")
                self.logger.info("Pre-Process")
                self.pre_process()
                self.update()

            if process:
                self.logger.info("")
                self.logger.info("Process")
                self.process()
                self.update()

            if post_process:
                self.logger.info("")
                self.logger.info("Post-Process")
                self.post_process()
                self.update()

        if not self.silent:
            self.print(verbose=self.verbose)
            self.check_data()
            self.check_keys()

        # if self.label is not None:
        #     self.split_Xy()

    def __repr__(self):
        """
        Repr method.
        """
        self.print()
        return self.name

    def __str__(self):
        """
        Str method.
        """
        return self.__repr__()

    def set_options(
        self,
        silent: bool = None,
        debug: bool = None,
        backend: str = "matplotlib",
        precision: int = 4,
        random_state: int = 0,
    ):
        """
        Set DataFactory options.

        .. seealso:: https://pandas.pydata.org/pandas-docs/stable/user_guide/options.html

        :param bool silent: Silent mode
        :param bool debug: Debug mode
        :param str backend: Plot backend ('matplotlib', 'plotly', 'seaborn', 'yellowbrick', 'hvplot', 'holoviews', 'pandas_bokeh')
        :param int precision: Numerical precision
        :param int random_state: Random seed
        """
        self.silent = silent if silent is not None and silent != self.silent else self.silent
        self.debug = debug if debug is not None and debug != self.debug else self.debug

        # pandarallel.initialize(progress_bar=False)

        reload(logging)
        # logging.basicConfig(format="INFO %(message)s", level=logging.INFO)
        # logging.basicConfig(format="WARN %(message)s", level=logging.WARNING)
        # logging.basicConfig(format="ERRO %(message)s", level=logging.ERROR)
        logging.basicConfig(format="%(levelname)s %(message)s")
        self.logger = logging.getLogger(self.name)
        if self.silent:
            logging.basicConfig(level=logging.WARNING)
            self.logger.setLevel(level=logging.WARNING)
        else:
            logging.basicConfig(level=logging.INFO)
            self.logger.setLevel(level=logging.INFO)

        ic.configureOutput(prefix=" IC  ", includeContext=False, contextAbsPath=False)
        if self.debug:
            ic.enable()
        else:
            ic.disable()

        # https://towardsdatascience.com/plotting-in-pandas-just-got-prettier-289d0e0fe5c0
        self.options["backend"] = backend
        if self.options["backend"] in [
            "matplotlib",
            "plotly",
            "hvplot",
            "holoviews",
            "pandas_bokeh",
        ]:
            pd.set_option("plotting.backend", self.options["backend"])
        # else:
        #     pd.set_option('plotting.backend', 'matplotlib')

        # if self.options["backend"] == "holoviews":
        #     hv.extension("bokeh")

        # https://towardsdatascience.com/style-your-pandas-dataframes-814e6a078c6d
        # https://towardsdatascience.com/style-pandas-dataframe-like-a-master-6b02bf6468b0
        # df.style.highlight_null(null_color='yellow')

        # https://pandas.pydata.org/pandas-docs/stable/user_guide/options.html
        # pd.set_option('display.max_rows', 999)

        self.options["precision"] = precision
        np.set_printoptions(precision=self.options["precision"])
        pd.set_option("display.precision", self.options["precision"])
        precision = f"{{:,.{self.options['precision']}f}}"
        pd.set_option("display.float_format", precision.format)
        # pd.set_option("display.float_format", f"{{:,.{self.options['precision']}f}}")

        self.options["alignment"] = 20

        self.options["random_state"] = random_state
        os.environ["PYTHONHASHSEED"] = str(self.options["random_state"])
        random.seed(self.options["random_state"])
        np.random.seed(self.options["random_state"])

    def set_kwargs(self, key: str = None, value=None, pop: str = None, **kwargs):
        """
        Set kwargs options.

        :param str key: Key to add
        :param value: Value to add
        :param str pop: Key to pop
        """
        if key is not None and value is not None:
            if key not in self.kwargs or self.kwargs[key] is None:
                self.kwargs[key] = value
        elif pop is not None:
            self.kwargs.pop(pop, None)
        else:
            self.kwargs.update(**kwargs)

    def print_options(self):
        """
        Print DataFactory options.
        """
        self.logger.info("")
        for key, value in self.options.items():
            self.logger.info(f"{key:<{self.options['alignment']}} = {value}")

    def print_kwargs(self):
        """
        Print kwargs.
        """
        self.logger.info("")
        for key, value in self.kwargs.items():
            self.logger.info(f"{key:<{self.options['alignment']}} = {value}")

    def set_label(
        self,
        label: str,
    ):
        """
        Set label.

        :param str label
        """
        if label is not None:
            self.label = label
        if self.label is not None:
            if isinstance(self.label, list):
                self.is_continuous = False
                for t in self.label:
                    self.types.drop(index=[t], inplace=True)
            elif self.label in self.types.index:
                self.is_continuous = True if self.types["float"][self.label] else False
                self.types.drop(index=[self.label], inplace=True)
            self.classes = sorted(list(np.unique(self.df[self.label])))
            self.is_binary = True if len(self.classes) == 2 else False
            self.detect_columns()

    def load(self):  # NOQA C901
        """
        Load DataFrame.
        """
        ic()

        if isinstance(self.df, pd.DataFrame):
            self.logger.info("")
            self.logger.info("Load from pd.DataFrame")
            self.df = self.df.copy(deep=True)

        elif isinstance(self.df, Bunch):
            self.logger.info("")
            self.logger.info("Load from sklearn.Bunch")
            self.label = "target"
            self.X = pd.DataFrame(data=self.df.data, columns=self.df.feature_names)
            self.y = pd.Series(self.df.target, name=self.label)
            self.merge_Xy()

        elif isinstance(self.df, str) and "sklearn." in self.df:
            self.set_kwargs(key="random_state", value=self.options["random_state"])

            self.logger.info("")
            self.logger.info("Generate sklearn.datasets")
            self.label = "t"
            self.df = self.df.replace("sklearn.", "")
            if self.df == "blobs":
                from sklearn.datasets import make_blobs

                X, y = make_blobs(**self.kwargs)
            elif self.df == "classification":
                from sklearn.datasets import make_classification

                X, y = make_classification(**self.kwargs)
            elif self.df == "classification_multilabel":
                from sklearn.datasets import make_multilabel_classification

                X, y = make_multilabel_classification(**self.kwargs)
            elif self.df == "regression":
                from sklearn.datasets import make_regression

                X, y = make_regression(**self.kwargs)
            else:
                raise ValueError(f"Invalid sklearn.datasets: {self.df}")

            self.X = pd.DataFrame(X)
            self.X.columns = [f"f{x}" for x in self.X.columns]
            if self.df == "classification_multilabel":
                self.y = pd.DataFrame(y)
                self.y.columns = [f"{self.label}{y}" for y in self.y.columns]
                self.label = self.y.columns.to_list()
            else:
                self.y = pd.Series(y, name=self.label)
            self.merge_Xy()

        elif isinstance(self.df, str):
            if ".db" in self.df and " | " in self.df:
                self.df, table = self.df.split(" | ")

            if self.df.split(".")[-1] in ["zip"]:
                zip = self.df.split(".")[-1]
                ext = self.df.split(".")[-2]
            else:
                zip = None
                ext = self.df.split(".")[-1]

            if "*" in self.df:
                path = Path(self.df).parents[0]
                files = all_files(path, ext if not zip else zip)
                files = [f for f in files if "DataFactory" not in str(f)]
                if not len(files):
                    raise ValueError(f"Invalid ext: {self.df}")
                self.logger.info("")
                self.logger.info(f"Load {len(files)} {ext if not zip else zip} files from {path}")
                dfs = list()
                for f in files:
                    self.df = str(f)
                    if zip:
                        self.df = self.df.replace(f".{zip}", f".{ext}.{zip}")
                    self.load()
                    dfs.append(self.df)
                self.df = pd.concat(dfs, axis=0, ignore_index=True)

            else:
                if not os.path.isfile(self.df) and zip is not None:
                    self.df = self.df.replace(f".{ext}", "")
                if not os.path.isfile(self.df):
                    raise ValueError(f"Invalid file: {self.df}")

                self.logger.info("")
                self.logger.info(f"Load from {os.path.abspath(self.df)}")
                self.logger.info(f"Size {round(os.path.getsize(self.df) / 1024**2, 2)} MB")
                self.file = Path(self.df)

                if ext == "csv":
                    self.df = pd.read_csv(
                        self.df,
                        **self.kwargs,
                    )
                elif ext == "tsv":
                    self.df = pd.read_csv(
                        self.df,
                        sep="\t",
                        **self.kwargs,
                    )
                elif ext in ["xlsx", "ods"]:
                    engine = "odf" if ext == "ods" else None
                    self.df = pd.read_excel(
                        self.df,
                        engine=engine,
                        **self.kwargs,
                    )
                elif ext == "db":
                    with connect(self.df) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        tables = [t[0] for t in cursor.fetchall()]
                        self.logger.info("")
                        self.logger.info(f"Table {table}")
                        if table in tables:
                            self.file = Path(str(self.df.replace(".db", f"-{table}.db")))
                            query = f'SELECT * FROM "{table}"'
                            if "nrows" in self.kwargs:
                                query = f"{query} LIMIT {self.kwargs['nrows']}"
                                self.set_kwargs(pop="nrows")
                            self.logger.info(f"Query {query}")
                            self.df = pd.read_sql_query(
                                query,
                                conn,
                                **self.kwargs,
                            )
                        else:
                            print("Available tables")
                            for t in sorted(tables):
                                print(f"- {t}")
                            raise ValueError(f"Invalid table: {table}")
                elif ext == "h5":
                    self.df = pd.read_hdf(
                        self.df,
                        key="key",
                        mode="r",
                        **self.kwargs,
                    )
                elif ext == "html":
                    self.df = pd.read_html(
                        self.df,
                        **self.kwargs,
                    )
                elif ext == "json":
                    self.df = pd.read_json(
                        self.df,
                        **self.kwargs,
                    )
                elif ext == "prqt":
                    self.df = pd.read_parquet(
                        self.df,
                        **self.kwargs,
                    )
                elif ext == "pkl":
                    self.df = pd.read_pickle(
                        self.df,
                        # **self.kwargs,
                    )
                    if "nrows" in self.kwargs:
                        self.df = self.df.head(self.kwargs["nrows"])
                elif ext == "xml":
                    self.df = pd.read_xml(
                        self.df,
                        **self.kwargs,
                    )
                elif ext in ["yaml", "yml"]:
                    self.df = pd.json_normalize(
                        self.df,
                        **self.kwargs,
                    )
                else:
                    raise ValueError(f"Invalid ext: {self.df}")

        else:
            raise ValueError(f"Invalid type: {type(self.df)}")

        if not isinstance(self.df, pd.DataFrame):
            raise ValueError(f"Invalid DataFrame: {type(self.df)}")

        if self.df.empty or self.df.shape[0] == 0 or self.df.shape[1] == 0:
            raise ValueError(f"Empty DataFrame: {self.df.shape}")

    def save(self, filename: str = None, ext: str = "pkl", compression: str = None):
        """
        Save DataFrame.

        :param str filename: File name
        :param str ext: File extension
        :param str compression: Compression type
        """
        ic()

        ext = self.ext if ext is None else ext
        compression = self.compression if compression is None else compression

        if self.file is not None:
            path = self.file.parents[0]
            if filename is None:
                filename = (
                    f"{self.file.stem.split('.')[0]}_DataFactory.{ext}"
                    if "DataFactory" not in self.file.stem
                    else f"{self.file.stem.split('.')[0]}.{ext}"
                )
        else:
            path = Path().cwd().parents[0] / "data"
            if filename is None:
                filename = f"{self.name}.{ext}"

        if ext not in filename:
            filename = f"{filename}.{ext}"

        if compression != "" and ext in ["pkl"]:
            filename = f"{filename}.{compression}"

        self.logger.info("")
        self.logger.info(f"Save to {path / filename}")

        if ext == "csv":
            self.df.to_csv(path / filename, index=False)
        elif ext == "xlsx":
            self.df.to_excel(path / filename)
        elif ext == "pkl":
            self.df.to_pickle(path / filename, compression=compression)
        else:
            raise ValueError(f"Invalid ext: {ext}")

        self.logger.info(f"Size {round(os.path.getsize(path / filename) / 1024**2, 2)} MB")

    def backup(self, copy: bool = False):
        """
        Backup DataFrame.

        :param bool copy:
        """
        if self.df is not None and isinstance(self.df, pd.DataFrame):
            self.df_bkp = self.df.copy(deep=copy)

    def restore(self):
        """
        Restore DataFrame.
        """
        if self.df_bkp is not None:
            self.df = self.df_bkp
            self.update()

    # @abstractmethod
    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        pass

    # @abstractmethod
    def process(self):
        """
        Process DataFrame.
        """
        pass

    # @abstractmethod
    def post_process(self):
        """
        Post-Process DataFrame.
        """
        pass

    def print(self, verbose: bool = False):
        """
        Print info.

        :param bool verbose: Verbose mode
        """
        self.logger.info("")
        self.logger.info(f"Shape  {self.df.shape}")
        self.logger.info(f"Memory {mem_usage(self.df):03.2f} MB")

        head = 10
        self.logger.info("")
        if not self.silent:
            self.print_all_columns(nrows=head)
        if verbose:
            if not self.silent:
                self.print_types()
                self.logger.info("")
                num = self.get_columns("numeric")
                cat = self.get_columns("category")
                dtm = self.get_columns("datetime")
                obj = self.get_columns("object")
                if len(num) != 0 or len(cat) != 0 or len(dtm) != 0 or len(obj) != 0:
                    if len(num) != 0:
                        self.logger.info("numeric")
                        with pd.option_context("display.max_columns", None):
                            display(self.df[num].describe(datetime_is_numeric=True))
                    if len(cat) != 0:
                        self.logger.info("category")
                        with pd.option_context("display.max_columns", None):
                            display(self.df[cat].describe())
                    if len(dtm) != 0:
                        self.logger.info("datetime")
                        with pd.option_context("display.max_columns", None):
                            display(self.df[dtm].describe(datetime_is_numeric=True))
                    if len(obj) != 0:
                        self.logger.info("object")
                        with pd.option_context("display.max_columns", None):
                            display(self.df[obj].describe())
                else:
                    with pd.option_context("display.max_columns", None):
                        display(self.df.describe(include="all", datetime_is_numeric=True))

                # display(
                #     self.df.memory_usage(deep=True)
                #     .apply(lambda x: round(x / 1024**2, 2))
                #     .to_frame(name="Memory [MB]")
                # )
                del num, obj

    def print_size(self):
        """
        Print size.
        """
        self.logger.info("")
        self.logger.info("Size")
        if self.df is not None:
            self.logger.info(f"df:      {self.df.shape}")
        if self.X is not None:
            self.logger.info(f"X:       {self.X.shape}")
        if self.y is not None:
            self.logger.info(f"y:       {len(self.y)}")
        if self.X_train is not None:
            self.logger.info(f"X_train: {self.X_train.shape}")
        if self.y_train is not None:
            self.logger.info(f"y_train: {len(self.y_train)}")
        if self.X_test is not None:
            self.logger.info(f"X_test:  {self.X_test.shape}")
        if self.y_test is not None:
            self.logger.info(f"y_test:  {len(self.y_test)}")

    def print_all_columns(self, nrows: int = None, sort: bool = False):
        """
        Print columns.

        :param int nrows: Number of rows to print
        :param bool sort: Number of rows to print
        """
        with pd.option_context("display.max_columns", None):
            if sort:
                display(self.df[sorted(self.columns)].head(n=nrows))
            else:
                display(self.df.head(n=nrows))

    def print_all_rows(self, nrows: int = None):
        """
        Print rows.

        :param int nrows: Number of rows to print
        """
        with pd.option_context("display.max_rows", None):
            display(self.df.head(n=nrows))

    def print_all(self, nrows: int = None):
        """
        Print all.

        :param int nrows: Number of rows to print
        """
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            display(self.df.head(n=nrows))

    def print_all_na(self, nrows: int = None, neg: bool = False):
        """
        Print all.

        :param int nrows: Number of rows to print
        :param bool neg: Print not na
        """
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            if not neg:
                display(self.df[self.df.isna().any(axis=1).head(n=nrows)])
            else:
                display(self.df[~self.df.isna().any(axis=1).head(n=nrows)])

    def print_counts(self, columns: Union[str, list] = None, verbose: bool = False):
        """
        Print counts.

        :param Union[str, list] columns: Columns to print
        :param bool verbose: Verbose mode
        """
        if not columns:
            columns = self.columns
        elif columns == "keys":
            columns = self.keys
        elif not isinstance(columns, list):
            columns = [columns]

        if self.df[columns].isnull().sum()[0] != 0:
            self.logger.info(
                f"NaNs {self.df[columns].isnull().sum()[0]} ({self.df[columns].isnull().sum()[0]/(self.df[columns].shape[0]*self.df[columns].shape[1]):.2%})"
            )
            self.logger.info("")
        if len(columns) > 1:
            self.logger.info(
                f"{columns} {'unique' if len(self.df[columns].drop_duplicates()) == self.df.shape[0] else 'not unique'}"
            )
            self.logger.info(
                f"Unique values: {len(self.df[columns].drop_duplicates())} ({len(self.df[columns].drop_duplicates()) / self.df[columns].shape[0]:.2%})"
            )
            self.logger.info("")
        for c in columns:
            self.logger.info(f"{c} {'unique' if self.df[c].is_unique else 'not unique'}")
            self.logger.info(
                f"Unique values: {self.df[c].nunique(dropna=False)} ({self.df[c].nunique(dropna=False) / self.df[c].shape[0]:.2%})"
            )
        if verbose or max(self.df[columns].nunique(dropna=False).to_list()) != self.df.shape[0]:

            def counts(columns):
                counts = self.df[columns].value_counts(dropna=False)
                percs = self.df[columns].value_counts(dropna=False, normalize=True)
                percs = percs.multiply(100)
                display(pd.concat([counts, percs], axis=1, keys=["Counts", "Percentage"]))
                del counts, percs

            if len(columns) > 3:
                self.logger.info("")
                for c in columns:
                    if self.df[c].dtype.kind != "f" and self.df[c].nunique(dropna=False) < 1000:
                        self.logger.info(f"{c}")
                        self.logger.info(
                            f"Unique values: {self.df[c].nunique(dropna=False)} ({self.df[c].nunique(dropna=False) / self.df[c].shape[0]:.2%})"
                        )
                        counts(c)
            else:
                counts(columns)

    def print_duplicates(self, columns: Union[str, list] = None):
        """
        Print duplicates.

        :param Union[str, list] columns: Columns to print
        """
        if not columns:
            columns = self.columns
        elif columns == "keys":
            columns = self.keys
        elif not isinstance(columns, list):
            columns = [columns]

        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            self.display(self.df[self.df.duplicated(subset=columns, keep=False)])

    def display(self, df):
        """
        Display DataFactory.
        """
        if not self.silent:
            display(df)

    def detect_types(self):
        """
        Detect column types with Data Analysis Baseline Library.

        .. seealso:: https://dabl.github.io
        .. seealso:: https://pandas.pydata.org/docs/user_guide/basics.html#basics-dtypes
        """
        ic()

        # from dabl import detect_types

        self.dbltypes = pd.DataFrame()
        # self.dbltypes = detect_types(self.df, verbose=0)
        # /home/bifani/miniconda3/envs/ama/lib/python3.9/site-packages/dabl/preprocessing.py:200: FutureWarning: The behavior of `series[i:j]` with an integer-dtype index is deprecated. In a future version, this will be treated as *label-based* indexing, consistent with e.g. `series[i]` lookups. To retain the old behavior, use `series.iloc[i:j]`. To get the future behavior, use `series.loc[i:j]`.
        #   if series[:10].isna().any():

        import pandas.api.types as ptypes

        self.types = pd.DataFrame()

        self.types["bool"] = self.df.dtypes.apply(ptypes.is_bool_dtype)
        self.types["category"] = self.df.dtypes.apply(ptypes.is_categorical_dtype)
        self.types["datetime"] = self.df.dtypes.apply(ptypes.is_datetime64_any_dtype)
        self.types["timedelta"] = self.df.dtypes.apply(ptypes.is_timedelta64_dtype)
        self.types["float"] = self.df.dtypes.apply(ptypes.is_float_dtype)
        self.types["integer"] = self.df.dtypes.apply(ptypes.is_integer_dtype)
        self.types["numeric"] = self.df.dtypes.apply(ptypes.is_numeric_dtype)
        self.types["string"] = self.df.dtypes.apply(ptypes.is_string_dtype)
        self.types["object"] = self.df.dtypes.apply(ptypes.is_object_dtype)

        def combine(df, c0, c1):
            df[c0] = df[c0].combine(df[c1], lambda s1, s2: any([s1, s2]))
            df.drop([c1], axis=1, inplace=True)
            return df

        if self.types["datetime"].any() and self.types["timedelta"].any():
            self.types = combine(self.types, "datetime", "timedelta")
        if self.types["object"].any() and self.types["string"].any():
            self.types = combine(self.types, "object", "string")

        self.dtypes = self.df.dtypes.apply(lambda x: x.name).to_dict()
        # self.dtypes = dict(sorted(self.dtypes.items()))

    def print_dtypes(self):
        """
        Print dtypes.
        """
        self.logger.info("")
        for key, value in self.dtypes.items():
            if value != "datetime64[ns]":
                print(f'"{key}" : "{value}",')
        self.logger.info("")

    def print_types(self):
        """
        Print types.
        """
        self.logger.info("")
        self.logger.info("Data Types")
        data = pd.DataFrame(self.df.dtypes, columns=["Type"])
        with pd.option_context("display.max_rows", None):
            self.display(data.sort_index())
            try:
                self.display(data["Type"].value_counts().to_frame(name="Counts"))
            except Exception:
                pass
        del data

        for t in [self.types, self.dbltypes]:
            if not len(t.columns):
                continue
            self.logger.info("")
            for c in sorted(t.columns):
                cols = t.loc[t[c] == True].index.to_list()  # NOQA E712
                if cols != []:
                    self.logger.info("{:14} {} - {}".format(c, len(cols), sorted(cols, key=lambda x: str(x))))
                del cols
            self.logger.info("")
            self.logger.info(
                "{:14} {} - {}".format(
                    "TOTAL",
                    t.shape[0],
                    sorted(list(t.index), key=lambda x: str(x)),
                )
            )

    def convert_types(self):
        """
        Convert types.
        """
        ic()

        self.logger.info("Convert Types")

        dtypes1 = set(self.dtypes.items())
        self.df = self.df.convert_dtypes()
        self.update()
        dtypes2 = set(self.dtypes.items())

        if dtypes1 ^ dtypes2 != set():
            self.logger.info(sorted(dtypes1 ^ dtypes2))

        del dtypes1, dtypes2

    def infer_types(self):
        """
        Infer types.
        """
        ic()

        self.logger.info("Infer Types")

        dtypes1 = set(self.dtypes.items())
        self.df = self.df.infer_objects()
        self.update()
        dtypes2 = set(self.dtypes.items())

        if dtypes1 ^ dtypes2 != set():
            self.logger.info(sorted(dtypes1 ^ dtypes2))

        del dtypes1, dtypes2

    def set_types(self, columns: Union[str, list, dict], dtype: str = None, format: str = None):
        """
        Set columns types.

        .. seealso::  https://pbpython.com/pandas_dtypes.html

        :param Union[str, list, dict] columns: Column name
        :param str dtype: Column type
        :param str format: Datetime format
        """
        ic()

        if columns is None:
            return

        self.logger.info("")
        if isinstance(columns, dict):
            self.logger.info(f"Set Column Types: {columns}")

            for k, v in columns.items():
                self.df[k] = self.df[k].astype(v)
        else:
            if dtype is None:
                return
            if not isinstance(columns, list):
                columns = [columns]

            self.logger.info(f"Set Column Types: {columns} -> {dtype} {format if format is not None else ''}")

            if "c2p" in dtype:
                self.df[columns] = self.df[columns].replace(r",", ".", regex=True)
                dtype = dtype.replace("c2p", "").strip()
            if "d0" in dtype:
                self.df[columns] = self.df[columns].replace(r"\.0$", "", regex=True)
                dtype = dtype.replace("d0", "").strip()
            if "l0" in dtype:
                self.df[columns] = self.df[columns].applymap(lambda x: x.lstrip("0") if isinstance(x, str) else x)
                dtype = dtype.replace("l0", "").strip()
            if "loc_IT" in dtype:
                loc_IT = {
                    "gen": "01",
                    "feb": "02",
                    "mar": "03",
                    "apr": "04",
                    "mag": "05",
                    "giu": "06",
                    "lug": "07",
                    "ago": "08",
                    "set": "09",
                    "ott": "10",
                    "nov": "11",
                    "dic": "12",
                }
                for k, v in loc_IT.items():
                    self.df[columns] = self.df[columns].replace(k, v, regex=True)
                dtype = dtype.replace("loc_IT", "").strip()

            if dtype == "datetime":
                infer_datetime_format = True if not format else False
                self.df[columns] = self.df[columns].apply(
                    pd.to_datetime, infer_datetime_format=infer_datetime_format, format=format, errors="coerce"
                )
            elif dtype == "numeric":
                self.df[columns] = self.df[columns].apply(pd.to_numeric, errors="coerce")
            elif dtype == "int":
                self.df[columns] = self.df[columns].fillna("0").astype("int64")
            elif dtype == "float":
                self.df[columns] = self.df[columns].fillna("0").astype("float64")
            elif dtype == "float":
                self.df[columns] = self.df[columns].fillna("NAN").astype("category")
            else:
                self.df[columns] = self.df[columns].astype(dtype)

        self.update()

    def detect_columns(self):
        """
        Detect columns.
        """
        ic()

        self.columns = self.df.columns
        self.features = (
            self.columns.drop(self.label) if self.label is not None and self.label in self.columns else self.columns
        )
        self.columns = list(self.columns)
        self.features = list(self.features)

    def get_columns(self, dtype: str = None) -> list:
        """
        Get columns by type.

        :param str dtype: Column type
        :return: Column list
        """
        ic()

        if self.types is None:
            return []

        if dtype is not None:
            if dtype in self.types.columns:
                columns = self.types[dtype][self.types[dtype]].index.to_list()
            else:
                columns = dtype if isinstance(dtype, list) else [dtype]
        else:
            columns = self.columns

        return columns

    def get_column(self, columns: Union[int, str, list]) -> tuple:
        """
        Get column names by index.

        :param Union[int, str, list] columns: Column index
        :return: Column name
        """
        ic()

        if not isinstance(columns, list):
            columns = [columns]

        tup = tuple()
        for c in columns:
            c = self.columns[c] if isinstance(c, int) else c if c in self.columns else None
            tup = tup + (c,)

        if len(tup) == 1:
            tup = tup[0]

        return tup

    def get_features(self, dtype: str = None) -> list:
        """
        Get features by type.

        :param str dtype: Column type
        :return: Feature list
        """
        ic()

        if dtype is not None:
            features = self.get_columns(dtype)
        else:
            features = self.features

        if self.label in features:
            features = features.drop([self.label])

        return features

    def print_columns(self):
        """
        Print columns.
        """
        self.logger.info("")
        self.logger.info(f"Columns:  {len(self.columns)} - {self.columns}")
        if self.label is not None:
            self.logger.info(f"Features: {len(self.features)} - {self.features}")
            self.logger.info(f"Label:    {self.label} - {'CONTINUOUS' if self.is_continuous else 'CATEGORICAL'}")
            if not self.is_continuous:
                self.logger.info(f"Classes:  {len(self.classes)} - {self.classes}")
            else:
                self.logger.info(f"Classes:  {len(self.classes)} - {self.y.dtype}")

    def drop_columns(self, columns: Union[str, list]):
        """
        Drop columns.

        :param Union[str, list] columns: Column name
        """
        ic()

        if not isinstance(columns, list):
            columns = [columns]

        columns = [c for c in columns if c in self.df.columns]
        if columns:
            self.logger.info("")
            self.logger.info(f"Drop columns: {len(columns)} - {columns}")

            shp1 = self.df.shape
            self.df.drop(
                columns=columns,
                inplace=True,
            )
            self.update()

            shp2 = self.df.shape
            self.logger.info(f"Shape: {shp1} -> {shp2} ({shp2[1]/shp1[1]:.2%})")

    def set_columns(self, columns: Union[str, list], copy: bool = False):
        """
        Set columns.

        :param Union[str, list] columns: Column name
        """
        ic()

        if not isinstance(columns, list):
            columns = [columns]

        columns = [c for c in columns if c in self.df.columns]
        if columns:
            self.logger.info("")
            self.logger.info(f"Set columns: {len(columns)} - {columns}")

            if self.df_bkp is None:
                self.backup(copy=copy)
            else:
                self.restore()

            shp1 = self.df.shape
            self.df = self.df[columns]
            self.update()

            shp2 = self.df.shape
            self.logger.info(f"Shape: {shp1} -> {shp2} ({shp2[1]/shp1[1]:.2%})")

    def drop_rows(self, rows: Union[int, list]):
        """
        Drop rows.

        :param Union[str, list] rows: Row index
        """
        ic()

        if not isinstance(rows, list):
            rows = [rows]

        self.logger.info("")
        self.logger.info(f"Drop rows: {len(rows)}")

        shp1 = self.df.shape
        self.df.drop(
            index=rows,
            inplace=True,
        )
        self.df.reset_index(drop=True, inplace=True)

        shp2 = self.df.shape
        self.logger.info(f"Shape: {shp1} -> {shp2} ({shp2[0]/shp1[0]:.2%})")

    def sort_columns(self):
        """
        Sort columns.
        """
        self.df[sorted(self.columns)]

    def unsort_columns(self):
        """
        Unsort columns.
        """
        self.df[self.columns]

    def split_columns(self, method: int = 2) -> dict:
        """
        Split columns.

        :param method:
        :return: Column dictionary
        """
        ic()

        columns = {}
        if method == 1:
            columns["num"] = self.get_data("numeric")
        elif method == 2:
            columns["num"] = self.get_data("numeric")
            columns["obj"] = self.get_data("object")
        elif method == 3:
            columns["con"] = self.get_data("float")
            columns["ord"] = self.get_data("integer")
            columns["cat"] = self.get_data("object")
        else:
            raise ValueError(f"Invalid method: {method}")

        return columns

    def update(self):
        """
        Update DataFrame.
        """
        ic()

        self.detect_types()
        self.detect_columns()

    def get_data(self, type: str):  # NOQA C901
        """
        Get data.

        :param str type: Data type
        :return: DataFrame or Series
        """
        ic()

        if type is None:
            raise ValueError(f"Invalid data type: {type}")
        elif type == "Xy":
            if not self.is_split_Xy:
                raise ValueError("No X/y split")
        elif "train" in type or "test" in type or "Xy_tt" in type:
            if not self.is_split_tt:
                raise ValueError("No Train/Test split")
        elif "valid" in type or "Xy_tv" in type:
            if not self.is_split_tvt:
                raise ValueError("No Train/Validation/Test split")

        self.logger.info("")
        self.logger.info(f"Data type: {type}")

        if type == "df":
            return self.df
        elif type == "Xy":
            return self.X, self.y
        elif type == "train":
            return self.X_train, self.y_train
        elif type == "valid":
            return self.X_valid, self.y_valid
        elif type == "test":
            return self.X_test, self.y_test
        elif type == "Xy_tt":
            return self.X_train, self.X_test, self.y_train, self.y_test
        elif type == "Xy_tv":
            return self.X_train, self.X_valid, self.y_train, self.y_valid
        elif type == "Xy_tvt":
            return self.X_train, self.X_valid, self.X_test, self.y_train, self.y_valid, self.y_test
        elif type == "dftrain":
            return pd.concat([self.X_train, self.y_train], axis=1)
        elif type == "dfvalid":
            return pd.concat([self.X_valid, self.y_valid], axis=1)
        elif type == "dftest":
            return pd.concat([self.X_test, self.y_test], axis=1)

        else:
            try:
                X = None
                if len(self.get_columns(type)) != 0:
                    X = self.df[self.get_columns(type)]
                return X
            except Exception:
                raise ValueError(f"Invalid data type: {type}")

    def check_data(self):
        """
        Check data for NaNs and duplicates.
        """
        ic()

        nans = self.df.isnull().sum().sum()
        if nans != 0:
            self.logger.info("")
            if self.nans and self.nans != nans:
                self.logger.info(f"Data has NaNs: {self.nans} -> {nans} ({nans/self.nans:.2%})")
            self.logger.info(f"Data has NaNs: {nans} ({nans / (self.df.shape[0]*self.df.shape[1]):.2%})")
            with pd.option_context("display.max_rows", None):
                self.display(self.df.stb.missing(clip_0=True, style=True))
            if not self.nans:
                self.nans = nans

        dups = self.df.duplicated().sum()
        if dups != 0:
            self.logger.info("")
            if self.dups and self.dups != dups:
                self.logger.info(f"Data has Duplicates: {self.dups} -> {dups} ({dups/self.dups:.2%})")
            self.logger.info(f"Data has Duplicates: {dups} ({dups / self.df.shape[0]:.2%})")
            if dups < 100:
                self.print_duplicates()
            if not self.dups:
                self.dups = dups
        if self.df.index.has_duplicates:
            self.logger.info("")
            self.logger.info("Data has Index Duplicates")

    def check_keys(self):
        """
        Check keys.
        """
        if self.keys:
            self.logger.info("")
            self.logger.info(f"Keys: {len(self.keys)} - {self.keys}")

            nans = self.df[self.keys].isnull().sum().sum()
            if nans != 0:
                self.logger.info("")
                self.logger.info(f"Keys has NaNs: {nans} ({nans / (self.df.shape[0]*self.df.shape[1]):.2%})")
                with pd.option_context("display.max_rows", None):
                    self.display(self.df[self.keys].stb.missing(clip_0=True, style=True))
            dups = self.df.duplicated(subset=self.keys).sum()
            if dups != 0:
                self.logger.info("")
                self.logger.info(f"Keys has Duplicates: {dups} ({dups / self.df.shape[0]:.2%})")
                if dups < 100 and not self.silent:
                    self.print_duplicates("keys")

    def check_multicollinearity(self):
        """
        Check multi-collinearity.
        """
        X = self.get_data("numeric")
        self.logger.info("")
        self.logger.info("Check Multicollinearity")
        from statsmodels.stats.outliers_influence import variance_inflation_factor
        from statsmodels.tools.tools import add_constant

        vif = pd.DataFrame()
        df = add_constant(X)
        vif["Feature"] = df.columns
        vif["VIF"] = [variance_inflation_factor(df.values, i) for i in range(df.shape[1])]
        with pd.option_context("display.max_rows", None):
            display(vif.sort_values("VIF", ascending=False))

    def check_outliers(self, score: float = 3, columns: Union[str, list] = None, inplace=False):
        """
        Check outliers.

        :param float score: Score
        :param Union[str, list] columns: Column name
        """
        X = self.get_data("numeric")

        self.logger.info("")
        self.logger.info(f"Check Outliers: {score}")

        from scipy import stats

        zscore = np.abs(stats.zscore(X))
        zscore = np.nan_to_num(zscore)
        zscore = zscore < score
        zscore = zscore.all(axis=1)
        X_score = X[zscore]
        if X_score.shape[0] != X.shape[0]:
            self.logger.info(f"Outliers: {len(X.index) - len(X_score.index)}")
            display(X[~zscore])
            if inplace:
                self.logger.info(f"Outliers: {len(X.index)} -> {len(X_score.index)}")
                self.df = self.df[zscore]
                if self.X is not None:
                    self.X = self.X[zscore]
                if self.y is not None:
                    self.y = self.y[zscore]
                self.print()

    def check_variance(self, threshold: float = 0.0, inplace=False):
        """
        Check variance.

        :param float threshold: Threshold
        """
        X = self.get_data("numeric")
        self.logger.info("")
        self.logger.info(f"Check Variance: {threshold}")
        from sklearn.feature_selection import VarianceThreshold

        selector = VarianceThreshold(threshold=threshold)
        X_select = selector.fit_transform(X)
        if X_select.shape != X.shape:
            mask = selector.get_support()
            features = np.array(X.columns)
            features = features[(~mask).tolist()]
            self.logger.info(f"Variance: {features}")
            if inplace and len(features) != 0:
                self.features_drop(features, inplace=True)

    def clean_data(  # NOQA C901
        self,
        headers: bool = False,
        empty: bool = False,
        datetime: Union[str, list, bool] = None,
        tolo: Union[str, list, bool] = None,
        toup: Union[str, list, bool] = None,
        spaces: bool = False,
        chars: Union[str, list, bool] = None,
        dropdup: Union[str, list, bool] = None,
        keep: str = "first",
        dropna: Union[str, list, bool] = None,
        fillna: Union[str, list, bool] = None,
    ):
        """
        Clean data.

        :param bool headers: Column names
        :param bool empty: Empty to NaNs
        :param Union[str, list, bool] datetime: Datetime
        :param Union[str, list, bool] tolo: To Lower
        :param Union[str, list, bool] toup: To Upper
        :param bool spaces: Extra space
        :param Union[str, list, bool] chars: Special characters
        :param Union[str, list, bool] dropdup: Drop duplicates
        :param str keep: Keep duplicates
        :param Union[str, list, bool] dropna: Drop NaNs
        :param Union[str, list, bool] fillna: Fill NaNs
        """
        ic()

        self.logger.info("")

        if headers:
            self.logger.info("Clean Headers")
            self.df = clean_headers(self.df, case="const")
            self.update()

        if empty:
            self.logger.info("Clean Empty")
            self.df.replace(r"^ +$", np.nan, regex=True, inplace=True)

        if datetime is not None:
            if isinstance(datetime, bool) and datetime:
                columns = self.get_columns("datetime")
            else:
                columns = datetime if isinstance(datetime, list) else [datetime]

            if len(columns) != 0:
                format = "%Y-%m-%d"
                self.logger.info(f"Clean Dates: {columns} -> {format}")
                for c in columns:
                    # self.df[columns] = self.df[columns].apply(pd.to_datetime, format=format, errors="coerce")
                    self.df[c] = pd.to_datetime(
                        pd.to_datetime(self.df[c], format=format, errors="coerce").dt.strftime(format), errors="coerce"
                    )

        if tolo is not None:
            if isinstance(tolo, bool) and tolo:
                columns = self.columns
            else:
                columns = tolo if isinstance(tolo, list) else [tolo]
            if len(columns) != 0:
                self.logger.info(f"To Lower: {columns}")
                self.df[columns] = self.df[columns].applymap(lambda x: x.upper() if isinstance(x, str) else x)
        if toup is not None:
            if isinstance(toup, bool) and toup:
                columns = self.columns
            else:
                columns = toup if isinstance(toup, list) else [toup]
            if len(columns) != 0:
                self.logger.info(f"To Upper: {columns}")
                self.df[columns] = self.df[columns].applymap(lambda x: x.upper() if isinstance(x, str) else x)

        if spaces:
            if len(self.get_columns("object")) != 0:
                self.logger.info("Extra Spaces")
                self.df[self.get_columns("object")] = self.df[self.get_columns("object")].applymap(
                    lambda x: re.sub(" +", " ", x.lstrip().rstrip()) if isinstance(x, str) else x
                )

        if chars is not None:
            if isinstance(chars, bool) and chars:
                columns = self.columns
            else:
                columns = chars if isinstance(chars, list) else [chars]
            if len(columns) != 0:
                self.logger.info(f"Special Characters: {columns}")
                self.df[columns] = self.df[columns].applymap(
                    lambda x: x.replace("'", " ").replace('"', " ") if isinstance(x, str) else x
                )

        if dropdup is not None:
            if isinstance(dropdup, bool) and dropdup:
                columns = None
            else:
                columns = dropdup if isinstance(dropdup, list) else [dropdup]
            if self.df.duplicated(subset=columns).sum().sum() != 0:
                self.logger.info(f"Drop Duplicates: {columns}" if columns else "Drop Duplicates")
                shp1 = self.df.shape
                self.df.drop_duplicates(subset=columns, keep=keep, inplace=True, ignore_index=True)
                shp2 = self.df.shape
                self.logger.info(f"Shape: {shp1} -> {shp2} ({shp2[0]/shp1[0]:.2%})")

        if dropna is not None:
            if isinstance(dropna, bool) and dropna:
                columns = self.columns
            else:
                columns = dropna if isinstance(dropna, list) else [dropna]
            if len(columns) != 0 and self.df[columns].isnull().sum().sum() != 0:
                self.logger.info(f"Drop NaNs: {columns}" if columns else "Drop NaNs")
                shp1 = self.df.shape
                with pd.option_context("mode.use_inf_as_na", True):
                    self.df.dropna(subset=columns, inplace=True)
                shp2 = self.df.shape
                self.logger.info(f"Shape: {shp1} -> {shp2} ({shp2[0]/shp1[0]:.2%})")

        if fillna is not None:
            nans = self.df.isnull().sum().sum()
            if nans != 0:
                FILLNA = {
                    "category": "",
                    "numeric": 0,
                    "object": "",
                    # "datetime": pd.Timestamp.min + pd.DateOffset(1),
                }

                if isinstance(fillna, bool) and fillna:
                    columns = self.get_columns("category")
                    if len(columns) != 0:
                        if self.df[columns].isnull().sum().sum() != 0:
                            self.logger.info("Fill NaNs: category")
                            for c in columns:
                                self.df[c] = (
                                    self.df[c]
                                    .astype("category")
                                    .cat.add_categories(FILLNA.get("category"))
                                    .fillna(FILLNA.get("category"))
                                )
                    for c in ["numeric", "object"]:
                        columns = self.get_columns(c)
                        if len(columns) != 0:
                            if self.df[columns].isnull().sum().sum() != 0:
                                self.logger.info(f"Fill NaNs: {c}")
                                nans = self.df[columns].isnull().sum().sum()
                                values = dict(
                                    zip(
                                        columns,
                                        [FILLNA.get(c)] * len(columns),
                                    )
                                )
                                self.df.fillna(values, inplace=True)
                                self.logger.info(
                                    f"Fill NaNs: {nans} -> {self.df[columns].isnull().sum().sum()} ({self.df[columns].isnull().sum().sum()/nans:.2%})"
                                )
                else:
                    columns = self.get_columns(fillna)
                    if len(columns) != 0:
                        if self.df[columns].isnull().sum().sum() != 0:
                            self.logger.info(f"Fill NaNs: {fillna}")
                            nans = self.df[columns].isnull().sum().sum()
                            value = FILLNA.get(fillna, self.dtypes.get(fillna))
                            values = dict(
                                zip(
                                    columns,
                                    [value] * len(columns),
                                )
                            )
                            self.df.fillna(values, inplace=True)
                            self.logger.info(
                                f"Fill NaNs: {nans} -> {self.df[columns].isnull().sum().sum()} ({self.df[columns].isnull().sum().sum()/nans:.2%})"
                            )

    def split_Xy(self, label: str = None, features: list = None):
        """
        Split X/y.

        :param str label: Label name
        :param list features: Sub-set of features
        """
        ic()

        self.logger.info("")
        self.logger.info("Split X/y")
        self.set_label(label)
        if features is not None:
            self.features = features
        else:
            self.detect_columns()
        self.X = self.df[self.features]
        self.y = self.df[self.label]
        self.print_columns()
        self.is_split_Xy = True
        self.data_Xy = "train"

    def merge_Xy(self):
        """
        Merge X/y.
        """
        ic()

        shp1 = None
        if isinstance(self.df, pd.DataFrame):
            shp1 = self.df.shape
        self.df = pd.concat([self.X, self.y], axis=1)
        if shp1 is not None:
            shp2 = self.df.shape
            if shp1 != shp2:
                self.logger.info(f"Shape: {shp1} -> {shp2} ({shp2[1] / shp1[1]:.2%})")
        self.update()

    def split_tt(
        self,
        train_size: float = 0.80,
        shuffle: bool = True,
        stratify: bool = False,
        train_index: int = None,
        test_index: int = None,
        force: bool = False,
    ):
        """
        Split Train/Test.

        :param float train_size: Proportion to include in train split
        :param bool shuffle: Shuffle the data before splitting
        :param bool stratify: Split in a stratified fashion using the label
        :param int train_index: UpTo index
        :param int test_index: DownTo index
        :param bool force: Force splitting
        """
        ic()

        if not self.is_split_tt or force or "DATA_SPLIT" in self.X.columns:
            if not self.is_split_Xy:
                raise ValueError("No X/y split")

            self.logger.info("")
            if "DATA_SPLIT" in self.X:
                self.logger.info("ReSplit Train/Test")
                # print((self.X_train))
                self.X_train = self.X.query("DATA_SPLIT == 'train'")
                self.X_test = self.X.query("DATA_SPLIT == 'test'")
                self.y_train = self.y[self.X_train.index]
                self.y_test = self.y[self.X_test.index]
            else:
                self.logger.info(
                    f"Split Train/Test: {train_size:.{self.options['precision']}f} / {1-train_size:.{self.options['precision']}f}"
                )
                if not train_index and not test_index:
                    from sklearn.model_selection import train_test_split

                    self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
                        self.X,
                        self.y,
                        train_size=train_size,
                        shuffle=shuffle,
                        stratify=self.y if stratify else None,
                        random_state=self.options["random_state"],
                    )
                else:
                    self.X_train = self.X[:train_index]
                    self.X_test = self.X[test_index:]
                    self.y_train = self.y.loc[:train_index]
                    self.y_test = self.y.loc[test_index:]

            self.is_split_tt = True
            self.data_tt = "Xy_tt"

            self.logger.info(f"Features: {self.X_train.shape} / {self.X_test.shape}")
            self.logger.info(f"Label:    {len(self.y_train)} / {len(self.y_test)}")
        else:
            raise ValueError("Already split Train/Test")

    def merge_tt(self):
        """
        Merge Train/Test.
        """
        ic()

        if "DATA_SPLIT" not in self.X_train:
            self.X_train["DATA_SPLIT"] = "train"
        if "DATA_SPLIT" not in self.X_test:
            self.X_test["DATA_SPLIT"] = "test"
        self.X = pd.concat([self.X_train, self.X_test], axis=0)
        self.X["DATA_SPLIT"] = self.X["DATA_SPLIT"].astype("category")
        self.y = pd.concat([self.y_train, self.y_test], axis=0)
        self.merge_Xy()

    def split_tv(
        self,
        valid_size: float = 0.50,
        shuffle: bool = True,
        stratify: bool = False,
        valid_index: int = None,
        test_index: int = None,
        force: bool = False,
    ):
        """
        Split Test/Validation.

        :param float valid_size: Proportion to include in validation split
        :param bool shuffle: Shuffle the data before splitting
        :param bool stratify: Split in a stratified fashion using the label
        :param int valid_index: UpTo index
        :param int test_index: DownTo index
        :param bool force: Force splitting
        """
        ic()

        if not self.is_split_tvt or force or "DATA_SPLIT" in self.X.columns:
            if not self.is_split_Xy:
                raise ValueError("No X/y split")
            if not self.is_split_tt:
                raise ValueError("No Train/Test split")

            self.logger.info("")
            if "DATA_SPLIT" in self.X:
                self.logger.info("ReSplit Test/Validation")
                self.X_train = self.X.query("DATA_SPLIT == 'train'")
                self.X_valid = self.X.query("DATA_SPLIT == 'valid'")
                self.X_test = self.X.query("DATA_SPLIT == 'test'")
                self.y_train = self.y[self.X_train.index]
                self.y_valid = self.y[self.X_valid.index]
                self.y_test = self.y[self.X_test.index]
            else:
                self.logger.info(
                    f"Split Test/Validation: {valid_size:.{self.options['precision']}f} / {1-valid_size:.{self.options['precision']}f}"
                )
                if not valid_index and not test_index:
                    from sklearn.model_selection import train_test_split

                    self.X_valid, self.X_test, self.y_valid, self.y_test = train_test_split(
                        self.X_test,
                        self.y_test,
                        train_size=valid_size,
                        shuffle=shuffle,
                        stratify=self.y_test if stratify else None,
                        random_state=self.options["random_state"],
                    )
                else:
                    self.X_valid = self.X_test[:valid_index]
                    self.X_test = self.X_test[test_index:]
                    self.y_valid = self.y_test.loc[:valid_index]
                    self.y_test = self.y_test.loc[test_index:]

            self.is_split_tvt = True
            self.data_tt = "Xy_tv"

            self.logger.info(f"Features: {self.X_valid.shape} / {self.X_test.shape}")
            self.logger.info(f"Label:    {len(self.y_valid)} / {len(self.y_test)}")
        else:
            raise ValueError("Already split Test/Validation")

    def split_tvt(
        self,
        train_size: float = 0.80,
        valid_size: float = 0.50,
        shuffle: bool = True,
        stratify: bool = False,
        train_index: int = None,
        valid_index: int = None,
        test_index: int = None,
        force: bool = False,
    ):
        """
        Split Test/Validation.

        :param float train_size: Proportion to include in train split
        :param float valid_size: Proportion to include in validation split
        :param bool shuffle: Shuffle the data before splitting
        :param bool stratify: Split in a stratified fashion using the label
        :param int train_index: UpTo index
        :param int valid_index: UpTo index
        :param int test_index: DownTo index
        :param bool force: Force splitting
        """
        ic()

        self.split_tt(
            train_size=train_size,
            shuffle=shuffle,
            stratify=stratify,
            train_index=train_index,
            test_index=test_index,
            force=force,
        )
        self.split_tv(
            valid_size=valid_size,
            shuffle=shuffle,
            stratify=stratify,
            valid_index=valid_index,
            test_index=test_index,
            force=force,
        )

        self.logger.info("")
        self.logger.info(
            f"{'Split' if 'DATA_SPLIT' not in self.X else 'ReSplit'} Train/Validation/Test: {train_size:.{self.options['precision']}f} / {(1-train_size)*valid_size:.{self.options['precision']}f} / {(1-train_size)*(1-valid_size):.{self.options['precision']}f}"
        )
        self.logger.info(f"Features: {self.X_train.shape} / {self.X_valid.shape} / {self.X_test.shape}")
        self.logger.info(f"Label:    {len(self.y_train)} / {len(self.y_valid)} / {len(self.y_test)}")

    def merge_tvt(self):
        """
        Merge Train/Validation/Test.
        """
        ic()

        if "DATA_SPLIT" not in self.X_train:
            self.X_train["DATA_SPLIT"] = "train"
        if "DATA_SPLIT" not in self.X_valid:
            self.X_valid["DATA_SPLIT"] = "valid"
        if "DATA_SPLIT" not in self.X_test:
            self.X_test["DATA_SPLIT"] = "test"
        self.X = pd.concat([self.X_train, self.X_valid, self.X_test], axis=0)
        self.X["DATA_SPLIT"] = self.X["DATA_SPLIT"].astype("category")
        self.y = pd.concat([self.y_train, self.y_valid, self.y_test], axis=0)
        self.merge_Xy()

    def query(self, query: str, nrows: int = None, inplace: bool = True):
        """
        Print all query.

        :param str query: Query
        :param int nrows: Number of rows to print
        :param bool inplace: Inplace
        """
        df = self.df.query(query).head(n=nrows)
        with pd.option_context("display.max_columns", None):
            display(df)
        if not inplace:
            return df
        del df

    def query_apply(self, query: str, verbose: bool = False, inplace: bool = False):
        """
        Apply query to self.df.

        :param str query: Query
        :param bool verbose: Verbose mode
        :param bool inplace: Inplace
        """
        if self.df_bkp is None and not inplace:
            self.backup(copy=True)

        try:
            self.logger.info("")
            self.logger.info(f"Query: {query}")

            shp1 = self.df.shape
            if inplace:
                self.df.query(query, inplace=inplace)
            else:
                self.df = self.df_bkp.query(query)
            self.update()

            shp2 = self.df.shape
            self.logger.info(f"Shape: {shp1} -> {shp2} ({shp2[0]/shp1[0]:.2%})")

            if verbose:
                self.print(verbose=verbose)
                self.check_data()

        except Exception as e:
            raise ValueError(f"{e}")

    def query_reset(self):
        """
        Reset self.df.
        """
        if self.df_bkp is not None:
            self.restore()
            self.print(verbose=True)

    def cross_table(
        self,
        c0: Union[str, list],
        c1: Union[str, list],
        c2: str = None,
        func: str = None,
        query: str = None,
    ):
        """
        Cross-table.

        :param Union[str, list] c0: Indexes
        :param Union[str, list] c1: Columns
        :param str c2: Values
        :param Union[str, list] func: Function to aggregate by
        :param str query: Filter query
        """
        if not isinstance(c0, list):
            c0 = [c0]
        if not isinstance(c1, list):
            c1 = [c1]

        df = self.df.query(query) if query else self.df

        c0l = [df[c].fillna("NaN") for c in c0]
        c1l = [df[c].fillna("NaN") for c in c1]
        c2 = df[c2] if c2 else None

        if func == "count":
            c2 = None
            func = None
        if func and not isinstance(func, list):
            func = [func]

        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            display(
                pd.crosstab(
                    index=c0l, columns=c1l, values=c2, aggfunc=func, margins=True, dropna=False, normalize=False
                )
            )

    def pivot_table(
        self,
        c0: Union[str, list],
        c1: Union[str, list],
        c2: str = None,
        func: str = None,
    ):
        """
        Pivot-table.

        :param Union[str, list] c0: Indexes
        :param Union[str, list] c1: Columns
        :param str c2: Values
        :param Union[str, list] func: Function to aggregate by
        """
        if not isinstance(c0, list):
            c0 = [c0]
        if not isinstance(c1, list):
            c1 = [c1]

        if func == "count":
            c2 = None
            func = None
        if func and not isinstance(func, list):
            func = [func]

        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            display(
                pd.pivot_table(
                    self.df,
                    values=c2,
                    index=c0,
                    columns=c1,
                    aggfunc=func,
                    margins=True,
                    dropna=False,
                    observed=False,
                    sort=True,
                )
            )

    def side_table(self, columns: Union[str, list] = None, value=None):
        """
        Sidetables.

        .. seealso:: https://github.com/chris1610/sidetable
        .. seealso:: https://towardsdatascience.com/pandas-sidetable-just-announced-708e5f65938f

        :param Union[str, list] columns: Columns to print
        :param value: Value
        """
        if columns is None:
            columns = self.columns
        if not isinstance(columns, list):
            columns = [columns]

        with pd.option_context("display.max_rows", None):
            display(self.df.stb.freq(columns, style=True))  # , value=value, thresh=None

    def groupby(
        self,
        c0: Union[str, list],
        c1: Union[str, list],
        func: Union[str, list],
        freq: str = "M",
        bins: int = None,
        query: str = None,
        sort: bool = False,
    ):
        """
        Group-by.

        :param Union[str, list] c0: Columns to group-by
        :param Union[str, list] c1: Columns to aggregate
        :param Union[str, list] func: Functions to aggregate by
        :param str freq: Grouper frequency for dates
        :param int bins: Number of bins
        :param str query: Query to filter group-by
        :param bool sort:
        """
        if not isinstance(c0, list):
            c0 = [c0]
        if not isinstance(c1, list):
            c1 = [c1]
        if not isinstance(func, list):
            func = [func]

        grouper = list()
        for c in c0:
            if c in self.get_columns("datetime"):  # "DATA" in c or c.startswith("DT_"):
                grouper.append(pd.Grouper(key=c, dropna=False, freq=freq))
            elif bins and c in self.get_columns("numeric"):  # "SUPERFICIE" in c:
                grouper.append(pd.cut(self.df[c], bins=bins, right=False))
            else:
                grouper.append(pd.Grouper(key=c, dropna=False))

        df = self.df.query(query) if query else self.df
        df = df.groupby(grouper, dropna=False)[c1].aggregate(func)

        if sort:
            df.sort_values(["IMPORTO_DELTA"], ignore_index=False, inplace=True)

        with pd.option_context("display.max_rows", None):
            display(df)

        del df

    def isin(
        self,
        df: pd.DataFrame,
        columns: Union[str, list],
        datafactory: bool = False,
    ):
        """
        Is in DataFrame.

        :param pd.DataFrame df: DataFrame
        :param Union[str, list] columns: Column name
        :param bool datafactory: Return DataFactory
        :return: Merged dataframe
        """
        if not isinstance(columns, list):
            columns = [columns]

        df1 = self.df[columns].sort_values(columns)
        df2 = df[columns].sort_values(columns)
        dm = df1.merge(df2, on=columns, how="inner")

        self.logger.info("")
        self.logger.info(f"df1 {df1.shape[0]}")
        self.logger.info(f"df2 {df2.shape[0]}")
        self.logger.info("")
        self.logger.info(f"Inner {dm.shape[0]}")

        if datafactory:
            dm = DataFactory(dm, silent=False, debug=False)

        results = list()
        results.append(dm)
        for c in columns:
            inc = df1[df1[c].isin(df2[c]) == True]  # NOQA E712
            exc = df1[df1[c].isin(df2[c]) != True]  # NOQA E712
            results.append(inc[c].to_list())
            results.append(exc[c].to_list())
            self.logger.info("")
            self.logger.info(c)
            self.logger.info(f"isin  {inc.shape[0]}")
            self.logger.info(f"~isin {exc.shape[0]}")

        del df1, df2

        return results

    def merge(
        self,
        df: pd.DataFrame,
        columns: Union[str, list] = None,
        lcolumns: Union[str, list] = None,
        rcolumns: Union[str, list] = None,
        how: str = "outer",
        # indicator: bool = False,
        validate: str = None,
        drop: bool = False,
        datafactory: bool = True,
        silent: bool = False,
    ):
        """
        Merge DataFrame.

        :param pd.DataFrame df: DataFrame
        :param Union[str, list] columns: Column name (common)
        :param Union[str, list] lcolumns: Column name (left)
        :param Union[str, list] rcolumns: Column name (right)
        :param str how: Merge type
        :param str validate: Validate merge (1:1, 1:m, m:1, m:m)
        :param bool drop: Drop duplicate merge columns
        :param bool datafactory: Return DataFactory
        :param bool silent: Silent mode
        :return: Merged dataframe
        """
        if columns is not None:
            if not isinstance(columns, list):
                columns = [columns]
            self.logger.info("")
            self.logger.info(f"On (both):  {columns}")

        if lcolumns is not None:
            if not isinstance(lcolumns, list):
                lcolumns = [lcolumns]
            self.logger.info("")
            self.logger.info(f"On (left):  {lcolumns}")
        else:
            lcolumns = columns

        if rcolumns is not None:
            if not isinstance(rcolumns, list):
                rcolumns = [rcolumns]
            self.logger.info(f"On (right): {rcolumns}")
        else:
            rcolumns = columns

        dm = self.df.merge(df, left_on=lcolumns, right_on=rcolumns, how=how, validate=validate, indicator=True)

        lunique = len(self.df[lcolumns].drop_duplicates()) == self.df.shape[0]
        runique = len(df[rcolumns].drop_duplicates()) == df.shape[0]
        self.logger.info("")
        if not lunique and not runique:
            self.logger.info("Many-2-Many")
        elif not lunique:
            self.logger.info("Many-2-One")
        elif not runique:
            self.logger.info("One-2-Many")
        else:
            self.logger.info("One-2-One")

        self.logger.info("")
        self.logger.info(f"df1: {self.df.shape[0]}")
        self.logger.info(f"df2: {df.shape[0]}")
        self.logger.info(f"dm:  {dm.shape[0]}")

        lonly = dm.query('_merge == "left_only"').shape[0]
        both = dm.query('_merge == "both"').shape[0]
        ronly = dm.query('_merge == "right_only"').shape[0]

        self.logger.info("")
        self.logger.info(f"Left:  {lonly} ({lonly/(both+lonly+ronly):.2%})")
        self.logger.info(f"Both:  {both} ({both/(both+lonly+ronly):.2%})")
        self.logger.info(f"Right: {ronly} ({ronly/(both+lonly+ronly):.2%})")

        if how == "outer":
            if columns is not None:
                cname = "_".join(columns)
                dm.rename(columns={"_merge": f"{cname}_merge"}, inplace=True)
            else:
                lname = "_".join(lcolumns)
                rname = "_".join(rcolumns)
                dm.rename(columns={"_merge": f"{lname}_merge_{rname}"}, inplace=True)
        else:
            dm.drop(columns="_merge", inplace=True)

        if drop:
            lcolumns2, rcolumns2 = list(set(lcolumns) - set(rcolumns)), list(set(rcolumns) - set(lcolumns))
            if how == "left":
                dm.drop(columns=rcolumns2, inplace=True)
            if how == "right":
                dm.drop(columns=lcolumns2, inplace=True)

        if datafactory:
            dm = DataFactory(dm, silent=silent, debug=False)
            dm.file = self.file

        return dm

    def merge_clean(
        self,
        columns: Union[str, list] = None,
        inplace: bool = False,
    ):
        """
        Merge DataFrame.

        :param Union[str, list] columns: Column name (common)
        :param bool inplace: Inplace
        """
        if columns is not None:
            if not isinstance(columns, list):
                columns = [columns]
        else:
            columns = sorted({c[:-2] for c in self.columns if c.endswith("_x") or c.endswith("_y")})

        self.logger.info("")
        self.logger.info(f"Merge clean columns: {columns}")
        self.logger.info("")

        for c in columns:
            if f"{c}_x" in self.columns and f"{c}_y" in self.columns:
                self.df[c] = self.df[f"{c}_x"].fillna(self.df[f"{c}_y"])
                self.logger.info(
                    f'{c}: {self.df[f"{c}_x"].isnull().sum().sum()} + {self.df[f"{c}_y"].isnull().sum().sum()} -> {self.df[f"{c}"].isnull().sum().sum()}'
                )
                if inplace:
                    self.df.drop(columns=[f"{c}_x", f"{c}_y"], inplace=True)
                else:
                    self.df.drop(columns=[f"{c}"], inplace=True)

        if inplace:
            self.display(self.df)
            self.detect_columns()
            self.check_data()
            self.check_keys()

    def merge_duplicates(self, c0: Union[str, list], c1: Union[str, list], func: Union[str, list, dict] = "sum"):
        """
        Merge duplicates.

        :param Union[str, list] c0: Columns to groupby
        :param Union[str, list] c1: Columns to merge
        :param Union[str, list] func: Functions to aggregate by
        """
        if not isinstance(c0, list):
            c0 = [c0]
        if not isinstance(c1, list):
            c1 = [c1]

        self.logger.info("")
        self.logger.info(f"Merge duplicates rows: groupby on {c0} - {func} on {c1}")

        imp = self.df.groupby(by=c0)[c1].agg(func).reset_index()
        self.clean_data(dropdup=c0)
        self.df = self.df.merge(imp, how="left", on=c0)
        c1_x = [f"{c}_x" for c in c1]
        c1_y = [f"{c}_y" for c in c1]
        c1_y_2_c1 = dict(zip(c1_y, c1))
        self.df.drop(columns=c1_x, inplace=True)
        self.df.rename(columns=c1_y_2_c1, inplace=True)
        del imp

    def sql_query(
        self,
        df: pd.DataFrame = None,
        query: str = None,
        datafactory: bool = True,
        silent: bool = False,
    ):
        """
        Run SQL query.

        :param Union[pd.DataFrame, list] df: DataFrames
        :param str query: SQL query
        :param bool datafactory: Return DataFactory
        :param bool silent: Silent mode
        :return: Dataframe
        """
        if not isinstance(df, list):
            df = [df]

        self.logger.info("")
        self.logger.info(f"SQL query: {query}")

        with connect(":memory:") as conn:
            self.logger.info("")
            self.logger.info("df0.to_sql")
            self.df.to_sql("df0", conn, index=False)
            for i, d in enumerate(df):
                self.logger.info(f"df{i + 1}.to_sql")
                d.to_sql(f"df{i+1}", conn, index=False)

            try:
                ds = pd.read_sql_query(query, conn)
            except Exception as e:
                raise ValueError(f"{e}")

        self.logger.info("")
        self.logger.info(f"df0 {self.df.shape[0]}")
        for i, d in enumerate(df):
            self.logger.info(f"df{i + 1} {d.shape[0]}")
        self.logger.info(f"ds  {ds.shape[0]}")

        if datafactory:
            ds = DataFactory(ds, silent=silent, debug=False)
            ds.file = self.file

        return ds

    def transform_data(self, columns: list, data: str, obj, inplace: bool = False, replace: bool = False):
        """
        Transform data.

        :param list columns: Column name
        :param obj: Object
        :return: File path
        """
        self.logger.info(f"Transform: {data}")

        X_train, X_test, y_train, y_test = None, None, None, None
        if data == "df":
            X_train = self.get_data(data)
        elif data == "Xy":
            X_train, y_train = self.get_data(data)
        elif data == "Xy_tt":
            X_train, X_test, y_train, y_test = self.get_data(data)
        else:
            raise ValueError(f"Invalid data type: {data}")

        X_train_columns = X_train[columns]
        X_test_columns = X_test[columns] if X_test is not None else None

        name = obj.__class__.__name__
        X_train_transformed, X_test_transformed = fit_transform(
            obj, X_train=X_train_columns, y_train=y_train, X_test=X_test_columns, y_test=y_test
        )

        X_train_transformed = convert_data(X_train_transformed, "dataframe")
        if X_test_transformed is not None:
            X_test_transformed = convert_data(X_test_transformed, "dataframe")

        X_train_transformed.columns = [f"{c}_{name}" for c in X_train_transformed.columns]
        if X_test_transformed is not None:
            X_test_transformed.columns = [f"{c}_{name}" for c in X_test_transformed.columns]

        if replace:
            X_train.drop(columns=columns, inplace=True)
            if X_test is not None:
                X_test.drop(columns=columns, inplace=True)

        X_train = pd.concat([X_train, X_train_transformed], axis=1)
        X_train = X_train.loc[:, ~X_train.columns.duplicated()]
        if X_test is not None:
            X_test = pd.concat([X_test, X_test_transformed], axis=1)
            X_test = X_test.loc[:, ~X_test.columns.duplicated()]

        if inplace:
            if data == "df":
                self.df = X_train
            elif data == "Xy":
                self.X, self.y = X_train, y_train
                self.merge_Xy()
            elif data == "Xy_tt":
                self.X_train, self.X_test, self.y_train, self.y_test = X_train, X_test, y_train, y_test
                self.merge_tt()
            self.print()
        else:
            self.logger.info("")
            self.logger.info(f"Shape  {X_train.shape}")
            display(X_train)
            if X_test is not None:
                self.logger.info("")
                self.logger.info(f"Shape  {X_test.shape}")
                display(X_test)
            if y_train is not None:
                self.logger.info("")
                self.logger.info(f"Shape  {y_train.shape}")
                display(y_train)
            if y_test is not None:
                self.logger.info("")
                self.logger.info(f"Shape  {y_test.shape}")
                display(y_test)

    def column_embed(
        self,
        columns: Union[str, list],
        method: str = "PCA",
        n_components: int = 2,
        inplace: bool = False,
        replace: bool = False,
        **kwargs,
    ):
        """
        Embed columns.

        :param Union[str, list] columns: Column name
        :param str method: Embed method
        :param int n_components: Number of components
        """
        if not isinstance(columns, list):
            columns = [columns]

        self.logger.info("")
        self.logger.info(f"Columns:   {columns}")
        self.logger.info(f"Embedder:  {method} - {n_components}")

        if len(columns) <= n_components:
            return

        if method == "PCA":
            from sklearn.decomposition import PCA

            embedder = PCA(n_components=n_components, random_state=self.kwargs["random_state"], **kwargs)
        elif method == "IPCA":
            from sklearn.decomposition import IncrementalPCA

            embedder = IncrementalPCA(n_components=n_components, batch_size=10, **kwargs)
        elif method == "LDA":
            from sklearn.discriminant_analysis import LinearDiscriminantAnalysis

            embedder = LinearDiscriminantAnalysis(n_components=n_components, **kwargs)
        elif method == "NCA":
            from sklearn.neighbors import NeighborhoodComponentsAnalysis

            embedder = NeighborhoodComponentsAnalysis(
                n_components=n_components, random_state=self.kwargs["random_state"], **kwargs
            )
        elif method == "MDS":
            from sklearn.manifold import MDS

            embedder = MDS(
                n_components=n_components,
                random_state=self.kwargs["random_state"],
                n_jobs=self.kwargs["n_jobs"],
                **kwargs,
            )
        elif method == "TSNE":
            from sklearn.manifold import TSNE

            embedder = TSNE(
                n_components=n_components,
                random_state=self.kwargs["random_state"],
                n_jobs=self.kwargs["n_jobs"],
                **kwargs,
            )
        elif method == "UMAP":
            import umap

            embedder = umap.UMAP(n_components=n_components, random_state=self.kwargs["random_state"], **kwargs)
        else:
            raise ValueError(f"Invalid method: {method}")

        # X_train_embedded, X_test_embedded = self.__scale(X_train[columns], X_test[columns], "Standard")
        self.transform_data(columns, "df", embedder, inplace=inplace, replace=replace)

        if hasattr(embedder, "explained_variance_ratio_"):
            self.logger.info(f"Variation ratio: {embedder.explained_variance_ratio_}")
        if hasattr(embedder, "stress_"):
            self.logger.info(f"Stress: {embedder.stress_}")
        if hasattr(embedder, "kl_divergence_"):
            self.logger.info(f"Kullback-Leibler divergence: {embedder.kl_divergence_}")

        # if self.options["backend"] == "yellowbrick":
        #     from yellowbrick.contrib import ScatterVisualizer
        #
        #     plt.figure()
        #     viz = ScatterVisualizer(features=components, classes=self.classes, alpha=0.5)
        #     viz.fit(X_train_embedded, y_train.to_numpy())
        #     viz.transform(X_train_embedded)
        #     viz.show()
        #     plt.tight_layout()
        # elif hasattr(embedder, "components_"):
        #     plt.figure(figsize=(8, 4))
        #     plt.imshow(embedder.components_, interpolation="none", cmap="plasma")
        #     plt.gca().set_xticks(np.arange(-0.5, len(features)))
        #     plt.gca().set_yticks(np.arange(0.5, n_components))
        #     plt.gca().set_xticklabels(features, rotation=90, ha="left", fontsize=12)
        #     plt.gca().set_yticklabels(components, va="bottom", fontsize=12)
        #     plt.colorbar(
        #         orientation="horizontal",
        #         ticks=[embedder.components_.min(), 0, embedder.components_.max()],
        #         pad=0.65,
        #     )
        #     plt.tight_layout()
        #     plt.show()

    def column_impute(
        self,
        columns: Union[str, list],
        method: str = "simple",
        strategy: str = "mean",
        fill_value: float = None,
        inplace: bool = False,
        replace: bool = False,
        **kwargs,
    ):
        """
        Impute columns.

        .. seealso:: https://github.com/kearnz/autoimpute

        :param Union[str, list] columns: Column name
        :param str method: Impute method
        :param str strategy: Strategy
        :param float fill_value: Fill value
        """
        if not isinstance(columns, list):
            columns = [columns]

        self.logger.info("")
        self.logger.info(f"Columns:   {columns}")
        self.logger.info(f"Imputer:   {method}")

        if self.df[columns].isnull().sum().sum() != 0:
            data = pd.DataFrame(self.df[columns].isnull().sum(), columns=["Null"])
            data = data[data["Null"] != 0]

            if method == "Simple":
                from sklearn.impute import SimpleImputer

                imputer = SimpleImputer(
                    missing_values=np.nan,
                    strategy=strategy,
                    fill_value=fill_value,
                    **kwargs,
                )
            elif method == "Iterative":
                from sklearn.experimental import enable_iterative_imputer
                from sklearn.impute import IterativeImputer

                imputer = IterativeImputer(
                    max_iter=10,
                    random_state=self.kwargs["random_state"],
                    **kwargs,
                )
            elif method == "KNN":
                from sklearn.impute import KNNImputer

                imputer = KNNImputer(
                    n_neighbors=5,
                    **kwargs,
                )
            elif method == "Mice":
                from autoimpute.analysis import MiceImputer

                imputer = MiceImputer(
                    **kwargs,
                )
            # elif method == "MiLinear":
            #     from autoimpute.analysis import MiLinearRegression
            #     imputer = MiLinearRegression()
            #     # fit the model on each multiply imputed dataset and pool parameters
            #     imputer.fit(X_col, y)
            #     # get summary of fit, which includes pooled parameters under Rubin's rules
            #     # also provides diagnostics related to analysis after multiple imputation
            #     imputer.summary()
            else:
                raise ValueError(f"Invalid method: {method}")

            display(self.df[data.index].describe())
            self.transform_data(columns, "df", imputer, inplace=inplace, replace=replace)
            display(self.df[data.index].describe())

    def column_interaction(
        self,
        columns: Union[str, list],
        degree: int = 2,
        interaction_only: bool = False,
        inplace: bool = False,
        **kwargs,
    ):
        """
        Interactions among columns.

        :param Union[str, list] columns: Column name
        :param int degree: Degree
        :param bool interaction_only: Drop non-interactions
        """
        if not isinstance(columns, list):
            columns = [columns]

        self.logger.info("")
        self.logger.info(f"Columns:   {columns}")
        self.logger.info(f"Interacts: {degree}")

        if degree > 1:
            X = self.get_data("df")
            X_num = X[columns]

            from sklearn.preprocessing import PolynomialFeatures

            poly = PolynomialFeatures(degree=degree, interaction_only=interaction_only, **kwargs)
            X_poly = poly.fit_transform(X_num)
            columns_poly = poly.get_feature_names_out(input_features=columns)
            X_poly = pd.DataFrame(data=X_poly, columns=columns_poly)
            X_poly.drop(columns="1", inplace=True)
            X_poly.drop(columns=columns, inplace=True)
            if inplace:
                self.X = pd.concat([X_poly, X[X.columns.difference(X_num.columns)]], axis=1)
                self.merge_Xy()
                self.print()
            else:
                self.logger.info("")
                self.logger.info(f"Shape  {X_poly.shape}")
                display(X_poly)

    def column_scale(
        self,
        columns: Union[str, list],
        method: str = "MinMax",
        inplace: bool = False,
        replace: bool = False,
        **kwargs,
    ):
        """
        Scale columns.

        :param Union[str, list] columns: Column name
        :param str method: Scale method
        """
        if not isinstance(columns, list):
            columns = [columns]

        self.logger.info("")
        self.logger.info(f"Columns:   {columns}")
        self.logger.info(f"Scaler:    {method}")

        if method == "Standard":
            from sklearn.preprocessing import StandardScaler

            scaler = StandardScaler(**kwargs)
        elif method == "MinMax":
            from sklearn.preprocessing import MinMaxScaler

            scaler = MinMaxScaler(**kwargs)
        elif method == "Robust":
            from sklearn.preprocessing import RobustScaler

            scaler = RobustScaler(**kwargs)
        elif method == "Power":
            from sklearn.preprocessing import PowerTransformer

            scaler = PowerTransformer(**kwargs)
        else:
            raise ValueError(f"Invalid method: {method}")

        self.transform_data(columns, "df", scaler, inplace=inplace, replace=replace)

    def column_transform(  # NOQA C901
        self,
        columns: Union[str, list],
        method: str,
        pos_label=None,
        bins: int = 10,
        inplace: bool = False,
        replace: bool = False,
        **kwargs,
    ):
        """
        Transform columns.

        .. seealso:: https://towardsdatascience.com/6-ways-to-encode-features-for-machine-learning-algorithms-21593f6238b0

        :param Union[str, list] columns: Column name
        :param str method: Transform method
        :param pos_label: Positive label
        :param int bins: Bins
        """
        if not isinstance(columns, list):
            columns = [columns]

        self.logger.info("")
        self.logger.info(f"Columns:   {columns}")
        self.logger.info(f"Transformer: {method}")

        # if pos_label is not None:
        #     y[y != pos_label] = 0
        #     y[y == pos_label] = 1
        # el

        transformer = None
        if method == "LBinarizer":
            from sklearn.preprocessing import LabelBinarizer

            transformer = LabelBinarizer(**kwargs)
        elif method == "LEncoder":
            from sklearn.preprocessing import LabelEncoder

            transformer = LabelEncoder(**kwargs)
        elif method == "Binarizer":
            from sklearn.preprocessing import Binarizer

            transformer = Binarizer(**kwargs)
        elif method == "KBins":
            from sklearn.preprocessing import KBinsDiscretizer

            transformer = KBinsDiscretizer(n_bins=bins, **kwargs)
        elif method == "BoxCox":
            from sklearn.preprocessing import PowerTransformer

            transformer = PowerTransformer(method="box-cox")
        elif method == "YeoJohnson":
            from sklearn.preprocessing import PowerTransformer

            transformer = PowerTransformer(method="yeo-johnson")
        elif method == "QuantileNormal":
            from sklearn.preprocessing import QuantileTransformer

            transformer = QuantileTransformer(
                output_distribution="normal",
                n_quantiles=bins,
                random_state=self.kwargs["random_state"],
            )
        elif method == "QuantileUniform":
            from sklearn.preprocessing import QuantileTransformer

            transformer = QuantileTransformer(
                output_distribution="uniform",
                n_quantiles=bins,
                random_state=self.kwargs["random_state"],
            )

        elif method == "Hashing":
            from category_encoders import HashingEncoder

            transformer = HashingEncoder(n_components=bins, **kwargs)
        elif method == "Ordinal":
            from category_encoders import OrdinalEncoder

            transformer = OrdinalEncoder(**kwargs)
        elif method == "OneHot":
            from category_encoders import OneHotEncoder

            transformer = OneHotEncoder(**kwargs)

        if transformer:
            self.transform_data(columns, "df", transformer, inplace=inplace, replace=replace)

        else:
            column = columns
            if method in ["cut", "qcut", "boxcox", "yeojohnson"] and len(columns) == 1:
                column = columns[0]
            if method == "cut":
                df = pd.cut(self.df[column], bins=bins, labels=np.arange(bins))
            elif method == "qcut":
                df = pd.qcut(self.df[column], q=bins, labels=np.arange(bins))
            elif method == "diff":
                df = self.df[column].diff(periods=bins)
            elif method == "shift":
                df = self.df[column].shift(periods=bins)
            elif method == "expanding_mean":
                df = self.df[column].expanding(min_periods=bins, center=False).mean()
            elif method == "rolling_mean":
                df = self.df[column].rolling(window=bins, min_periods=bins, center=False).mean()
            elif method == "rolling_std":
                df = self.df[column].rolling(window=bins, min_periods=bins, center=False).std()
            elif method == "resample":
                df = self.df[column].resample(rule=bins).mean()
            elif method == "log":
                df = self.df[column].apply(np.log)
            elif method == "boxcox":
                from scipy.stats import boxcox

                df = pd.DataFrame(boxcox(self.df[column])[0])
            elif method == "yeojohnson":
                from scipy.stats import yeojohnson

                df = pd.DataFrame(yeojohnson(self.df[column])[0])
            else:
                raise ValueError(f"Invalid method: {method}")

            df = convert_data(df, "dataframe")
            df.columns = [f"{c}_{method}" for c in df.columns]

            if inplace:
                if replace:
                    self.df.drop(columns=columns, inplace=True)
                self.df = pd.concat([self.df, df], axis=1)
            else:
                display(df)

    def column_best(self, method: str = "chi2"):
        """
        Best columns.

        .. seealso:: https://medium.com/swlh/feature-importance-hows-and-why-s-3678ede1e58f

        :param str method: Best method
        """
        self.logger.info("")
        self.logger.info(f"Best:      {method}")

        if method == "chi2":
            from sklearn.feature_selection import chi2

            score_func = chi2
        elif method == "f_classif":
            from sklearn.feature_selection import f_classif

            score_func = f_classif
        elif method == "mutual_info":
            from sklearn.feature_selection import mutual_info_classif

            score_func = mutual_info_classif
        elif method == "f_regression":
            from sklearn.feature_selection import f_regression

            score_func = f_regression
        elif method == "mutual_info_regression":
            from sklearn.feature_selection import mutual_info_regression

            score_func = mutual_info_regression
        else:
            raise ValueError(f"Invalid method: {method}")

        X, y = self.get_data(self.data_Xy)

        from sklearn.preprocessing import MinMaxScaler

        X = MinMaxScaler().fit_transform(X)

        from sklearn.feature_selection import SelectKBest

        select = SelectKBest(score_func, k="all").fit(X, y)

        pvalues = pd.Series(select.pvalues_, index=self.features)
        pvalues.sort_values(ascending=True, inplace=True)
        self.logger.info("")
        display(pvalues.sort_values(ascending=False).to_frame("p-values"))

        threshold = 0.05
        plt.figure()
        fig = pvalues.plot.barh(log=True)
        plt.axvline(x=threshold, linewidth=2, color="r")
        plt.title(f"SelectKBest {method}")
        plt.xlabel("p-value")
        plt.ylabel("Feature Name")
        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def column_resample(
        self,
        method: str,
        inplace: bool = False,
        replace: bool = False,
        **kwargs,
    ):
        """
        Resample label.

        :param str method: Resample method
        """
        self.logger.info("")
        self.logger.info(f"Sampler:   {method}")

        if method == "RUS":
            from imblearn.under_sampling import RandomUnderSampler

            sampler = RandomUnderSampler(random_state=self.kwargs["random_state"], **kwargs)
        elif method == "ROS":
            from imblearn.over_sampling import RandomOverSampler

            sampler = RandomOverSampler(random_state=self.kwargs["random_state"], **kwargs)
        elif method == "SMOTE":
            from imblearn.over_sampling import SMOTE

            sampler = SMOTE(
                k_neighbors=5, random_state=self.kwargs["random_state"], n_jobs=self.kwargs["n_jobs"], **kwargs
            )
        elif method == "SMOTEENN":
            from imblearn.combine import SMOTEENN

            sampler = SMOTEENN(random_state=self.kwargs["random_state"], n_jobs=self.kwargs["n_jobs"], **kwargs)
        else:
            raise ValueError(f"Invalid method: {method}")

        X, y = self.get_data("Xy")

        plt.figure()
        sns.countplot(x=y)
        X, y = sampler.fit_resample(X, y)
        plt.figure()
        sns.countplot(x=y)
        if inplace:
            self.X = X
            self.y = y
            self.merge_Xy()
        else:
            self.logger.info("")
            self.logger.info(f"Shape  {X.shape}")
            display(X)
            self.logger.info("")
            self.logger.info(f"Shape  {y.shape}")
            display(y)

    def plot_all(  # NOQA C901
        self,
        dry: bool = False,
        all: bool = False,
        column: str = None,
        category: bool = False,
        numeric: bool = False,
        string: bool = False,
        datetime: bool = False,
        nbins: int = 50,
        ntop: int = 20,
        logxy: bool = True,
    ):
        """
        Plot columns.

        :param bool dry: Dry mode
        :param str column: Column name
        :param bool category: Category columns
        :param bool numeric: Numeric columns
        :param bool string: String columns
        :param bool datetime: Datetime columns
        :param int nbins: Numer of bins
        :param int ntop: Top value_counts
        :param int logxy: Log x/y
        """
        X = self.get_data(self.data)

        if all:
            category = numeric = string = datetime = True

        if category or column is not None or dry:
            for c in self.get_columns("category"):
                if column is None:
                    self.logger.info(f"category - {c}")
                if not dry:
                    if column is not None and c != column:
                        continue
                    plt.figure()
                    X[c].value_counts(dropna=False).nlargest(ntop).plot.barh(logx=logxy)
                    plt.title(c)
                    plt.tight_layout()
        if numeric or column is not None or dry:
            for c in self.get_columns("numeric"):
                if column is None:
                    self.logger.info(f"numeric - {c}")
                if not dry:
                    if column is not None and c != column:
                        continue
                    plt.figure()
                    X[c].plot.hist(bins=nbins, logy=logxy)
                    plt.title(c)
                    plt.tight_layout()
        if string or column is not None or dry:
            for c in self.get_columns("object"):
                if column is None:
                    self.logger.info(f"object - {c}")
                if not dry:
                    if column is not None and c != column:
                        continue
                    plt.figure()
                    X[c].value_counts(dropna=False).nlargest(ntop).plot.barh(logx=logxy)
                    # for container in ax.containers:
                    #     ax.bar_label(container, fmt="  %d")
                    plt.title(c)
                    plt.tight_layout()
        if datetime or column is not None or dry:
            for c in self.get_columns("datetime"):
                if column is None:
                    self.logger.info(f"datetime - {c}")
                if not dry:
                    if column is not None and c != column:
                        continue
                    plt.figure()
                    X[c].value_counts(dropna=True).sort_index().plot()
                    plt.title(c)
                    plt.tight_layout()

    def plot_label_info(self):
        """
        Print label info.
        """
        if self.label is None:
            return

        self.logger.info("")
        self.logger.info("Label")
        self.logger.info(f"Name:     {self.label}")
        self.logger.info(f"Type:     {'Continuous' if self.is_continuous else 'Categorical'}")
        if not self.is_continuous:
            self.logger.info(f"Classes:  {len(self.classes)} {self.classes}")
        else:
            self.logger.info(f"Classes:  {len(self.classes)} {self.y.dtype}")
        self.logger.info(f"Mean:     {self.df[self.label].mean():.{self.options['precision']}f}")
        self.logger.info(f"Median:   {self.df[self.label].median():.{self.options['precision']}f}")
        self.logger.info(f"Std.Dev.: {self.df[self.label].std():.{self.options['precision']}f}")
        self.logger.info(f"Kurtosis: {self.df[self.label].kurt():.{self.options['precision']}f}")
        self.logger.info(f"Skewness: {self.df[self.label].skew():.{self.options['precision']}f}")

        if self.options["backend"] == "yellowbrick":
            from yellowbrick.target import class_balance

            y_train = self.get_data(self.data)[self.label]
            y_test = None

            plt.figure()
            class_balance(y_train, y_test, labels=self.classes)
            plt.tight_layout()
            plt.show()

            if self.is_split_tt or self.is_split_tvt:
                _, y_train = self.get_data("train")
                if self.is_split_tvt:
                    _, y_test = self.get_data("valid")
                if self.is_split_tt:
                    _, y_test = self.get_data("test")

                plt.figure()
                class_balance(y_train, y_test, labels=self.classes)
                plt.tight_layout()
                plt.show()
        else:
            self.plot_bar(c0=self.label)
            if self.is_continuous:
                self.plot_box(c0=self.label)

    def plot(
        self,
        c0: Union[int, str] = None,
        c1: Union[int, str] = None,
        by: str = None,
        kind: str = "line",
        subplots: bool = True,
    ):
        """
        Plot.

        :param Union[int, str] c0: Column name
        :param Union[int, str] c1: Column name
        :param str by: Column name to group-by
        :param str kind: Plot type (‘line’, ‘bar’, ‘barh’, ‘hist’, ‘box’, ‘kde’, ‘density’, ‘area’, ‘pie’, ‘scatter’, ‘hexbin’)
        :param bool subplots: Make subplots
        """
        X = self.get_data(self.data)
        c0, c1 = self.get_column([c0, c1])

        # plt.figure()
        fig = X.plot(x=c0, y=c1, by=by, kind=kind, subplots=subplots, sharex=False, sharey=False)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_bar(self, c0: Union[int, str], by: str = None, norm: bool = False):
        """
        Plot bar.

        :param Union[int, str] c0: Column name
        :param str by: Column name to group-by
        """
        X = self.get_data(self.data)
        c0 = self.get_column([c0])

        if self.options["backend"] == "plotly":
            df_stack = X.groupby([c0, by]).size().reset_index()
            df_stack["Percentage"] = (
                X.groupby([c0, by]).size().groupby(level=0).apply(lambda x: 100 * x / float(x.sum())).values
            )
            df_stack.columns = [c0, by, "Counts", "Percentage"]
            fig = px.bar(df_stack, y=c0, x="Percentage", color=by)
        else:
            plt.figure()
            if by:
                group = X.groupby(by)[c0].value_counts(dropna=False, normalize=norm).sort_index().unstack(0)
            else:
                group = X[c0].value_counts(dropna=False, normalize=norm).sort_index()
            fig = group.plot.barh(stacked=True)
            fig.set_xlabel("Percentage")
            fig.set_ylabel(c0)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_box(self, c0: Union[int, str] = None, c1: Union[int, str] = None, by: str = None, selection: str = None):
        """
        Plot box.

        :param Union[int, str] c0: Column name
        :param Union[int, str] c1: Column name
        :param str by: Column name to group-by
        :param str selection: Extra selection
        """
        X = self.get_data(self.data)
        if selection is not None:
            X = X[selection]
        if c0 is None:
            X = X.select_dtypes(exclude=["category", "object"])

        if self.options["backend"] == "plotly":
            fig = px.box(X, x=c0, y=c1, color=by, points="all")
        elif self.options["backend"] == "seaborn":
            plt.figure()
            fig = sns.boxplot(data=X, x=c0, y=c1, hue=by)
        else:
            plt.figure()
            # fig = pd.plotting.boxplot(X, column=c0, by=by)
            fig = X.boxplot(column=c0, by=by)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_cat(
        self,
        c0: Union[int, str] = None,
        c1: Union[int, str] = None,
        by: str = None,
        selection: str = None,
        kind: str = "point",
    ):
        """
        Plot cat.

        :param Union[int, str] c0: Column name
        :param Union[int, str] c1: Column name
        :param str by: Column name to group-by
        :param str selection: Extra selection
        :param str kind: Plot type ('point', 'bar', 'count', 'box', 'violin', 'boxen', 'strip', 'swarm')
        """
        X = self.get_data(self.data)
        if selection is not None:
            X = X[selection]
        if c0 is None:
            X = X.select_dtypes(exclude=["category", "object"])

        fig = sns.catplot(data=X, x=c0, y=c1, hue=by, kind=kind)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_density(self, c0: Union[int, str], c1: Union[int, str], by: str = None):
        """
        Plot density.

        :param Union[int, str] c0: Column name
        :param Union[int, str] c1: Column name
        :param str by: Column name to group-by
        """
        X = self.get_data(self.data)
        c0, c1 = self.get_column([c0, c1])

        if self.options["backend"] == "plotly":
            fig = px.density_heatmap(X, x=c0, y=c1, facet_col=by, histnorm="density")
        else:
            plt.figure()
            fig = plt.hist2d(x=X[c0], y=X[c1])

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_dist(self, c0: Union[int, str], by: str = None, kind: str = "hist"):
        """
        Plot distribution.

        :param Union[int, str] c0: Column name
        :param Union[int, str] by: Column name to group-by
        :param str kind: Plot type ('hist', 'kde', 'ecdf')
        """
        X = self.get_data(self.data)
        c0 = self.get_column([c0])

        if X.dtypes[c0] in ["category", "object"]:
            if self.options["backend"] == "plotly":
                fig = px.histogram(X, x=c0, color=by)
            elif self.options["backend"] == "seaborn":
                plt.figure()
                fig = sns.countplot(data=X, x=c0, hue=by, orient="h")
            else:
                plt.figure()
                if by is None:
                    fig = X[c0].value_counts().sort_index().plot.barh()
                else:
                    fig = X.groupby(by)[c0].value_counts().sort_index().unstack(0).plot.barh()
                fig.set_ylabel(c0)
        else:
            if self.options["backend"] == "plotly":
                fig = px.histogram(X, x=c0, color=by, marginal="box")
            elif self.options["backend"] == "seaborn":
                fig = sns.displot(data=X, x=c0, hue=by, kind=kind, rug=False)
            else:
                plt.figure()
                fig = X[c0].plot.hist(by=by)
                # fig = X.hist(column=c0, by=by)
                # fig = X.groupby(by)[c0].plot.hist()
                fig.set_xlabel(c0)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_heatmap(self, vmin=None, vmax=None, size=8, title: str = ""):
        """
        Plot heatmap.
        """
        X = self.get_data(self.data)

        plt.figure(figsize=(size, size))
        fig = sns.heatmap(X, vmin=vmin, vmax=vmax, cmap="coolwarm", linewidths=0.5, annot=True)
        plt.title(title)
        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_joint(self, c0: Union[int, str], c1: Union[int, str], by: str = None, kind: str = "kde"):
        """
        Plot joint.

        :param Union[int, str] c0: Column name
        :param Union[int, str] c1: Column name
        :param str by: Column name to group-by
        :param str kind: Plot type ('scatter', 'kde', 'hist', 'hex', 'reg', 'resid')
        """
        X = self.get_data(self.data)
        c0, c1 = self.get_column([c0, c1])

        plt.figure()
        fig = sns.jointplot(data=X, x=c0, y=c1, hue=by, kind=kind)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_matrix(self, columns: list = None, by: str = None, kind: str = "hist"):
        """
        Plot matrix.

        :param columns: Column list
        :param str by: Column name to group-by
        :param str kind: Plot type ('scatter', 'kde', 'hist', 'reg')
        """
        X = self.get_data(self.data)
        if columns is not None:
            X = X[columns]
        else:
            X = X.select_dtypes(exclude=["category", "object"])

        if self.options["backend"] == "plotly":
            fig = px.scatter_matrix(data_frame=X, dimensions=columns, color=by)
        elif self.options["backend"] == "seaborn":
            fig = sns.pairplot(data=X, vars=columns, diag_kind=kind, hue=by, corner=True, height=2)
        else:
            fig = pd.plotting.scatter_matrix(X, diagonal=kind)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_parallel(self, kind: str = "coord"):
        """
        Plot parallel.

        :param str kind: kind ('coord', 'cat')
        """
        X, y = self.get_data(self.data_Xy)

        columns = self.columns
        if kind == "coord":
            columns = self.get_columns("numeric")
        elif kind == "cat":
            columns = self.get_columns("object")

        if self.options["backend"] == "plotly":
            if kind == "coord":
                fig = px.parallel_coordinates(X, dimensions=columns, color="stroke")
            elif kind == "cat":
                fig = px.parallel_categories(X, dimensions=columns, color="stroke")
            else:
                fig = None
        elif self.options["backend"] == "yellowbrick":
            from yellowbrick.features import parallel_coordinates

            plt.figure()
            fig = parallel_coordinates(
                X,
                y,
                features=columns,
                classes=self.classes,
                random_state=self.kwargs["random_state"],
                fast=True,
            )
        else:
            fig = pd.plotting.parallel_coordinates(X, class_column=columns)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_pca(self, n_components=2, proj_features=True, heatmap=True):
        """
        Plot pca.
        """
        X, y = self.get_data(self.data_Xy)

        from yellowbrick.features import pca_decomposition

        plt.figure(figsize=(8, 8) if heatmap else None)
        fig = pca_decomposition(
            X,
            y,
            projection=n_components,
            scale=True,
            proj_features=proj_features,
            heatmap=heatmap,
            colorbar=True,
            classes=self.classes,
            random_state=self.kwargs["random_state"],
        )
        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_manifold(self, n_components: int = 2, manifold: str = "mds"):
        """
        Plot manifold.

        :param str manifold: kind ('lle', 'ltsa', 'hessian', 'modified', 'isomap', 'mds', 'spectral', 'tsne')
        """
        X, y = self.get_data(self.data_Xy)

        from yellowbrick.features import manifold_embedding

        plt.figure()
        fig = manifold_embedding(
            X,
            y,
            projection=n_components,
            manifold=manifold,
            n_neighbors=5,
            classes=self.classes,
            random_state=self.kwargs["random_state"],
        )

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_qq(self, c0):
        """
        Plot Q-Q.
        """
        X, _ = self.get_data(self.data_Xy)
        c0 = self.get_column(c0)

        from statsmodels.graphics.gofplots import qqplot

        # plt.figure()
        fig = qqplot(X[c0], line="q")
        plt.title(c0)
        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_radviz(self):
        """
        Plot radviz.
        """
        X, y = self.get_data(self.data_Xy)
        if isinstance(y, pd.Series):
            y = y.to_numpy()

        if self.options["backend"] == "yellowbrick":
            from yellowbrick.features.radviz import radviz

            plt.figure()
            fig = radviz(X, y, classes=self.classes)
        else:
            fig = pd.plotting.radviz(X, class_column=X.columns)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_ranking(self):
        """
        Plot ranking.
        """
        X, _ = self.get_data(self.data_Xy)
        _, ax = plt.subplots(ncols=2, figsize=(8, 4))
        from yellowbrick.features import rank1d, rank2d

        rank1d(X, ax=ax[0], show=False)
        rank2d(X, ax=ax[1], show=False)
        plt.tight_layout()
        plt.show()

    def plot_scatter(
        self, c0: Union[int, str], c1: Union[int, str], by: str = None, alpha: float = 0.5, y_predicted=None
    ):
        """
        Plot scatter.

        :param Union[int, str] c0: Column name
        :param Union[int, str] c1: Column name
        :param str by: Column name to group-by
        """
        X = self.get_data(self.data)
        y = None
        if y_predicted is not None:
            X, _ = self.get_data(self.data_Xy)
            y = y_predicted
        c0, c1 = self.get_column([c0, c1])

        if self.options["backend"] == "matplotlib":
            plt.figure()
            by = X[by] if by is not None else None
            fig = plt.scatter(x=X[c0], y=X[c1], c=by, alpha=alpha)
        elif self.options["backend"] == "plotly":
            fig = px.scatter(X, x=c0, y=c1, color=by, marginal_x="box", marginal_y="box")
        elif self.options["backend"] == "seaborn":
            plt.figure()
            fig = sns.scatterplot(data=X, x=c0, y=c1, hue=by, alpha=alpha)
        elif self.options["backend"] == "yellowbrick" and y is not None:
            features_2d = [c0, c1]
            X = X[features_2d]
            X = convert_data(X, "array")
            y = convert_data(y, "array")

            from yellowbrick.contrib import ScatterVisualizer

            plt.figure()
            fig = ScatterVisualizer(features=features_2d, classes=self.classes, alpha=alpha)
            fig.fit(X, y)
            fig.transform(X)
        else:
            plt.figure()
            fig = X.plot.scatter(x=c0, y=c1)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_sunburst(self, by=None, path=None):
        """
        Plot sunburst.
        """
        X, _ = self.get_data(self.data_Xy)
        if by == "label":
            by = self.label
        if path is None:
            path = self.get_types("object")

        fig = px.sunburst(X, path=path, color=by)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_ternary(self, c0: Union[int, str], c1: Union[int, str], c2: Union[int, str], by: str = None):
        """
        Plot ternary.

        :param Union[int, str] c0: Column name
        :param Union[int, str] c1: Column name
        :param Union[int, str] c2: Column name
        :param str by: Column name to group-by
        """
        X = self.get_data(self.data)

        c0, c1, c2 = self.get_column([c0, c1, c2])

        fig = px.scatter_ternary(X, a=c0, b=c1, c=c2, color=by)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()

    def plot_treemap(self, by=None, path=None):
        """
        Plot treemap.
        """
        X, _ = self.get_data(self.data_Xy)
        if by == "label":
            by = self.label
        if path is None:
            path = self.get_types("object")

        fig = px.treemap(X, path=path, color=by)

        if hasattr(fig, "show"):
            fig.show()
        plt.tight_layout()
