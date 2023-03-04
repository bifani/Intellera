from src.datafactory import DataFactory

from IPython.display import display

from typing import Union

import pandas as pd
import numpy as np

BASE_FOLDER_ENR = "../data/AMA_Progetto_Fabbisogni/EmessoNonRiscosso"
SQLITE_DB = "TARI_19_Feb_23.db"


class ENR_DOM_CONTRATTI_ATTIVI(DataFactory):
    """
    Utenze Domestiche - Contratti Attivi.
    """

    def __init__(self, **kwargs):
        dtype = {
            "CNT_COD": "string",
            "CNT_TCN_COD": "string",
            "CNT_COGNOME": "string",
            "CNT_NOME": "string",
            "CNT_RAG_SOC": "string",
            "CNT_PAR_IVA": "string",
            "CNT_COD_FSC": "string",
            "RCP_NOMINATIVO": "string",
            "RCP_VIA_DES": "string",
            "RCP_NUM_CIV": "string",
            "RCP_ESP_CIV": "string",
            "RCP_CAP": "string",
            "RCP_CMN_DES": "string",
            "RCP_DES_LOC": "string",
            "RCP_SGL_PRV": "string",
            "RCP_SGL_NAZ": "string",
            "UTZ_CONTRATTO": "string",
            "UTZ_TARIFTYP": "string",
            "UTZ_VIA_COD": "string",
            "UTZ_VIA_DES": "string",
            "UTZ_NUM_CIV": "string",
            "UTZ_ESP_CIV": "string",
            "UTZ_EDF": "string",
            "UTZ_SCA": "string",
            "UTZ_PIA": "string",
            "UTZ_NUM_INT": "string",
            "UTZ_IMM_FOG": "string",
            "UTZ_IMM_PAR": "string",
            "UTZ_IMM_SUB": "string",
            "UTZ_BOX_FOG": "string",
            "UTZ_BOX_PAR": "string",
            "UTZ_BOX_SUB": "string",
            "UTZ_BOX2_FOG": "string",
            "UTZ_BOX2_PAR": "string",
            "UTZ_BOX2_SUB": "string",
            "UTZ_CANT_FOG": "string",
            "UTZ_CANT_PAR": "string",
            "UTZ_CANT_SUB": "string",
            "UTZ_BLOCCO_CNT": "string",
            "UTZ_BLOCCO_CA": "string",
            "SUPERFICIE": "string",
            "COMPONENTI": "string",
        }
        self.parse_dates = {
            "UTZ_DAT_ATT": "%Y%m%d",
            "UTZ_DAT_CESS": "%Y%m%d",
            "UTZ_DAT_CREAZ": "%Y%m%d",
            "UTZ_BLOCCO_CA_DATA": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | DOM_Contratti_Attivi_20220929",
            keys=["CNT_COD", "UTZ_CONTRATTO", "CNT_COD_FSC"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.drop_columns(
            columns=[
                "RCP_CAP",
                "RCP_CMN_DES",
                "RCP_DES_LOC",
                "RCP_ESP_CIV",
                "RCP_NOMINATIVO",
                "RCP_NUM_CIV",
                "RCP_SGL_NAZ",
                "RCP_SGL_PRV",
                "RCP_VIA_DES",
                "UTZ_BLOCCO_CA",
                "UTZ_BLOCCO_CA_DATA",
                "UTZ_BLOCCO_CNT",
                "UTZ_BOX2_FOG",
                "UTZ_BOX2_PAR",
                "UTZ_BOX2_SUB",
                "UTZ_BOX_FOG",
                "UTZ_BOX_PAR",
                "UTZ_BOX_SUB",
                "UTZ_CANT_FOG",
                "UTZ_CANT_PAR",
                "UTZ_CANT_SUB",
                "UTZ_DAT_ATT",
                "UTZ_DAT_CESS",
                "UTZ_DAT_CREAZ",
                "UTZ_EDF",
                "UTZ_ESP_CIV",
                "UTZ_IMM_FOG",
                "UTZ_IMM_PAR",
                "UTZ_IMM_SUB",
                "UTZ_NUM_INT",
                "UTZ_PIA",
                "UTZ_SCA",
                "UTZ_VIA_COD",
            ],
        )

        self.df["UTZ_NUM_CIV"] = self.df["UTZ_NUM_CIV"].str.replace("SN", "0")
        self.set_types(["UTZ_NUM_CIV"], "int")

        self.set_types(["SUPERFICIE", "COMPONENTI"], "float64")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(toup="UTZ_VIA_DES")
        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_DOM_AVVISI_CONTRATTI_ATTIVI(DataFactory):
    """
    Utenze Domestiche - Avvisi Contratti Attivi.
    """

    def __init__(self, **kwargs):
        dtype = {
            "CNT_COD": "string",
            "CONTO": "string",
            "NUM_FATT": "string",
            "UTZ_CONTRATTO": "string",
            "IMPORTO_CONTRATTO": "string",
            "MOTIVO_PAREGGIO": "string",
            "IMPORTO_PAREGGIO": "string",
            "TIPO_DOC_PAREGGIO": "string",
        }
        self.parse_dates = {"DATA_EMISSIONE": "%Y%m%d", "DATA_PAREGGIO": "%Y%m%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | DOM_Avvisi_Contratti_Attivi_20230131_tdoc",
            keys=["CNT_COD", "UTZ_CONTRATTO", "NUM_FATT"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.drop_columns(
            columns=[
                "CONTO",
                "DATA_EMISSIONE",
            ],
        )

        self.set_types(["IMPORTO_CONTRATTO", "IMPORTO_PAREGGIO"], "numeric c2p")

        self.set_types(["NUM_FATT", "MOTIVO_PAREGGIO"], "string d0")

        self.df["MOTIVO_PAREGGIO"].replace(
            {
                "01": "01 - Pagamento",
                "05": "05 - Storno",
                "08": "08 - Compensazione",
                "11": "11 - Reset Pareggio",
            },
            inplace=True,
        )
        self.df["TIPO_DOC_PAREGGIO"].replace(
            {
                "BC": "BC - Incasso Cassa",
                "PA": "PA - Incasso PagoPA",
                "PM": "PM - Incasso PagoPA MLT",
                "PC": "PC - Pareggio TARI RC",
                "PI": "PI - Pareggio TARI F24",
                "ST": "ST - Storno Fatture",
            },
            inplace=True,
        )
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply(
            "NUM_FATT.str.startswith('1122') & (DATA_PAREGGIO >= '2022-06-01' | DATA_PAREGGIO.isnull())", inplace=True
        )

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_DOM_RECAPITI(DataFactory):
    """
    Utenze Domestiche - Recapiti.
    """

    def __init__(self, **kwargs):
        dtype = {
            "BP": "string",
            "CONTO": "string",
            "CONTRATTO": "string",
            "NUM_FATT": "string",
            "RCP_NOMINATIVO": "string",
            "RCP_VIA_DES": "string",
            "RCP_NUM_CIV": "string",
            "RCP_ESP_CIV": "string",
            "RCP_CAP": "string",
            "RCP_CMN_DES": "string",
            "RCP_SGL_PRV": "string",
            "RCP_SGL_NAZ": "string",
        }
        self.parse_dates = {"DATA_EMISSIONE": "%Y%m%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | 2_recapiti_emissione_domestici_2022",
            keys=["BP", "CONTRATTO", "NUM_FATT"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["CONTRATTO", "NUM_FATT"], "string d0")

        self.df["FLAG_RECAPITO"] = 1
        self.set_types(["FLAG_RECAPITO"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply("NUM_FATT.str.startswith('1122') & RCP_SGL_NAZ == 'IT'", inplace=True)

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")

        cols = [
            "RCP_CAP",
            "RCP_CMN_DES",
            # "RCP_ESP_CIV",
            # "RCP_NOMINATIVO",
            "RCP_NUM_CIV",
            "RCP_SGL_NAZ",
            "RCP_SGL_PRV",
            "RCP_VIA_DES",
        ]
        self.clean_data(toup=cols)

        geo = ENR_COMUNI(pre_process=True, process=True, post_process=True, silent=True)
        self.df.loc[~self.df["RCP_CMN_DES"].isin(geo.comuni()), "FLAG_RECAPITO"] = -1
        self.df.loc[~self.df["RCP_SGL_PRV"].isin(geo.sigle_provincie()), "FLAG_RECAPITO"] = -2
        del geo

        for c in cols:
            self.df.loc[self.df[c].isnull(), "FLAG_RECAPITO"] = 0
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_NDOM_CONTRATTI_ATTIVI(DataFactory):
    """
    Utenze Non Domestiche - Contratti Attivi.
    """

    def __init__(self, **kwargs):
        dtype = {
            "CNT_COD": "string",
            "CNT_TCN_COD": "string",
            "CNT_COGNOME": "string",
            "CNT_NOME": "string",
            "CNT_RAG_SOC": "string",
            "CNT_PAR_IVA": "string",
            "CNT_COD_FSC": "string",
            "UTZ_CONTRATTO": "string",
            "UTZ_TARIFTYP": "string",
            "UTZ_VIA_COD": "string",
            "UTZ_VIA_DES": "string",
            "UTZ_NUM_CIV": "string",
            "UTZ_ESP_CIV": "string",
            "UTZ_EDF": "string",
            "UTZ_SCA": "string",
            "UTZ_PIA": "string",
            "UTZ_NUM_INT": "string",
            "UTZ_IMM_FOG": "string",
            "UTZ_IMM_PAR": "string",
            "UTZ_IMM_SUB": "string",
            "SUPERFICIE": "string",
        }
        self.parse_dates = {"UTZ_DAT_ATT": "%Y%m%d", "UTZ_DAT_CESS": "%Y%m%d", "UTZ_DAT_CREAZ": "%Y%m%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | NdomConParticelle",
            keys=["CNT_COD", "UTZ_CONTRATTO", "CNT_COD_FSC"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.drop_columns(
            columns=[
                "UTZ_DAT_ATT",
                "UTZ_DAT_CESS",
                "UTZ_DAT_CREAZ",
                "UTZ_EDF",
                "UTZ_ESP_CIV",
                "UTZ_IMM_FOG",
                "UTZ_IMM_PAR",
                "UTZ_IMM_SUB",
                "UTZ_NUM_INT",
                "UTZ_PIA",
                "UTZ_SCA",
                "UTZ_VIA_COD",
            ],
        )

        self.df["UTZ_NUM_CIV"] = self.df["UTZ_NUM_CIV"].str.replace("SN", "0")
        self.set_types(["UTZ_NUM_CIV"], "int")

        self.set_types(["SUPERFICIE"], "float64")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(toup="UTZ_VIA_DES")
        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_NDOM_AVVISI_CONTRATTI_ATTIVI(DataFactory):
    """
    Utenze Non Domestiche - Avvisi Contratti Attivi.
    """

    def __init__(self, **kwargs):
        dtype = {
            "CNT_COD": "string",
            "CONTO": "string",
            "NUM_FATT": "string",
            "UTZ_CONTRATTO": "string",
            "IMPORTO_CONTRATTO": "string",
            "MOTIVO_PAREGGIO": "string",
            "IMPORTO_PAREGGIO": "string",
            "TIPO_DOC_PAREGGIO": "string",
        }
        self.parse_dates = {"DATA_EMISSIONE": "%Y%m%d", "DATA_PAREGGIO": "%Y%m%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | NDOM_Avvisi_Contratti_Attivi_20230131_tdoc",
            keys=["CNT_COD", "UTZ_CONTRATTO", "NUM_FATT"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.drop_columns(
            columns=[
                "CONTO",
                "DATA_EMISSIONE",
            ],
        )

        self.set_types(["IMPORTO_CONTRATTO", "IMPORTO_PAREGGIO"], "numeric c2p")

        self.set_types(["NUM_FATT", "MOTIVO_PAREGGIO"], "string d0")

        self.df["MOTIVO_PAREGGIO"].replace(
            {
                "01": "01 - Pagamento",
                "05": "05 - Storno",
                "08": "08 - Compensazione",
                "11": "11 - Reset Pareggio",
            },
            inplace=True,
        )
        self.df["TIPO_DOC_PAREGGIO"].replace(
            {
                "BC": "BC - Incasso Cassa",
                "PA": "PA - Incasso PagoPA",
                "PM": "PM - Incasso PagoPA MLT",
                "PC": "PC - Pareggio TARI RC",
                "PI": "PI - Pareggio TARI F24",
                "ST": "ST - Storno Fatture",
            },
            inplace=True,
        )
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply(
            "NUM_FATT.str.startswith('1122') & (DATA_PAREGGIO >= '2022-06-01' | DATA_PAREGGIO.isnull())", inplace=True
        )

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_NDOM_PEC_SAP(DataFactory):
    """
    Utenze Non Domestiche - Senza PEC.
    """

    def __init__(self, **kwargs):
        dtype = {
            "CNT_COD": "string",
            "CNT_TCN_COD": "string",
            "CNT_COGNOME": "string",
            "CNT_NOME": "string",
            "CNT_RAG_SOC": "string",
            "CNT_PAR_IVA": "string",
            "CNT_COD_FSC": "string",
            "CNT_PEC": "string",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | CNT_NDOM_SENZA_PEC",
            keys=["CNT_COD"],
            dtype=dtype,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["CNT_PAR_IVA", "CNT_COD_FSC"], "string d0")

        self.df.rename(columns={"CNT_PEC": "FLAG_PEC_SAP"}, inplace=True)

        self.set_types(["FLAG_PEC_SAP"], "int")
        self.df["FLAG_PEC_SAP"] = 0
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_NDOM_PEC_INVII(DataFactory):
    """
    Utenze Non Domestiche - Esiti PEC.
    """

    def __init__(self, **kwargs):
        dtype = {
            "NUMERO_FATTURA": "string",
            "DESTINATARIO": "string",
            "CAP": "string",
            "LOCALITA": "string",
            "INDIRIZZO": "string",
            "CODICE_ESITO": "string",
            "CODICE_MOTIVO": "string",
            "FLAG_FONTE_ESITO": "string",
            "DESCRIZIONE_ERRORE_SEIPEC": "string",
            "SEMESTRE": "string",
        }
        self.parse_dates = {
            "DATA_NOTIFICA": "%Y%m%d",
            "DATA_ELABORA_ESITO": "%Y%m%d",
            "DATA_EMISSIONE_FATTURA": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | Esiti PEC 2022",
            keys=["NUMERO_FATTURA"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["CAP"], "string d0")

        self.df["FLAG_PEC_INV"] = 1
        self.df.loc[self.df["DESCRIZIONE_ERRORE_SEIPEC"].notnull(), "FLAG_PEC_INV"] = 2
        self.set_types(["FLAG_PEC_INV"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        # self.query_apply("DESCRIZIONE_ERRORE_SEIPEC.isnull()", inplace=True)

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_UTENZE(DataFactory):
    """
    Utenze - Base Class.
    """

    def __init__(self, **kwargs):
        super().__init__(
            **kwargs,
        )
        self.add_extra_columns()

        self.query_apply("MOTIVO_PAREGGIO != '05 - Storno' & MOTIVO_PAREGGIO != '08 - Compensazione'", inplace=True)
        self.query_apply("TIPO_DOC_PAREGGIO != 'ST - Storno Fatture'", inplace=True)

    def add_extra_columns(self):
        """
        Add extra columns.
        """
        if self.columns is not None and len(self.columns) != 0:
            self.logger.info("")
            self.logger.info("Add extra columns")

            if "FLAG_RECAPITO" in self.columns:
                self.df["FLAG_NON_RECAPITATO"] = self.df["FLAG_RECAPITO"] == 0
            if "FLAG_NOPEC" in self.columns:
                self.df["FLAG_NON_RECAPITATO"] = self.df["FLAG_NOPEC"] == 1
            if "FLAG_DECEDUTO" in self.columns and "FLAG_CESSATA" in self.columns:
                self.df["FLAG_DATI_ERRATI"] = (self.df["FLAG_DECEDUTO"] == 1) | (self.df["FLAG_CESSATA"] == 1)

            if (
                "FLAG_NON_RECAPITATO" in self.columns
                and "FLAG_DATI_ERRATI" in self.columns
                and "FLAG_RATE" in self.columns
                and "FLAG_ESENTE" in self.columns
            ):
                self.df["FLAG_GIUSTIFICATO"] = (
                    (self.df["FLAG_NON_RECAPITATO"] == 1)
                    | (self.df["FLAG_DATI_ERRATI"] == 1)
                    | (self.df["FLAG_RATE"] == 1)
                    | (self.df["FLAG_ESENTE"] == 1)
                )

                if "FLAG_BOLLETTINO_WEB" in self.columns and "FLAG_BOLLETTINO_CC" in self.columns:
                    self.df["FLAG_NOTIFICATO"] = (
                        (self.df["FLAG_NON_RECAPITATO"] == 1)
                        | (self.df["FLAG_DATI_ERRATI"] == 1)
                        | (self.df["FLAG_RATE"] == 1)
                        | (self.df["FLAG_ESENTE"] == 1)
                    ) & ((self.df["FLAG_BOLLETTINO_WEB"] == 1) | (self.df["FLAG_BOLLETTINO_CC"] == 1))

            # if "IMPORTO_CONTRATTO" in self.columns:
            #     self.df["IMPORTO_CONTRATTO_NO_TEFA"] = self.df["IMPORTO_CONTRATTO"] * 100 / 105
            # if "IMPORTO_PAREGGIO" in self.columns:
            #     self.df["IMPORTO_PAREGGIO_NO_TEFA"] = self.df["IMPORTO_PAREGGIO"] * 100 / 105

            if "IMPORTO_CONTRATTO" in self.columns and "IMPORTO_PAREGGIO" in self.columns:
                self.df["PERCENTUALE_NON_PAGATO"] = (
                    self.df["IMPORTO_CONTRATTO"] - self.df["IMPORTO_PAREGGIO"]
                ) / self.df["IMPORTO_CONTRATTO"]
                self.df["PERCENTUALE_NON_PAGATO"].fillna(0, inplace=True)
                self.df["FLAG_PAGATO"] = 0
                self.df.loc[self.df["IMPORTO_CONTRATTO"] == self.df["IMPORTO_PAREGGIO"], "FLAG_PAGATO"] = 1
                self.df.loc[self.df["IMPORTO_CONTRATTO"] == 0, "FLAG_PAGATO"] = 0

            # if "TIPO_DOC_PAREGGIO" in self.columns:
            #     self.df["TIPO_DOC_PAREGGIO"].fillna("NA", inplace=True)
            # if "MOTIVO_PAREGGIO" in self.columns:
            #     self.df["MOTIVO_PAREGGIO"].fillna("NA", inplace=True)
            # if "NUM_CIR" in self.columns:
            #     self.df["NUM_CIR"].fillna("NA", inplace=True)
            # if "COD_ZON_URB" in self.columns:
            #     self.df["COD_ZON_URB"].fillna("NA", inplace=True)
            # if "CAP" in self.columns:
            #     self.df["CAP"].fillna("NA", inplace=True)

            self.update()
            self.check_data()

    def print_importi(
        self,
        columns: Union[str, list],
        query: str = None,
        freq: str = "M",
        bins: int = None,
    ):
        """
        Print importi.

        :param Union[str, list] columns: Columns to group-by
        :param str query: Filter query
        :param str freq: datetime group-by frequency
        :param int bins: numeric group-by bins
        """
        if not isinstance(columns, list):
            columns = [columns]

        func = {
            "CNT_COD": ["count", pd.Series.nunique],
            "UTZ_CONTRATTO": [pd.Series.nunique],
            "CNT_COD_FSC": [pd.Series.nunique],
            "IMPORTO_CONTRATTO": ["sum"],
            "IMPORTO_PAREGGIO": ["sum"],
        }
        if "IMPORTO_PAGAMENTO" in self.columns:
            func["IMPORTO_PAGAMENTO"] = ["sum"]

        cols = columns.copy()
        cols.extend(func.keys())

        grouper = list()
        for c in columns:
            if "DATA" in c:
                grouper.append(pd.Grouper(key=c, dropna=False, freq=freq))
            elif bins and "SUPERFICIE" in c:
                grouper.append(pd.cut(self.df[c], bins, right=False))
            else:
                grouper.append(pd.Grouper(key=c, dropna=False))

        df = self.df.query(query) if query else self.df
        df = df[cols].groupby(grouper, dropna=False)

        df = df.agg(func=func)

        # df[("IMPORTO_DELTA", "sum")] = df[("IMPORTO_CONTRATTO", "sum")] - df[("IMPORTO_PAREGGIO", "sum")]
        # df[("IMPORTO_DELTA_PERC", "sum")] = df[("IMPORTO_DELTA", "sum")] / df[("IMPORTO_CONTRATTO", "sum")] * 100

        with pd.option_context("display.max_rows", None):
            display(df)

        del df

    def print_importi_flags(
        self,
        columns: Union[str, list],
        query: str = None,
        flags: Union[str, list] = None,
    ):
        """
        Print importi split by flag.

        :param Union[str, list] columns: Columns to group-by
        :param str query: Filter query
        :param Union[str, list] columns: Flags
        """
        if not flags:
            flags = sorted([c for c in self.columns if c.startswith("FLAG_")])
        self.logger.info("")
        self.logger.info(f"Flags: {len(flags)} - {flags})")
        self.logger.info("")
        for f in flags:
            cols = columns.copy()
            cols.extend([f])
            self.print_importi(columns=cols, query=query)

    def print_flags(self):
        """
        Print flags.
        """
        flags = sorted([c for c in self.columns if c.startswith("FLAG_")])
        self.logger.info("")
        self.logger.info(f"Flags: {len(flags)} - {flags})")
        self.logger.info("")
        for f in flags:
            self.print_counts(f)

    def print_crosstab(
        self,
        columns: list = None,
        query: str = None,
        split: int = 2,
    ):
        """
        Print crosstab.

        :param list columns: Columns
        :param str query: Filter query
        :param int split: Split
        """
        if not columns:
            columns = sorted([c for c in self.columns if c.startswith("FLAG_")])

        split = 1 if len(columns) == 2 else split
        self.logger.info("")
        self.logger.info(f"Columns: {len(columns)} - {columns})")
        self.logger.info("")
        self.cross_table(columns[:split], columns[split:], query=query)

    def print_municipi(
        self,
        query: str = None,
    ):
        """
        Print municipi.

        :param str query: Filter query
        """
        self.df["IMPORTO_PAREGGIO"].replace(0, np.nan, inplace=True)

        df = self.df.query(query) if query else self.df
        func = {
            "IMPORTO_CONTRATTO": ["count", "sum", "median", "mean", "std", "min", "max"],
            "IMPORTO_PAREGGIO": ["count", "sum", "median", "mean", "std", "min", "max"],
            # "MOTIVO_PAREGGIO": ["count"],
        }
        df = df.groupby("NUM_CIR").agg(func)

        df[("count", "P/C")] = df[("IMPORTO_PAREGGIO", "count")] / df[("IMPORTO_CONTRATTO", "count")]
        df[("count", "P")] = df[("IMPORTO_PAREGGIO", "count")] / df[("IMPORTO_PAREGGIO", "count")].sum()
        df[("count", "C")] = df[("IMPORTO_CONTRATTO", "count")] / df[("IMPORTO_CONTRATTO", "count")].sum()
        # df[("count", "P / C")] = df[("count", "P")] / df[("count", "C")]

        df[("sum", "P/C")] = df[("IMPORTO_PAREGGIO", "sum")] / df[("IMPORTO_CONTRATTO", "sum")]
        df[("sum", "P")] = df[("IMPORTO_PAREGGIO", "sum")] / df[("IMPORTO_PAREGGIO", "sum")].sum()
        df[("sum", "C")] = df[("IMPORTO_CONTRATTO", "sum")] / df[("IMPORTO_CONTRATTO", "sum")].sum()
        # df[("sum", "P / C")] = df[("sum", "P")] / df[("sum", "C")]

        self.df["IMPORTO_PAREGGIO"].fillna(0, inplace=True)

        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            display(df)

        del df

    def merge_GE_VIE(self):
        """
        Merge ENR_GE_VIE_DB.
        """
        try:
            self.backup()
            self.drop_columns(["COD_VIA", "DSC_VIA", "LIM_CIV_DA", "LIM_CIV_A", "CAP", "COD_ZON_URB", "NUM_CIR"])
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_GE_VIE(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )

            from thefuzz import fuzz
            from thefuzz import process

            self.logger.info("")
            self.logger.info("thefuzz")
            UTZ_VIA_DES = self.df["UTZ_VIA_DES"].value_counts().index.to_list()
            DSC_VIA = df.df["DSC_VIA"].value_counts().index.to_list()
            UTZ_VIA_DES_UC = sorted(list(set(UTZ_VIA_DES) - set(DSC_VIA)))
            self.logger.info(f"Uncommon:  {len(UTZ_VIA_DES_UC)}")
            rep = dict()
            for v in UTZ_VIA_DES_UC:
                eo = process.extractOne(v, DSC_VIA)
                if eo[1] > 90:
                    rep[v] = eo[0]
            self.logger.info(f"Recovered: {len(rep)}")
            self.df["UTZ_VIA_DES"].replace(rep, inplace=True)
            UTZ_VIA_DES = self.df["UTZ_VIA_DES"].value_counts().index.to_list()
            DSC_VIA = df.df["DSC_VIA"].value_counts().index.to_list()
            UTZ_VIA_DES_UC = sorted(list(set(UTZ_VIA_DES) - set(DSC_VIA)))
            self.logger.info(f"Missing:   {len(UTZ_VIA_DES_UC)} - {UTZ_VIA_DES_UC}")

            df.set_options(silent=self.silent)
            df.merge_GE_TERRIT()
            self.logger.info("")
            self.logger.info("Merge")
            # LEFT JOIN
            # self.df = self.merge(
            #     df.df,
            #     lcolumns=["UTZ_VIA_DES"],
            #     rcolumns=["DSC_VIA"],
            #     how="left",
            #     datafactory=False,
            #     silent=self.silent,
            # )
            # RANGE JOIN
            # query = "SELECT * FROM df0 LEFT JOIN df1 ON df0.UTZ_VIA_DES = df1.DSC_VIA AND df0.DSC_VIA BETWEEN df1.LIM_CIV_DA and df1.LIM_CIV_A"
            query = "SELECT * FROM df0 LEFT JOIN df1 ON df0.UTZ_VIA_DES = df1.DSC_VIA AND df0.UTZ_NUM_CIV >= df1.LIM_CIV_DA and df0.UTZ_NUM_CIV < df1.LIM_CIV_A"
            self.df = self.sql_query(
                df.df,
                query,
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.drop_columns(["COD_VIA", "DSC_VIA", "LIM_CIV_DA", "LIM_CIV_A"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_DECESSI(self):
        """
        Merge ENR_DECESSI_DB.
        """
        try:
            self.backup()
            self.drop_columns(["COD_FISCALE", "FLAG_DECEDUTO"])
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_DECESSI(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["COD_FISCALE", "FLAG_DECEDUTO"]],
                lcolumns=["CNT_COD_FSC"],
                rcolumns=["COD_FISCALE"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_DECEDUTO"].fillna(0, inplace=True)
            self.drop_columns(["COD_FISCALE"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_CESSAZIONI(self):
        """
        Merge ENR_CESSAZIONI_DB.
        """
        try:
            self.backup()
            self.drop_columns(["COD_FISCALE", "FLAG_CESSATA", "RI_CLEAN_STA_PAR_IVA"])
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_CESSAZIONI(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["COD_FISCALE", "FLAG_CESSATA", "RI_CLEAN_STA_PAR_IVA"]],
                lcolumns=["CNT_COD_FSC"],
                rcolumns=["COD_FISCALE"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_CESSATA"].fillna(0, inplace=True)
            self.drop_columns(["COD_FISCALE"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_ESENZIONI(self):
        """
        Merge ENR_ESENZIONI_SAP_DB and ENR_ESENZIONI_RICHIESTE_DB.
        """
        try:
            self.backup()
            self.logger.info("")
            self.logger.info("Load")
            self.drop_columns(["BP", "FLAG_ESENTE_SAP", "FLAG_ESENTE_RIC", "FLAG_ESENTE"])
            df = ENR_ESENZIONI_SAP(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["CNT_COD", "FLAG_ESENTE_SAP"]],
                columns=["CNT_COD"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_ESENTE_SAP"].fillna(0, inplace=True)
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_ESENZIONI_RICHIESTE(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["BP", "FLAG_ESENTE_RIC"]],
                lcolumns=["CNT_COD"],
                rcolumns=["BP"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_ESENTE_RIC"].fillna(0, inplace=True)
            self.df["FLAG_ESENTE"] = (self.df["FLAG_ESENTE_SAP"] == 1) | (self.df["FLAG_ESENTE_RIC"] == 1)
            self.drop_columns(["BP"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_RATEIZZAZIONI(self):
        """
        Merge ENR_RATEIZZAZIONI_SAP_DB and ENR_RATEIZZAZIONI_RICHIESTE_DB.
        """
        try:
            self.backup()
            self.drop_columns(
                ["Codice_Utente", "FLAG_RATE_SAP", "FLAG_RATE_RIC", "FLAG_RATE", "NUM_RATE", "IMPORTO_RATEIZZATO"]
            )
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_RATEIZZAZIONI_SAP(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["NUM_FATT", "FLAG_RATE_SAP", "NUM_RATE", "IMPORTO_RATEIZZATO"]],
                columns=["NUM_FATT"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_RATE_SAP"].fillna(0, inplace=True)
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_RATEIZZAZIONI_RICHIESTE(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["Codice_Utente", "FLAG_RATE_RIC"]],
                lcolumns=["CNT_COD"],
                rcolumns=["Codice_Utente"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_RATE_RIC"].fillna(0, inplace=True)
            self.df["FLAG_RATE"] = (self.df["FLAG_RATE_SAP"] == 1) | (self.df["FLAG_RATE_RIC"] == 1)
            self.drop_columns(["Codice_Utente"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_BOLLETTINI(self):
        """
        Merge ENR_BOLLETTINI_WEB_DB.
        """
        try:
            self.backup()
            self.drop_columns(["FATTURA", "FLAG_BOLLETTINO_WEB"])
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_BOLLETTINI_WEB(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["FATTURA", "FLAG_BOLLETTINO_WEB"]],
                lcolumns=["NUM_FATT"],
                rcolumns=["FATTURA"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_BOLLETTINO_WEB"].fillna(0, inplace=True)
            self.drop_columns(["FATTURA"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_COPIE_CONFORMI(self):
        """
        Merge ENR_BOLLETTINI_CC_DB.
        """
        try:
            self.backup()
            self.drop_columns(["Codice Utente", "FLAG_BOLLETTINO_CC"])
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_BOLLETTINI_CC(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["Codice Utente", "FLAG_BOLLETTINO_CC"]],
                lcolumns=["CNT_COD"],
                rcolumns=["Codice Utente"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_BOLLETTINO_CC"].fillna(0, inplace=True)
            self.drop_columns(["Codice Utente"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_PAGAMENTI(self):
        """
        Merge ENR_PAGAMENTI_DB.
        """
        try:
            self.backup()
            self.drop_columns(["BP", "CONTRATTO", "IMPORTO_PAGAMENTO"])
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_PAGAMENTI(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["BP", "CONTRATTO", "NUM_FATT", "IMPORTO_PAGAMENTO"]],
                lcolumns=["CNT_COD", "UTZ_CONTRATTO", "NUM_FATT"],
                rcolumns=["BP", "CONTRATTO", "NUM_FATT"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["IMPORTO_PAGAMENTO"].fillna(0, inplace=True)
            self.drop_columns(["BP", "CONTRATTO"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_PAGAMENTI_F24(self):
        """
        Merge ENR_PAGAMENTI_F24_DB.
        """
        try:
            self.backup()
            self.drop_columns(["CodFis", "FLAG_F24", "IMPORTO_F24"])
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_PAGAMENTI_F24(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["CodFis", "FLAG_F24", "IMPORTO_F24"]],
                lcolumns=["CNT_COD_FSC"],
                rcolumns=["CodFis"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_F24"].fillna(0, inplace=True)
            self.df["IMPORTO_F24"].fillna(0, inplace=True)
            self.drop_columns(["CodFis"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")


class ENR_DOM(ENR_UTENZE):
    """
    Utenze Domestiche.
    """

    def __init__(self, **kwargs):
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/DOM.pkl.zip",
            keys=["CNT_COD", "UTZ_CONTRATTO", "CNT_COD_FSC", "NUM_FATT"],
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        pass

    def process(self):
        """
        Process DataFrame.
        """
        # self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        # self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass

    def merge_ALL(self):
        """
        Merge all DBs.
        """
        try:
            self.merge_CONTRATTI()
            self.merge_GE_VIE()
            self.merge_DECESSI()
            self.merge_CESSAZIONI()
            self.merge_RECAPITI()
            self.merge_ESENZIONI()
            self.merge_RATEIZZAZIONI()
            self.merge_BOLLETTINI()
            self.merge_COPIE_CONFORMI()
            self.merge_PAGAMENTI_F24()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_CONTRATTI(self):
        """
        Merge ENR_DOM_AVVISI_CONTRATTI_ATTIVI_DB and ENR_DOM_CONTRATTI_ATTIVI_DB.
        """
        try:
            self.backup()
            self.logger.info("")
            self.logger.info("Load")
            df1 = ENR_DOM_AVVISI_CONTRATTI_ATTIVI(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.file = df1.file
            self.df = df1.df
            df2 = ENR_DOM_CONTRATTI_ATTIVI(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df2.df,
                columns=["CNT_COD", "UTZ_CONTRATTO"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df1, df2
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_RECAPITI(self):
        """
        Merge ENR_DOM_RECAPITI_DB.
        """
        try:
            self.backup()
            self.drop_columns(["FLAG_RECAPITO", "BP", "CONTRATTO"])
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_DOM_RECAPITI(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["BP", "CONTRATTO", "NUM_FATT", "FLAG_RECAPITO"]],
                lcolumns=["CNT_COD", "UTZ_CONTRATTO", "NUM_FATT"],
                rcolumns=["BP", "CONTRATTO", "NUM_FATT"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_RECAPITO"].fillna(-3, inplace=True)
            self.drop_columns(["BP", "CONTRATTO"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")


class ENR_NDOM(ENR_UTENZE):
    """
    Utenze Non Domestiche.
    """

    def __init__(self, **kwargs):
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/NDOM.pkl.zip",
            keys=["CNT_COD", "UTZ_CONTRATTO", "CNT_COD_FSC", "NUM_FATT"],
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        pass

    def process(self):
        """
        Process DataFrame.
        """
        # self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        # self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass

    def merge_ALL(self):
        """
        Merge all DBs.
        """
        try:
            self.merge_CONTRATTI()
            self.merge_GE_VIE()
            self.merge_DECESSI()
            self.merge_CESSAZIONI()
            self.merge_PEC()
            self.merge_GET()
            self.merge_ESENZIONI()
            self.merge_RATEIZZAZIONI()
            self.merge_BOLLETTINI()
            self.merge_COPIE_CONFORMI()
            self.merge_PAGAMENTI_F24()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_CONTRATTI(self):
        """
        Merge ENR_NDOM_AVVISI_CONTRATTI_ATTIVI_DB and ENR_NDOM_CONTRATTI_ATTIVI_DB.
        """
        try:
            self.backup()
            self.logger.info("")
            self.logger.info("Load")
            df1 = ENR_NDOM_AVVISI_CONTRATTI_ATTIVI(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.file = df1.file
            self.df = df1.df
            df2 = ENR_NDOM_CONTRATTI_ATTIVI(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df2.df,
                columns=["CNT_COD", "UTZ_CONTRATTO"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df1, df2
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_PEC(self):
        """
        Merge ENR_NDOM_PEC_SAP_DB and ENR_NDOM_PEC_INVII_DB.
        """
        try:
            self.backup()
            self.drop_columns(["FLAG_PEC_SAP", "FLAG_PEC_INV", "FLAG_NOPEC", "FLAG_PEC", "NUMERO_FATTURA", "SEMESTRE"])
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_NDOM_PEC_SAP(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["CNT_COD", "FLAG_PEC_SAP"]],
                columns=["CNT_COD"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_PEC_SAP"].fillna(1, inplace=True)
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_NDOM_PEC_INVII(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[
                    [
                        "NUMERO_FATTURA",
                        "SEMESTRE",
                        "FLAG_PEC_INV",
                    ]
                ],
                lcolumns=["NUM_FATT"],
                rcolumns=["NUMERO_FATTURA"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_PEC_INV"].fillna(0, inplace=True)
            self.df.loc[self.df["FLAG_PEC_INV"] == 2, "FLAG_PEC_INV"] = 1
            self.df["FLAG_NOPEC"] = (self.df["FLAG_PEC_SAP"] == 0) | (self.df["FLAG_PEC_INV"] == 0)
            # self.df.loc[self.df["FLAG_PEC_INV"] == 2, "FLAG_NOPEC"] = False
            self.df["FLAG_PEC"] = (self.df["FLAG_PEC_SAP"] == 1) | (self.df["FLAG_PEC_INV"] == 1)
            # self.df.loc[self.df["FLAG_PEC_INV"] == 2, "FLAG_PEC"] = True
            self.drop_columns(["NUMERO_FATTURA"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")

    def merge_GET(self):
        """
        Merge ENR_GET_ATTIVITA and ENR_GET_PRATICHE.
        """
        try:
            self.backup()
            self.drop_columns(
                [
                    "FLAG_GET_ATTIVITA",
                    "FLAG_GET_PRATICHE",
                    "SOGGETTO_INTESTATARIO",
                    "STATO",
                    "CODICE_FISCALE_RICHIEDENTE",
                    "STATO_LAVORAZIONE",
                ]
            )
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_GET_ATTIVITA(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["SOGGETTO_INTESTATARIO", "STATO", "FLAG_GET_ATTIVITA"]],
                lcolumns=["CNT_COD_FSC"],
                rcolumns=["SOGGETTO_INTESTATARIO"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_GET_ATTIVITA"].fillna(0, inplace=True)
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_GET_PRATICHE(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["CODICE_FISCALE_RICHIEDENTE", "STATO_LAVORAZIONE", "FLAG_GET_PRATICHE"]],
                lcolumns=["CNT_COD_FSC"],
                rcolumns=["CODICE_FISCALE_RICHIEDENTE"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.df["FLAG_GET_PRATICHE"].fillna(0, inplace=True)
            self.drop_columns(["SOGGETTO_INTESTATARIO", "CODICE_FISCALE_RICHIEDENTE"])
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")


class ENR_DECESSI(DataFactory):
    """
    Deceduti.
    """

    def __init__(self, **kwargs):
        dtype = {
            "ID": "string",
            "ID_ANA_FORNITURA": "string",
            "DENOMINAZIONE": "string",
            "P_IVA": "string",
            "COD_FISCALE": "string",
            "DES_CMN_NSC": "string",
            "IDR_SOG": "string",
            "REGOLA": "string",
            "TIP_SOG": "string",
            "AT_CLEAN_DENOMIMAZIONE": "string",
            "AT_CLEAN_COD_FIS": "string",
            "AT_CLEAN_PAR_IVA": "string",
            "FLG_DEC": "string",
            "VERIFICA_SOGG": "string",
            "CMN_DES": "string",
            "DEN_VIA": "string",
            "NUM_CIV": "string",
            "IDR_VIA_NORM": "string",
            "DEN_VIA_NORM": "string",
            "NUM_CIV_NORM": "string",
            "REGOLA_NORM": "string",
            "VERIFICA_IND": "string",
        }
        self.parse_dates = {"DAT_NSC": "%Y%m%d", "DAT_DEC": "%Y%m%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | Soggetti_Deceduti",
            keys=["COD_FISCALE"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.df["FLAG_DECEDUTO"] = 0
        self.set_types(["FLAG_DECEDUTO"], "int")
        self.df.loc[self.df["VERIFICA_SOGG"] == "DECEDUTI", "FLAG_DECEDUTO"] = 1

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropdup=self.keys)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_CESSAZIONI(DataFactory):
    """
    Partite IVA cessate.
    """

    def __init__(self, **kwargs):
        dtype = {
            "ID_ANA_FORNITURA": "string",
            "DENOMINAZIONE": "string",
            "COD_FISCALE": "string",
            "P_IVA": "string",
            "DAT_NSC": "string",
            "DES_CMN_NSC": "string",
            "FORNITURA": "string",
            "IDR_SOG": "string",
            "REGOLA": "string",
            "AT_CLEAN_DENOMIMAZIONE": "string",
            "AT_CLEAN_COD_FIS": "string",
            "AT_CLEAN_PAR_IVA": "string",
            "AT_FLG_STA_PI": "string",
            "RI_CLEAN_DENOMIMAZIONE": "string",
            "RI_CLEAN_COD_FIS": "string",
            "RI_CLEAN_PAR_IVA": "string",
            "RI_CLEAN_STA_PAR_IVA": "string",
            "RI_CLEAN_DAT_CESS": "string",
            "ANNO_RI_DAT_CESS": "string",
            "AT_DEN_SIM": "string",
            "RI_DEN_SIM": "string",
            "VERIFICA": "string",
            "IDR_NAT_GIU": "string",
            "DECOD_IDR_NAT_GIU": "string",
            "SUM_IMPORTO_CONTRATTO": "string",
            "SUM_IMP_PAREGGIO": "string",
            "MOTIVO_PAREGGIO": "string",
            "ANNO_PAREGGIO": "string",
            "FLG_PRESENTE_BOL": "string",
        }
        self.parse_dates = {"DAT_NSC": "%Y%m%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/RisultatoAnalisi/Verifica_anagrafica_3 (NON DOMESTICHE).xlsx",
            sheet_name="Aggregazione per cnt_cod",
            keys=["COD_FISCALE"],
            dtype=dtype,
            # parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.df["FLAG_CESSATA"] = 0
        self.set_types(["FLAG_CESSATA"], "int")
        self.df.loc[self.df["RI_CLEAN_STA_PAR_IVA"] == "Cessata", "FLAG_CESSATA"] = 1

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropna=self.keys)
        self.clean_data(dropdup=self.keys)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_ESENZIONI_SAP(DataFactory):
    """
    Esenti in SAP - persone fisiche con ISEE <= 6500EUR.
    """

    def __init__(self, **kwargs):
        dtype = {
            "CNT_COD": "string",
            "NOME": "string",
            "COGNOME": "string",
            "CF": "string",
            "CNT_VIA_DES": "string",
            "CNT_NUM_CIV": "string",
            "CNT_DES_LOC": "string",
            "CNT_CAP": "string",
            "CNT_CMN_DES": "string",
            "CNT_SGL_PRV": "string",
            "CONTO": "string",
            "CONTRATTO": "string",
            "IMPIANTO": "string",
            "Anno": "string",
            "Mese": "string",
        }
        self.parse_dates = {"DAT_ATT": "%Y%m%d", "DAT_CESS": "%Y%m%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | QB20230019208-STIMA_ESENTI_AL_30",
            keys=["CNT_COD"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["CNT_COD"], "string d0")

        self.df["FLAG_ESENTE_SAP"] = 1
        self.set_types(["FLAG_ESENTE_SAP"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropdup=True)
        self.clean_data(dropdup=self.keys)
        self.drop_rows(rows=self.df.index[self.df[["CNT_COD"]].isnull().all(1)].to_list())
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_ESENZIONI_RICHIESTE(DataFactory):
    """
    Richieste di esenzione.
    """

    def __init__(self, **kwargs):
        dtype = {
            "A01_TIP_DEN": "string",
            "TIPO_DENUNCIA": "string",
            "NUM_DENUNCIA": "string",
            "ANNO_DENUNCIA": "string",
            "STATO_PRATICA": "string",
            "DESCR_LOTTO": "string",
            "ESENZIONE": "string",
            "ACCET_ESENZIONE": "string",
            "BP": "string",
            "CNT_COGNOME": "string",
            "CNT_NOME": "string",
            "CF": "string",
            "IMPIANTO": "string",
            "CONTRATTO": "string",
        }
        self.parse_dates = {"DATADENUNCIA": "%Y%m%d", "DATASTATO": "%Y%m%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | QB20230019208-ESENZIONI_RICEVUTE_2022___STATO",
            keys=["BP"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        # self.df.rename(columns={"DATASTATO": "DATA_ESENTE_RIC"}, inplace=True)

        self.df["FLAG_ESENTE_RIC"] = 1
        self.set_types(["FLAG_ESENTE_RIC"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply("STATO_PRATICA == 'CONCLUSO' | STATO_PRATICA == 'IN LAVORAZIONE'", inplace=True)

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropdup=True)
        self.clean_data(dropdup=self.keys)
        self.drop_rows(rows=self.df.index[self.df[["BP"]].isnull().all(1)].to_list())
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_GET_ATTIVITA(DataFactory):
    """
    GET attività.
    """

    def __init__(self, **kwargs):
        dtype = {
            "CIU": "string",
            "SOGGETTO_INTESTATARIO": "string",
            "TIPO": "string",
            "RAGIONE_SOCIALE": "string",
            "COGNOME": "string",
            "NOME": "string",
            "SEDE_LEGALE_VIA": "string",
            "SEDE_LEGALE_CIVICO": "float64",
            "SEDE_LEGALE_ESPONENTE": "string",
            "SEDE_LEGALE_CAP": "float64",
            "SEDE_LEGALE_COMUNE": "string",
            "SEDE_LEGALE_PROVINCIA": "string",
            "RESIDENZA_VIA": "string",
            "RESIDENZA_CIVICO": "float64",
            "RESIDENZA_ESPONENTE": "string",
            "RESIDENZA_CAP": "string",
            "RESIDENZA_COMUNE": "string",
            "RESIDENZA_PROVINCIA": "string",
            "RUOLO": "string",
            "STATO": "string",
            "STRUTTURA_COMPETENTE": "string",
            "DATA_ULTIMA_PRATICA": "string",
            "PROTOCOLLO_ULTIMA_PRATICA": "string",
        }
        self.parse_dates = {"DATA_ULTIMA_PRATICA": "%Y-%m-%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/Dati-GET/AFFIDAMENTI.xlsx",
            keys=["SOGGETTO_INTESTATARIO"],
            dtype=dtype,
            # parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.df["FLAG_GET_ATTIVITA"] = 1
        self.set_types(["FLAG_GET_ATTIVITA"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply(
            "STATO == 'AUTORIZZATA' | STATO == 'SEGNALATA' | STATO == 'SOSPESA' | STATO == 'SOSPESA_PER_PROVVEDIMENTO'",
            inplace=True,
        )

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropna=self.keys)
        self.clean_data(dropdup=self.keys)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_GET_PRATICHE(DataFactory):
    """
    GET pratiche.
    """

    def __init__(self, **kwargs):
        dtype = {
            "PROTOCOLLO_PRATICA": "string",
            "CODICE_FISCALE_RICHIEDENTE": "string",
            "TIPO": "string",
            "RAGIONE_SOCIALE": "string",
            "NATURA_GIURIDICA": "string",
            "COGNOME": "string",
            "NOME": "string",
            "SEDE_LEGALE_VIA": "string",
            "SEDE_LEGALE_CIVICO": "string",
            "SEDE_LEGALE_ESPONENTE": "string",
            "SEDE_LEGALE_CAP": "string",
            "SEDE_LEGALE_COMUNE": "string",
            "SEDE_LEGALE_PROVINCIA": "string",
            "RESIDENZA_VIA": "string",
            "RESIDENZA_CIVICO": "string",
            "RESIDENZA_ESPONENTE": "string",
            "RESIDENZA_CAP": "string",
            "RESIDENZA_COMUNE": "string",
            "RESIDENZA_PROVINCIA": "string",
            "TIPOLOGIA_PRATICA": "string",
            "DATA_PRESENTAZIONE": "string",
            "STATO_LAVORAZIONE": "string",
            "STRUTTURA_COMPENTENTE": "string",
        }
        self.parse_dates = {"DATA_PRESENTAZIONE": "%Y-%m-%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/Dati-GET/PRATICHE.xlsx",
            keys=["CODICE_FISCALE_RICHIEDENTE"],
            dtype=dtype,
            # parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.df["FLAG_GET_PRATICHE"] = 1
        self.set_types(["FLAG_GET_PRATICHE"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply(
            "STATO_LAVORAZIONE == 'ACQUISIZIONE' | STATO_LAVORAZIONE == 'ISTRUTTORIA' | STATO_LAVORAZIONE == 'RICEVUTA'",
            inplace=True,
        )

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropna=self.keys)
        self.clean_data(dropdup=self.keys)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_PAGAMENTI(DataFactory):
    """
    Pagamenti Regolarizzati in SAP.
    """

    def __init__(self, **kwargs):
        dtype = {
            "BP": "string",
            "CONTRATTO": "string",
            "NUM_FATT": "string",
            "IMPORTO_PAREGGIO": "float64",
        }
        self.parse_dates = {"DATA_PAREGGIO": "%Y%m%d"}
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | 8_pagamenti_doc_20220131_con_importo",
            keys=["BP", "CONTRATTO", "NUM_FATT"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["CONTRATTO", "NUM_FATT"], "string d0")

        self.df.rename(
            columns={"IMPORTO_PAREGGIO": "IMPORTO_PAGAMENTO", "DATA_PAREGGIO": "DATA_PAGAMENTO"}, inplace=True
        )
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply("DATA_PAGAMENTO >= '2022-06-01' | DATA_PAGAMENTO.isnull()", inplace=True)

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_PAGAMENTI_BONIFICI(DataFactory):
    """
    Pagamenti Extra Nodo con Bonifico.
    """

    def __init__(self, **kwargs):
        dtype = {
            "ID": "string",
            "CCP": "string",
            "TP_BON": "string",
            "IMPORTO_BON": "string",
            "SOGG": "string",
            "NOTE": "string",
        }
        # self.parse_dates = {"DT_REG": "%d-%b-%y", "DT_PG": "%d-%b-%y"}  # LOCALE IT
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | BONIF_TARI_export",
            keys=["ID"],
            dtype=dtype,
            # parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["DT_REG", "DT_PG"], "datetime loc_IT", "%d-%m-%y")

        self.set_types(["IMPORTO_BON"], "numeric c2p")

        self.df.rename(columns={"DT_PG": "DATA_BON"}, inplace=True)
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply("DATA_BON >= '2022-06-01'", inplace=True)
        self.query_apply(
            "NOTE.str.contains('TA[., ]?RI',regex=True,case=False) & NOTE.str.contains('sem',regex=True,case=False)",
            inplace=True,
        )
        self.query_apply(  # NOQA W605
            r"~NOTE.str.contains('[\D\S]II[\D\S]',regex=True,case=False) & ~NOTE.str.contains('[\D\S]2o[\D\S]',regex=True,case=False) & ~NOTE.str.contains('[\s]sec',regex=True,case=False)",
            inplace=True,
        )

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_PAGAMENTI_F24(DataFactory):
    """
    Pagamenti Extra Nodo con F24.
    """

    def __init__(self, **kwargs):
        dtype = {
            "ProgrDel": "string",
            "ProgrRig": "string",
            "CodEn": "string",
            "TipoEn": "string",
            "Cab": "string",
            "CodFis": "string",
            "CodTrib": "string",
            "Rateaz": "string",
            "AnnoRif": "string",
            "CodVal": "string",
            "Importo": "float64",
            "RavvIci": "string",
            "ImmVarIci": "string",
            "AccIci": "string",
            "SalIci": "string",
            "NumFabIci": "string",
            "Filler1": "string",
            "IdeFile": "string",
        }
        self.parse_dates = {
            "DataBon": "%Y%m%d",
            "DataRisc": "%Y%m%d",
            "DataRip": "%Y%m%d",
            "DataForn": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | Tares_3944_3950_365E_368E_TEFA",
            keys=["CodFis"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.df.rename(columns={"Importo": "IMPORTO_F24", "DataBon": "DATA_F24"}, inplace=True)

        self.df["FLAG_F24"] = 1
        self.set_types(["FLAG_F24"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply("AnnoRif=='2022' & CodTrib=='3944' & DataRisc >= '2022-06-01'", inplace=True)
        self.query_apply("Rateaz == '101' | Rateaz == '102'", inplace=True)

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")

        self.merge_duplicates("CodFis", "IMPORTO_F24")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_PAGAMENTI_PAGOPA(DataFactory):
    """
    Pagamenti con Nodo PagoPA.
    """

    def __init__(self, **kwargs):
        dtype = {
            "CONVENZIONATORE": "string",
            "CONTODIACCREDITO": "string",
            "CODICERIF.CREDITORE": "string",  # NUMERO FATTURA
            "CODICEAVVISO": "string",
            "TIPORATA": "string",
            "CODICERATA": "string",
            "IMPORTOAVVISO": "float64",
            "CAUSALE": "string",
            "TIPODATIPA": "string",
            "DATIPA": "string",
            "IDVERSANTE": "string",  # CODICE FISCALE
            "ANAGRAFICAVERSANTE": "string",
            "RETEINCASSO": "string",
            "IDENTIFICATIVOPSP": "string",
            "IMPORTOPAGATO": "float64",
            "IMPORTOTOTALEACCREDITATO": "float64",
            "DOMINIOBENEFICIARIO": "string",
            "QUOTASECONDARIA": "string",
        }
        self.parse_dates = {
            "DATA_OPERAZIONE": "%Y%m%d",
            "DATA_SCADENZA": "%Y%m%d",
            "DATA_PAGAMENTO": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | RiconciliatiPoste_totale",
            keys=["IDVERSANTE", "CODICERIF.CREDITORE"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.df.rename(
            columns={
                "CODICERIF.CREDITORE": "CODICERIFCREDITORE",
                "IMPORTOAVVISO": "IMPORTO_AVVISO",
                "IMPORTOPAGATO": "IMPORTO_PAGOPA",
                "DATA_PAGAMENTO": "DATA_PAGOPA",
            },
            inplace=True,
        )
        self.keys = ["CODICERIFCREDITORE" if key == "CODICERIF.CREDITORE" else key for key in self.keys]

        self.df["FLAG_PAGOPA"] = 1
        self.set_types(["FLAG_PAGOPA"], "int")

        # self.df.replace(r"^=", "", regex=True, inplace=True)
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply(
            "CODICERIFCREDITORE.str.startswith('1122') & (QUOTASECONDARIA == '' | QUOTASECONDARIA == 'N' | QUOTASECONDARIA.isnull()) & DATA_PAGOPA >= '2022-06-01'",
            inplace=True,
        )

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_BOLLETTINI_WEB(DataFactory):
    """
    Utenze che hanno effettuato autonomamente il download del bollettino.
    """

    def __init__(self, **kwargs):
        dtype = {
            "FATTURA": "string",
            "BP": "string",
        }
        self.parse_dates = {
            "DATA": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | 1_elenco_pdf_2022",
            keys=["FATTURA"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.df.rename(columns={"DATA": "DATA_BOLLETTINO"}, inplace=True)

        self.set_types(["FATTURA"], "string d0")

        self.df["FLAG_BOLLETTINO_WEB"] = 1
        self.set_types(["FLAG_BOLLETTINO_WEB"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropdup=True)
        self.clean_data(dropdup=self.keys)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_BOLLETTINI_CC(DataFactory):
    """
    Utenze che hanno ricevuto la copia conforme del bollettino.
    """

    def __init__(self, **kwargs):
        dtype = {
            "Numero Protocollo": "string",
            "Codice Utente": "string",
            "Stato": "string",
            "Categoria": "string",
            "Canale di Trasmissione": "string",
            "Modello": "string",
            "Tipologia Documento": "string",
            "Gruppo": "string",
            "Assegnato a": "string",
            "Pratica Non Completa ": "string",
            "Sportello AMA": "string",
            "Codice TariWeb ": "string",
            "Anno": "string",
            "Tipo_utenza": "string",
        }
        self.parse_dates = {
            "DATA_CREAZIONE": "%Y%m%d",
            "DATA_MODIFICA": "%Y%m%d",
            "DATA_CHIUSURA": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/Flussi/Copie_Conformi_pulito.csv",
            keys=["Codice Utente"],
            dtype=dtype,
            # parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["DATA_CREAZIONE", "DATA_MODIFICA", "DATA_CHIUSURA"], "datetime", "%Y%m%d")

        self.set_types(["Codice Utente"], "string l0")

        self.df["FLAG_BOLLETTINO_CC"] = 1
        self.set_types(["FLAG_BOLLETTINO_CC"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropdup=True)
        self.clean_data(dropdup=self.keys)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_RATEIZZAZIONI_SAP(DataFactory):
    """
    Fatture raetizzate.
    """

    def __init__(self, **kwargs):
        dtype = {
            "CODICE_PIANO": "string",
            "CODICE_UTENTE": "string",
            "NUM_FATT": "string",
            "NUM_RATE": "int64",
            "IMPORTO_RATEIZZATO": "float64",
        }
        self.parse_dates = {
            "DATA_EMISSIONE_FATT": "%Y%m%d",
            "DATA_FINE_PIANO": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | 5b_rateizzazioni_concesse_fatt_2022",
            keys=["NUM_FATT"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["NUM_FATT"], "string d0")

        self.df["FLAG_RATE_SAP"] = 1
        self.set_types(["FLAG_RATE_SAP"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply("~CODICE_UTENTE.str.startswith('T')", inplace=True)

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_RATEIZZAZIONI_RICHIESTE(DataFactory):
    """
    Richieste di rateizzazioni.
    """

    def __init__(self, **kwargs):
        dtype = {
            "Numero Protocollo": "string",
            "Codice Utente": "string",
            "Stato": "string",
            "Categoria": "string",
            "Canale di Trasmissione": "string",
            "Modello": "string",
            "Tipologia Documento": "string",
            "Gruppo": "string",
            "Assegnato a": "string",
            "Pratica Non Completa ": "string",
            "Sportello AMA": "string",
            "Codice TariWeb ": "string",
            "Anno": "string",
            "Tipo_utenza": "string",
            "DATA_MODIFICA": "string",
            "DATA_CHIUSURA": "string",
        }
        self.parse_dates = {
            "DATA_CREAZIONE": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | Rateizzazioni_TARI_2022_v2",
            keys=["Codice Utente"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["DATA_MODIFICA", "DATA_CHIUSURA"], "datetime d0", "%Y%m%d")

        self.set_types(["Codice TariWeb "], "string d0")

        self.df.rename(columns={"Codice Utente": "Codice_Utente"}, inplace=True)
        self.keys = ["Codice_Utente" if key == "Codice Utente" else key for key in self.keys]

        self.df["FLAG_RATE_RIC"] = 1
        self.set_types(["FLAG_RATE_RIC"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply(
            "~Codice_Utente.str.startswith('T') & Stato != 'Annullata' & Stato != 'Rifiutata'",
            inplace=True,
        )

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropdup=True)
        self.clean_data(dropdup=["Codice_Utente"])
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_RECLAMI(DataFactory):
    """
    Reclami.
    """

    def __init__(self, **kwargs):
        dtype = {
            "ID": "string",
            "PROTOCOLLO": "string",
            "CODICEUTENTE": "string",
            "STATO": "string",
            "DATACREAZIONE": "string",
            "CATEGORIA": "string",
            "CANALEDITRASMISSIONE": "string",
            "MODELLO": "string",
            "TIPOLOGIADOCUMENTO": "string",
            "DATAMODIFICA": "string",
            "PRATICANONCOMPLETATA": "string",
            "SPORTELLOAMA": "string",
            "CODICETARIWEB": "string",
            "DATACHIUSURA": "string",
            "DATASPEDIZIONE": "string",
            "GIRATARIMBORSO": "string",
            "RAGGRUPPAMENTO": "string",
        }
        self.parse_dates = {
            "DATACREAZIONE": "%d-%m-%Y",
            "DATAMODIFICA": "%d-%m-%Y",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | 7_CONTESTAZIONI_RECLAMI",
            keys=["CODICEUTENTE"],
            dtype=dtype,
            parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.df["FLAG_RECLAMO"] = 1
        self.set_types(["FLAG_RECLAMO"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(self.keys, ignore_index=True, inplace=True)
        pass


class ENR_COMUNI(DataFactory):
    """
    COMUNI.
    """

    def __init__(self, **kwargs):
        dtype = {
            "Comune": "string",
            "Provincia": "string",
            "SiglaProvincia": "string",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/{SQLITE_DB} | Comuni",
            keys=["Comune"],
            dtype=dtype,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric", toup=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["SiglaProvincia"], ignore_index=True, inplace=True)
        pass

    def comuni(self):
        """
        Comune list.

        :return: Comune list
        """
        return sorted(self.df["Comune"].unique())

    def provincie(self):
        """
        Provincia list.

        :return: Provincia list
        """
        return sorted(self.df["Provincia"].unique())

    def sigle_provincie(self):
        """
        Sigla Provincia list.

        :return: SiglaProvincia list
        """
        return sorted(self.df["SiglaProvincia"].unique())


class ENR_GE_TERRIT(DataFactory):
    """
    GE_TERRIT.
    """

    def __init__(self, **kwargs):
        na_values = ["", " ", "  ", "-"]
        dtype = {
            "COD_VIA": "string",
            "PRG_TRA": "string",
            "NUM_CIR": "int64",
            "COD_SUD_TER": "string",
            "COD_TIP_CIV": "string",
            "TIP_NUM_NUM": "string",
            "LIM_CIV_DA": "int64",
            "COD_ALF_CIV_DA": "string",
            "FLG_TRA_DA": "string",
            "LIM_CIV_A": "int64",
            "COD_ALF_CIV_A": "string",
            "FLG_TRA_A": "string",
            "COD_ISO": "string",
            "SEZ_CEN": "string",
            "COD_ZON_URB": "string",
            "CAP": "string",
        }
        self.parse_dates = {
            "DTA_INI_VAL_STR": "%Y%m%d",
            "DTA_FIN_VAL_STR": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/Mappa/GE_TERRIT.csv",
            # keys=["COD_VIA", "CAP", "COD_ZON_URB", "NUM_CIR"],
            keys=["COD_VIA", "NUM_CIR"],
            na_values=na_values,
            sep="|",
            dtype=dtype,
            # parse_dates=self.parse_dates,
            low_memory=False,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["DTA_INI_VAL_STR", "DTA_FIN_VAL_STR"], dtype="datetime", format="%Y%m%d")
        self.set_types(["NUM_CIR"], dtype="string")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.query_apply("NUM_CIR != '0' & LIM_CIV_DA != 0 & LIM_CIV_A != 0", inplace=True)

        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        self.clean_data(dropna=self.keys)
        self.merge_duplicates(self.keys, ["LIM_CIV_DA", "LIM_CIV_A"], {"LIM_CIV_DA": "min", "LIM_CIV_A": "max"})
        self.clean_data(dropdup=["COD_VIA", "LIM_CIV_DA", "LIM_CIV_A"], keep=False)
        self.clean_data(dropdup=["COD_VIA", "LIM_CIV_DA"], keep=False)
        self.clean_data(dropdup=["COD_VIA", "LIM_CIV_A"], keep=False)
        self.clean_data(dropdup=["COD_VIA"], keep=False)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["COD_VIA", "LIM_CIV_DA", "LIM_CIV_A"], ignore_index=True, inplace=True)
        pass


class ENR_GE_VIE(DataFactory):
    """
    GE_VIE.
    """

    def __init__(self, **kwargs):
        na_values = ["", " ", "  ", "-"]
        dtype = {
            "COD_VIA": "string",
            "COD_VIA_ALT": "string",
            "COD_GTO": "string",
            "COD_SED": "string",
            "DSC_VIA": "string",
            "DSC_DID": "string",
            "DSC_VIA_BRV": "string",
            "COD_NEW": "string",
        }
        self.parse_dates = {
            "DT_IST": "%Y%m%d",
            "DT_SOP": "%Y%m%d",
            "DT_MOD_TAB": "%Y%m%d",
        }
        super().__init__(
            df=f"{BASE_FOLDER_ENR}/Mappa/GE_VIE.csv",
            keys=["COD_VIA"],
            na_values=na_values,
            sep="|",
            dtype=dtype,
            # parse_dates=self.parse_dates,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.set_types(["DT_IST", "DT_SOP"], "datetime", "%Y%m%d")
        self.set_types(["DT_MOD_TAB"], "datetime", "%Y-%m-%d %H:%M:%S.%f")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(toup="DSC_VIA")
        self.clean_data(datetime=True, empty=True, spaces=True, fillna="numeric")
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["COD_VIA"], ignore_index=True, inplace=True)
        pass

    def merge_GE_TERRIT(self):
        """
        Merge ENR_GE_TERRIT_DB.
        """
        try:
            self.drop_columns(
                columns=[
                    "COD_VIA_ALT",
                    "COD_GTO",
                    "COD_SED",
                    "DSC_DID",
                    "DSC_VIA_BRV",
                    "COD_NEW",
                    "DT_IST",
                    "DT_SOP",
                    "DT_MOD_TAB",
                ]
            )
            self.backup()
            self.logger.info("")
            self.logger.info("Load")
            df = ENR_GE_TERRIT(
                pre_process=True,
                process=True,
                post_process=True,
                silent=True,
            )
            self.logger.info("")
            self.logger.info("Merge")
            self.df = self.merge(
                df.df[["COD_VIA", "LIM_CIV_DA", "LIM_CIV_A", "CAP", "COD_ZON_URB", "NUM_CIR"]],
                columns=["COD_VIA"],
                how="left",
                datafactory=False,
                silent=self.silent,
            )
            del df
            self.set_types(["LIM_CIV_DA", "LIM_CIV_A"], "int")
            self.query_apply("LIM_CIV_DA != 0 & LIM_CIV_A != 0", inplace=True)
            self.update()
            self.print()
            self.check_data()
            self.check_keys()
        except Exception as e:
            self.display(self.df)
            self.restore()
            raise ValueError(f"{e}")
