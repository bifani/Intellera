from src.datafactory import DataFactory

BASE_FOLDER_BA = "../data/AMA_Progetto_Fabbisogni/BonificaAnagrafe"


class BA_CNT(DataFactory):
    """
    BA - CNT.
    """

    def __init__(self, **kwargs):
        na_values = ["", " ", "  "]
        dtype = {
            "CNT_COD": "object",
            "CNT_TCN_COD": "category",
            "CNT_COD_FSC": "object",
            "CNT_PAR_IVA": "object",
            "CNT_COGNOME": "object",
            "CNT_NOME": "object",
            "CNT_CMN_DES_NSC": "category",
            "CNT_CMN_COD": "object",
            "CNT_VIA_COD": "object",
            "CNT_VIA_DES": "object",
            "CNT_NUM_CIV": "object",
            "CNT_ESP_CIV": "object",
            "CNT_PIA": "object",
            "CNT_NUM_INT": "object",
            "CNT_SCA": "object",
            "CNT_LOT": "object",
            "CNT_EDF": "object",
            "CNT_DES_LOC": "category",
            "CNT_CAP": "category",
            "CNT_CMN_DES": "category",
            "CNT_SGL_PRV": "category",
            "CNT_SGL_NAZ": "category",
            "CNT_TEL": "object",
        }
        self.parse_dates = ["CNT_DAT_NSC"]
        super().__init__(
            df=f"{BASE_FOLDER_BA}/cnt_20220605.tsv",
            na_values=na_values,
            dtype=dtype,
            parse_dates=self.parse_dates,
            infer_datetime_format=True,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        # self.df["CNT_DAT_NSC"] = self.df["CNT_DAT_NSC"].str.replace("00000000","")
        self.clean_data(datetime=self.parse_dates)
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["CNT_COD"], ignore_index=True, inplace=True)
        pass


class BA_NCL(DataFactory):
    """
    BA - NCL.
    """

    def __init__(self, **kwargs):
        na_values = ["", " ", "  "]
        dtype = {
            "NCL_UTD_COD": "object",
            "NCL_IMPIANTO": "object",
            "NCL_NUM_OCC": "int64",
        }
        self.parse_dates = ["NCL_DAT_INI", "NCL_DAT_FIN"]
        super().__init__(
            df=f"{BASE_FOLDER_BA}/ncl_20220605.tsv",
            na_values=na_values,
            dtype=dtype,
            parse_dates=self.parse_dates,
            infer_datetime_format=True,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.clean_data(datetime=self.parse_dates)
        self.df["NCL_DAT_FI"] = (self.df["NCL_DAT_FIN"] - self.df["NCL_DAT_INI"]).astype("timedelta64[D]")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["NCL_UTD_COD", "NCL_DAT_INI"], ignore_index=True, inplace=True)
        self.df = self.df.groupby(["NCL_UTD_COD", "NCL_IMPIANTO"]).nth(-1).reset_index()
        pass


class BA_PAG(DataFactory):
    """
    BA - PAG.
    """

    def __init__(self, **kwargs):
        na_values = ["", " ", "  "]
        dtype = {
            "GPART": "object",
            "PAG_RCP_COD": "object",
        }
        self.parse_dates = ["PAG_DAT"]
        super().__init__(
            df=f"{BASE_FOLDER_BA}/pag_20220605.tsv",
            na_values=na_values,
            dtype=dtype,
            parse_dates=self.parse_dates,
            infer_datetime_format=True,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.clean_data(datetime=self.parse_dates)
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["PAG_RCP_COD"], ignore_index=True, inplace=True)
        self.df.groupby(["GPART", "PAG_RCP_COD"]).nth(-1).reset_index()
        pass


class BA_RCP(DataFactory):
    """
    BA - RCP.
    """

    def __init__(self, **kwargs):
        na_values = ["", " ", "  "]
        dtype = {
            "RCP_COD": "object",
            "RCP_CNT_COD": "object",
            "RCP_FLG": "category",
            "RCP_BLOCCO": "category",
            "RCP_CMN_COD": "object",
            "RCP_NOMINATIVO": "object",
            "RCP_VIA_COD": "object",
            "RCP_VIA_DES": "object",
            "RCP_NUM_CIV": "object",
            "RCP_ESP_CIV": "object",
            "RCP_PIA": "object",
            "RCP_NUM_INT": "object",
            "RCP_SCA": "object",
            "RCP_LOT": "object",
            "RCP_EDF": "object",
            "RCP_DES_LOC": "category",
            "RCP_CAP": "category",
            "RCP_CMN_DES": "category",
            "RCP_SGL_PRV": "category",
            "RCP_SGL_NAZ": "category",
            "RCP_TEL": "object",
            "RCP_FLG_FAT_ANNO": "category",
            "RCP_FLG_PAG_ANNO": "category",
        }
        super().__init__(
            df=f"{BASE_FOLDER_BA}/rcp_20220605.tsv",
            na_values=na_values,
            dtype=dtype,
            low_memory=False,
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
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["RCP_COD", "RCP_CNT_COD"], ignore_index=True, inplace=True)
        pass


class BA_UTD(DataFactory):
    """
    BA - UTD.
    """

    def __init__(self, **kwargs):
        na_values = ["", " ", "  "]
        dtype = {
            "UTD_COD": "object",
            "UTD_IMPIANTO": "object",
            "UTD_TIPO_RICH": "category",
            "UTD_RCP_COD": "object",
            "UTD_SPF_IMM": "object",
            "UTD_SPF_IMM_ALT": "object",
            "UTD_SPF_GAR": "object",
            "UTD_SPF_GAR_ALT": "object",
            "UTD_SPF_ALT": "object",
            "UTD_NUM_OCC": "object",
            "UTD_VIA_COD": "object",
            "UTD_NUM_CIV": "object",
            "UTD_ESP_CIV": "object",
            "UTD_PIA": "object",
            "UTD_NUM_INT": "object",
            "UTD_SCA": "object",
            "UTD_LOT": "object",
            "UTD_EDF": "object",
            "UTD_FLG_RID_COMP": "category",
            "UTD_FLG_RID_DIST": "category",
            "UTD_FLG_RID_NRES": "category",
            "UTD_FLG_SCONTO_NRES": "category",
            "UTD_FLG_RID_ESENZ": "category",
            "UTD_FLG_RID_ESEAN": "category",
            "UTD_BLOCCO": "category",
            "UTD_IMM_FOGLIO": "object",
            "UTD_IMM_PARTICELLA": "object",
            "UTD_IMM_SUBALTERNO": "object",
            "UTD_IMM_CLASSE": "object",
            "UTD_BOX_FOGLIO": "object",
            "UTD_BOX_PARTICELLA": "object",
            "UTD_BOX_SUBALTERNO": "object",
            "UTD_BOX_CLASSE": "object",
            "UTD_MRU": "object",
        }
        self.parse_dates = ["UTD_DAT_ATT", "UTD_DAT_CESS", "UTD_DAT_CREAZ"]
        super().__init__(
            df=f"{BASE_FOLDER_BA}/utd_20220605.tsv",
            na_values=na_values,
            dtype=dtype,
            parse_dates=self.parse_dates,
            infer_datetime_format=True,
            low_memory=False,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.clean_data(datetime=self.parse_dates)
        self.df["UTD_DAT_CA"] = (self.df["UTD_DAT_CESS"] - self.df["UTD_DAT_ATT"]).astype("timedelta64[D]")
        self.df["UTD_DAT_CA2"] = (self.df["UTD_DAT_CREAZ"] - self.df["UTD_DAT_ATT"]).astype("timedelta64[D]")
        self.df["UTD_DAT_CC"] = (self.df["UTD_DAT_CESS"] - self.df["UTD_DAT_CREAZ"]).astype("timedelta64[D]")

        self.set_types(
            ["UTD_NUM_OCC", "UTD_SPF_IMM", "UTD_SPF_IMM_ALT", "UTD_SPF_GAR", "UTD_SPF_GAR_ALT", "UTD_SPF_ALT"], "int"
        )
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df = self.df.query("UTD_DAT_ATT > 20170101")
        self.df.sort_values(["UTD_COD", "UTD_RCP_COD", "UTD_DAT_ATT"], ignore_index=True, inplace=True)
        pass


class BA_UTN(DataFactory):
    """
    BA - UTN.
    """

    def __init__(self, **kwargs):
        na_values = ["", " ", "  "]
        dtype = {
            "UTN_COD": "object",
            "UTN_IMPIANTO": "object",
            "UTN_TIPO_RICH": "category",
            "UTN_RCP_COD": "object",
            "UTN_SPF_IMM": "object",
            "UTN_SPF_ESENTE": "object",
            "UTN_FLG_STAG": "category",
            "UTN_CAT_COD": "category",
            "UTN_VIA_COD": "object",
            "UTN_NUM_CIV": "object",
            "UTN_ESP_CIV": "object",
            "UTN_PIA": "object",
            "UTN_NUM_INT": "object",
            "UTN_SCA": "object",
            "UTN_LOT": "object",
            "UTN_EDF": "object",
            "UTN_FLG_RID_DIST": "category",
            "UTN_FLG_RID_STAG": "category",
            "UTN_RID_RSU": "category",
            "UTN_FLG_RID_SC": "category",
            "UTN_BLOCCO": "category",
            "UTN_IMM_FOGLIO": "object",
            "UTN_IMM_PARTICELLA": "object",
            "UTN_IMM_SUBALTERNO": "object",
            "UTN_IMM_CLASSE": "object",
            "UTN_BOX_FOGLIO": "object",
            "UTN_BOX_PARTICELLA": "object",
            "UTN_BOX_SUBALTERNO": "object",
            "UTN_BOX_CLASSE": "object",
            "LETTURA": "object",
        }
        self.parse_dates = ["UTN_DAT_ATT", "UTN_DAT_CESS", "UTN_DAT_CREAZ"]
        super().__init__(
            df=f"{BASE_FOLDER_BA}/utn_20220605.tsv",
            na_values=na_values,
            dtype=dtype,
            parse_dates=self.parse_dates,
            infer_datetime_format=True,
            low_memory=False,
            **kwargs,
        )

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        self.clean_data(datetime=self.parse_dates)
        self.df["UTN_DAT_CA"] = (self.df["UTN_DAT_CESS"] - self.df["UTN_DAT_ATT"]).astype("timedelta64[D]")
        self.df["UTN_DAT_CA2"] = (self.df["UTN_DAT_CREAZ"] - self.df["UTN_DAT_ATT"]).astype("timedelta64[D]")
        self.df["UTN_DAT_CC"] = (self.df["UTN_DAT_CESS"] - self.df["UTN_DAT_CREAZ"]).astype("timedelta64[D]")

        self.set_types(["UTN_SPF_IMM", "UTN_SPF_ESENTE"], "int")
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df = self.df.query("UTN_DAT_ATT > 20170101")
        self.df.sort_values(["UTN_COD", "UTN_RCP_COD", "UTN_DAT_ATT"], ignore_index=True, inplace=True)
        pass


class BA_VIE(DataFactory):
    """
    BA - VIE.
    """

    def __init__(self, **kwargs):
        na_values = ["", " ", "  "]
        dtype = {
            "VIA_COD_SAP": "object",
            "VIA_COD_CMN": "object",
            "VIA_DESC": "object",
            "VIA_COD_CMN_NBIT": "object",
        }
        super().__init__(
            df=f"{BASE_FOLDER_BA}/vie_20220605.tsv",
            na_values=na_values,
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
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["VIA_COD_SAP"], ignore_index=True, inplace=True)
        pass


class BA_UTD_MERGED(BA_CNT):
    """
    BA - UTD MERGED.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        kwargs = {
            "pickled": True,
            "dry": True,
            "pre_process": False,
            "process": False,
            "post_process": False,
            "silent": True,
        }

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_RCP")
        df_rcp = BA_RCP(**kwargs)
        self.df = self.merge(df_rcp.df, lcolumn="CNT_COD", rcolumn="RCP_CNT_COD", how="left", datafactory=False)
        del df_rcp

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_PAG")
        df_pag = BA_PAG(**kwargs)
        df_pag_red = df_pag.df.groupby(["GPART", "PAG_RCP_COD"]).nth(-1).reset_index()
        self.df = self.merge(
            df_pag_red,
            lcolumn="RCP_COD",
            rcolumn="PAG_RCP_COD",
            how="left",
            datafactory=False,
        )
        del df_pag, df_pag_red

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_UTD")
        df_utd = BA_UTD(**kwargs)
        self.df = self.merge(df_utd.df, lcolumn="RCP_COD", rcolumn="UTD_RCP_COD", how="left", datafactory=False)
        del df_utd

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_NCL")
        df_ncl = BA_NCL(**kwargs)
        df_ncl.df.sort_values(["NCL_UTD_COD", "NCL_DAT_INI"], ignore_index=True, inplace=True)
        df_ncl_red = df_ncl.df.groupby(["NCL_UTD_COD", "NCL_IMPIANTO"]).nth(-1).reset_index()
        self.df = self.merge(
            df_ncl_red,
            lcolumn="UTD_COD",
            rcolumn="NCL_UTD_COD",
            how="left",
            datafactory=False,
        )
        del df_ncl, df_ncl_red

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_VIE")
        df_vie = BA_VIE(**kwargs)
        self.df = self.merge(
            df_vie.df,
            lcolumn="CNT_VIA_COD",
            rcolumn="VIA_COD_SAP",
            how="left",
            datafactory=False,
            silent=kwargs["silent"],
        )
        del df_vie
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["CNT_COD", "UTD_COD", "UTD_DAT_ATT"], ignore_index=True, inplace=True)
        pass


class BA_UTD_MERGED2(BA_UTD):
    """
    BA - UTD MERGED.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        kwargs = {
            "pickled": True,
            "dry": True,
            "pre_process": False,
            "process": False,
            "post_process": False,
            "silent": True,
        }

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_RCP")
        df_rcp = BA_RCP(**kwargs)
        self.df = self.merge(df_rcp.df, lcolumn="UTD_RCP_COD", rcolumn="RCP_COD", how="left", datafactory=False)
        del df_rcp

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_PAG")
        df_pag = BA_PAG(**kwargs)
        df_pag_red = df_pag.df.groupby(["GPART", "PAG_RCP_COD"]).nth(-1).reset_index()
        self.df = self.merge(
            df_pag_red,
            lcolumn="RCP_COD",
            rcolumn="PAG_RCP_COD",
            how="left",
            datafactory=False,
        )
        del df_pag, df_pag_red

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_CNT")
        df_cnt = BA_CNT(**kwargs)
        self.df = self.merge(df_cnt.df, lcolumn="RCP_CNT_COD", rcolumn="CNT_COD", how="left", datafactory=False)
        del df_cnt

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_NCL")
        df_ncl = BA_NCL(**kwargs)
        df_ncl.df.sort_values(["NCL_UTD_COD", "NCL_DAT_INI"], ignore_index=True, inplace=True)
        df_ncl_red = df_ncl.df.groupby(["NCL_UTD_COD", "NCL_IMPIANTO"]).nth(-1).reset_index()
        self.df = self.merge(
            df_ncl_red,
            lcolumn="UTD_COD",
            rcolumn="NCL_UTD_COD",
            how="left",
            datafactory=False,
        )
        del df_ncl, df_ncl_red

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_VIE")
        df_vie = BA_VIE(**kwargs)
        self.df = self.merge(
            df_vie.df,
            lcolumn="CNT_VIA_COD",
            rcolumn="VIA_COD_SAP",
            how="left",
            datafactory=False,
            silent=kwargs["silent"],
        )
        del df_vie
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["CNT_COD", "UTD_COD", "UTD_DAT_ATT"], ignore_index=True, inplace=True)
        pass


class BA_UTN_MERGED(BA_CNT):
    """
    BA - UTN MERGED.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        kwargs = {
            "pickled": True,
            "dry": True,
            "pre_process": False,
            "process": False,
            "post_process": False,
            "silent": True,
        }

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_RCP")
        df_rcp = BA_RCP(**kwargs)
        self.df = self.merge(df_rcp.df, lcolumn="CNT_COD", rcolumn="RCP_CNT_COD", how="left", datafactory=False)
        del df_rcp

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_PAG")
        df_pag = BA_PAG(**kwargs)
        df_pag_red = df_pag.df.groupby(["GPART", "PAG_RCP_COD"]).nth(-1).reset_index()
        self.df = self.merge(
            df_pag_red,
            lcolumn="RCP_COD",
            rcolumn="PAG_RCP_COD",
            how="left",
            datafactory=False,
        )
        del df_pag, df_pag_red

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_UTN")
        df_utd = BA_UTN(**kwargs)
        self.df = self.merge(df_utd.df, lcolumn="RCP_COD", rcolumn="UTN_RCP_COD", how="left", datafactory=False)
        del df_utd

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_VIE")
        df_vie = BA_VIE(**kwargs)
        self.df = self.merge(
            df_vie.df,
            lcolumn="CNT_VIA_COD",
            rcolumn="VIA_COD_SAP",
            how="left",
            datafactory=False,
            silent=kwargs["silent"],
        )
        del df_vie
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["CNT_COD", "UTN_COD", "UTN_DAT_ATT"], ignore_index=True, inplace=True)
        pass


class BA_UTN_MERGED2(BA_UTN):
    """
    BA - UTN MERGED.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def pre_process(self):
        """
        Pre-Process DataFrame.
        """
        kwargs = {
            "pickled": True,
            "dry": True,
            "pre_process": False,
            "process": False,
            "post_process": False,
            "silent": True,
        }

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_RCP")
        df_rcp = BA_RCP(**kwargs)
        self.df = self.merge(df_rcp.df, lcolumn="UTN_RCP_COD", rcolumn="RCP_COD", how="left", datafactory=False)
        del df_rcp

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_PAG")
        df_pag = BA_PAG(**kwargs)
        df_pag_red = df_pag.df.groupby(["GPART", "PAG_RCP_COD"]).nth(-1).reset_index()
        self.df = self.merge(
            df_pag_red,
            lcolumn="RCP_COD",
            rcolumn="PAG_RCP_COD",
            how="left",
            datafactory=False,
        )
        del df_pag, df_pag_red

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_CNT")
        df_cnt = BA_CNT(**kwargs)
        self.df = self.merge(df_cnt.df, lcolumn="RCP_CNT_COD", rcolumn="CNT_COD", how="left", datafactory=False)
        del df_cnt

        if kwargs["silent"]:
            self.logger.info("")
            self.logger.info("BA_VIE")
        df_vie = BA_VIE(**kwargs)
        self.df = self.merge(
            df_vie.df,
            lcolumn="CNT_VIA_COD",
            rcolumn="VIA_COD_SAP",
            how="left",
            datafactory=False,
            silent=kwargs["silent"],
        )
        del df_vie
        pass

    def process(self):
        """
        Process DataFrame.
        """
        self.clean_data(headers=True, empty=True, datetime=True, toup=True, spaces=True, fillna=True)
        pass

    def post_process(self):
        """
        Post-Process DataFrame.
        """
        self.df.sort_values(["CNT_COD", "UTN_COD", "UTN_DAT_ATT"], ignore_index=True, inplace=True)
        pass
