from __future__ import annotations

import os
import polars as pl
import pandas as pd
import datetime as dt

from PyPDF2 import PdfReader
from typing import Optional, Dict, Tuple

from src.config import *
from src.utils import date_to_str, str_to_date, get_full_name_fundation


def gs_trades (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : Optional[str] = None,
        
        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        structure : Optional[Dict] = None,
        schema_overrides : Optional[Dict] = None,

        rules : Optional[Dict] = None,

    ) -> Optional[pl.DataFrame] :
    """
    Docstring for gs_trades
    """
    date = str_to_date(date)
    fundation = "HV" if fundation is None else fundation

    if fundation == "WR" :
        return []
    
    filename = find_files_by_date_n_fundation(date, fundation, rules=rules, dir_abs_path=dir_abs_path) if filename is None else filename

    if filename is None :
        return []
    
    dir_abs_path = GS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    os.makedirs(dir_abs_path, exist_ok=True)

    dataframe = process_file(date, fundation, filename, dir_abs_path)
    
    return [dataframe]



def find_files_by_date_n_fundation (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : str = "HV",

        d_format : str = "%d_%b_%Y",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        extensions : Tuple[str, str] = ("xls", "xlsx")

    ) :
    """
    Docstring for gs_by_fund
    """

    date_obj = str_to_date(date)
    date_format = date_to_str(date, d_format)

    dir_abs_path = GS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    rules = GS_FILENAMES if rules is None else rules

    full_fundation = get_full_name_fundation(fundation)
    fund_words = [w for w in full_fundation.upper().split() if w]

    for entry in os.listdir(dir_abs_path) :

        if date_format in entry and entry.startswith(rules) and fund_words[0] in entry :

            if entry.lower().endswith(extensions) : 

                print(f"\n[+] [GS] File found for {date} and for {full_fundation} : {entry}")
                return entry
            
    return None


def process_file (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : str = "HV",

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        schema_overrides : Optional[Dict] = None,
        skip_rows : int = 9

    
    ) -> Optional[pl.DataFrame] :
    """
    Docstring for process_file
    """
    date = str_to_date(date)
    
    dir_abs_path = GS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = GS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    filename = find_files_by_date_n_fundation(date, fundation ) if filename is None else filename
    
    if filename is None :
        return None
    
    full_path = os.path.join(dir_abs_path, filename)

    dataframe = pd.read_excel(full_path, skiprows=skip_rows, engine="xlrd")
    dataframe = dataframe.dropna(subset=["GS Entity"])

    dataframe = pl.from_pandas(dataframe, schema_overrides=schema_overrides)

    return dataframe



