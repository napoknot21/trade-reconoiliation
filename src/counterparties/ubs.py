from __future__ import annotations

import os
import polars as pl
import pandas as pd
import datetime as dt

from typing import Optional, Dict, Tuple, List

from src.config import *
from src.utils import date_to_str, str_to_date, get_full_name_fundation


def ubs_trades (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : Optional[str] = None,
        
        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        rules : Optional[Dict] = None,

    ) -> List[pl.DataFrame] :
    """
    Docstring for gs_trades
    """
    date = str_to_date(date)
    fundation = "HV" if fundation is None else fundation

    if fundation == "WR" :
        return []
    
    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    os.makedirs(dir_abs_path, exist_ok=True)

    filename = find_files_by_date_n_fundation(date, fundation, rules=rules, dir_abs_path=dir_abs_path) if filename is None else filename

    if filename is None :
        return []
    
    dataframe = process_file(date, fundation, filename, dir_abs_path)
    
    return [dataframe]



def find_files_by_date_n_fundation (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : str = "HV",

        d_format : str = "%b %d, %Y",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        extensions : Tuple[str, str] = ("xls", "xlsx")

    ) :
    """
    Docstring for gs_by_fund
    """

    date = date_to_str(date, d_format)

    rules = UBS_FILENAMES if rules is None else rules
    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    full_fundation = get_full_name_fundation(fundation)

    for entry in os.listdir(dir_abs_path) :

        if entry.lower().endswith(extensions) and rules in entry :

            full_path = os.path.join(dir_abs_path, entry)
            
            out = pl.read_excel(full_path, engine="calamine")

            #if date in entry and rules in entry :
            if get_date_from_file_df(out, date) :

                print(f"\n[+] [UBS] File found for {date} and for {full_fundation} : {entry}")
                return entry
            
    return None


def process_file (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : str = "HV",

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        schema_overrides : Optional[Dict] = None,
        skip_rows : int = 16

    
    ) -> Optional[pl.DataFrame] :
    """
    Docstring for process_file
    """
    date = str_to_date(date)
    
    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = UBS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    filename = find_files_by_date_n_fundation(date, fundation ) if filename is None else filename
    
    if filename is None :
        return None
    
    full_path = os.path.join(dir_abs_path, filename)

    dataframe = pd.read_excel(full_path, skiprows=skip_rows, engine="xlrd")
    dataframe = dataframe.dropna(subset=["Deal Code"])

    dataframe = pl.from_pandas(dataframe)
    dataframe = dataframe[[s.name for s in dataframe if not (s.null_count() == dataframe.height)]]

    return dataframe



def get_date_from_file_df (
        
        df : pl.DataFrame,
        date : Optional[str | dt.datetime | dt.date] = None,
        format : str = "%b %d, %Y",

    ) :
    """
    Docstring for get_date_from_file_df
    
    :param df: Description
    :type df: pl.DataFrame
    :param date: Description
    :type date: Optional[str | dt.datetime | dt.date]
    :param format: Description
    :type format: str
    """
    date = date_to_str(date, format)
    date_formatted = date.replace(" 0", " ")

    colname = df.columns[0]

    # Get first 2 values from first column
    values = df[colname].head(2).to_list()

    for value in values :
        
        if value.endswith(date_formatted) :
            return True
        
    return False