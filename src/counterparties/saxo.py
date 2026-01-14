from __future__ import annotations

import os
import polars as pl
import pandas as pd
import datetime as dt

from PyPDF2 import PdfReader
from typing import Optional, Dict, Tuple

from src.config import *
from src.utils import date_to_str, str_to_date, get_full_name_fundation


def saxo_trades (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : Optional[str] = None,
        
        filenames : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        rules : Optional[Dict] = None,

    ) -> Optional[list[pl.DataFrame]] :
    """
    Docstring for gs_trades
    """
    date = str_to_date(date)
    fundation = "HV" if fundation is None else fundation

    if fundation == "WR" :
        return []

    rules = SAXO_FILENAMES if rules is None else rules
    
    dir_abs_path = SAXO_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    os.makedirs(dir_abs_path, exist_ok=True)
    
    filenames = find_files_by_date_n_fundation(date, fundation, rules=rules, dir_abs_path=dir_abs_path) if filenames is None else filenames

    if filenames is None or len(filenames) == 0 :
        return []
    
    dataframes = process_file(date, fundation, filenames, dir_abs_path)
    
    return dataframes



def find_files_by_date_n_fundation (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : str = "HV",

        d_format : str = "%d-%m-%Y",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

    ) :
    """
    Docstring for gs_by_fund
    """

    date = date_to_str(date)
    date_format = date_to_str(date, d_format)

    dir_abs_path = SAXO_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    rules = SAXO_FILENAMES if rules is None else rules

    full_fundation = get_full_name_fundation(fundation)

    entries = []
    
    for entry in os.listdir(dir_abs_path) :

        if rules in entry and date_format in entry :

            print(f"\n[+] [SAXO] File found for {date} and for {full_fundation} : {entry}")
            entries.append(entry)
            
    return entries


def process_file (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : str = "HV",

        filenames : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        separator : str = ";",
    
    ) -> Optional[pl.DataFrame] :
    """
    Docstring for process_file
    """
    date = str_to_date(date)
    dir_abs_path = SAXO_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    filenames = find_files_by_date_n_fundation(date, fundation ) if filenames is None else filenames
    
    if filenames is None or len(filenames) == 0 :
        return None
    
    full_paths = [os.path.join(dir_abs_path, filename) for filename in filenames]
    dataframes = []

    for full_path in full_paths :
        
        dataframe = pl.read_csv(full_path, separator=separator)

        if dataframe is None :
            continue
        
        #dataframe = pl.from_pandas(dataframe)
        dataframes.append(dataframe)

    return dataframes




