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
    
    filenames = find_files_by_date_n_fundation(date, fundation, rules=rules, dir_abs_path=dir_abs_path) if filename is None else filename

    if filenames is None :
        return []
    
    dir_abs_path = GS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    os.makedirs(dir_abs_path, exist_ok=True)

    dataframes = process_file(date, fundation, filenames, dir_abs_path)
    
    return dataframes



def find_files_by_date_n_fundation (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : str = "HV",

        d_format : str = "%Y%m%d",

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
    rules = GS_STOCKS if rules is None else rules

    full_fundation = get_full_name_fundation(fundation)
    fund_words = [w for w in full_fundation.upper().split() if w]

    filenames = {}

    for rule in rules :

        for entry in os.listdir(dir_abs_path) :

            if date_format in entry and rule in entry : # and fund_words[0] in entry :

                if entry.lower().endswith(extensions) : 

                    print(f"\n[+] [GS] File found for {date} and for {full_fundation} : {entry}")
                    filenames[rule] = os.path.join(dir_abs_path, entry)
          
    return filenames


def process_file (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : str = "HV",

        filenames : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        #schema_overrides : Optional[Dict] = None,
        stocks_sheets : Optional[Dict] = None,
        skip_rows : int = 2

    
    ) -> Optional[pl.DataFrame] :
    """
    Docstring for process_file
    """
    date = str_to_date(date)
    
    dir_abs_path = GS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    #schema_overrides = GS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides
    stocks_sheets = GS_STOCKS_SHEETS if stocks_sheets is None else stocks_sheets

    filenames = find_files_by_date_n_fundation(date, fundation ) if filenames is None else filenames
    
    if filenames is None :
        return None
    
    #full_path = os.path.join(dir_abs_path, filenames)
    #dfs = {stock : [] for stock in stocks_sheets.keys()}
    dfs = []

    for stock, filename in filenames.items() :
        
        sheets = stocks_sheets.get(stock)

        for sheet in sheets :

            try :
                dataframe = pd.read_excel(filename, sheet_name=sheet, skiprows=skip_rows, engine="xlrd", dtype=str) #, usecols=)

                mask_account_nan = dataframe["Account"].isna()

                if mask_account_nan.any() :

                    first_nan_position = mask_account_nan.to_numpy().argmax()
                    dataframe = dataframe.iloc[:first_nan_position]
                
                dataframe = pl.from_pandas(dataframe, include_index=False)  #, schema_overrides=schema_overrides)
                dataframe = dataframe.slice(1)  # drop first row
                
                dfs.append(dataframe)

            except Exception as e :

                print(f"Error processing file for stock {stock} : {e}")
                continue
    
    return dfs



