from __future__ import annotations

import os
import polars as pl
import pandas as pd
import datetime as dt

from PyPDF2 import PdfReader
from typing import Optional, Dict, Tuple

from src.config import *
from src.utils import date_to_str, str_to_date, get_full_name_fundation
#from src.parser import *


def gs_trades (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : Optional[str] = None,
        
        fillename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        structure : Optional[Dict] = None,
        schema_overrides : Optional[Dict] = None,

        rules : Optional[Dict] = None,

    ) -> Optional[pl.DataFrame] :
    """
    Docstring for gs_trades
    """

    return None



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

    full_fundation = get_full_name_fundation(fundation).upper()
    fund_words = [w for w in full_fundation.split() if w]

    for entry in os.listdir(dir_abs_path) :

        if date_format in entry and entry.startswith(rules) and fund_words[0] in entry :

            if entry.lower().endswith(extensions) : 

                print(f"\n[+] [GS] File found for {date} and for {full_fundation.lower()} : {entry}")
                return entry
            
    return None