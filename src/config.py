from __future__ import annotations

import os
import polars as pl

from dotenv import load_dotenv

load_dotenv()

# -------- Application settings and values --------

APPLICATION_ID=os.getenv("APPLICATION_ID")
SECRET_VALUE_ID=os.getenv("SECRET_VALUE_ID")

OBJECT_ID=os.getenv("OBJECT_ID")
TENANT_ID=os.getenv("TENANT_ID")
SECRET_ID=os.getenv("SECRET_ID")


# -------- Permissions + Scopes --------

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPES = ["https://graph.microsoft.com/.default"]
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"


# -------- Shared emails --------

SHARED_MAIL_1=os.getenv("SHARED_MAIL_1")
SHARED_MAIL_2=os.getenv("SHARED_MAIL_2")

SHARED_MAILS = [SHARED_MAIL_1, SHARED_MAIL_2]


# -------- Tech credentials --------

TECH_EMAIL=os.getenv("TECH_EMAIL")
TECH_PASSW=os.getenv("TECH_PASSW")


# -------- Group credentials --------

GROUP_EMAIL=os.getenv("GROUP_EMAIL")


# -------- Fundations --------

FUNDATIONS = {

    "HV" : os.getenv("HV"),
    "WR" : os.getenv("WR")

}


# -------- Email informtion schema --------

EMAIL_COLUMNS = {

    "Id" : pl.Utf8,
    "Subject" : pl.Utf8,
    "From" : pl.Utf8,
    "Received DateTime" : pl.Utf8,#pl.Datetime,
    "Attachments" : pl.Boolean,
    "Shared Email" : pl.Utf8

}



FREQUENCY_DATE_MAP = {

    "Day" : "1d",
    "Week" : "1w",
    "Month" : "1mo",
    "Quarter" : "1q",
    "Year" : "1y"

}


## Counterparties

# MS
MS = {

    "emails": {e.strip() for e in os.getenv("MS_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("MS_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("MS_FILENAMES").split(";") if f.strip()},

}



MS_TABLE_PAGES = {

    "HV" : os.getenv("MS_TABLE_PAGE_HV"),
    "WR" : os.getenv("MS_TABLE_PAGE_WR")

}

MS_FILENAMES = os.getenv("MS_FILENAMES")

MS_ATTACHMENT_DIR_ABS_PATH = os.getenv("MS_ATTACHMENT_DIR_ABS_PATH")


MS_ACCOUNTS = {

    "HV" : os.getenv("MS_ACCOUNT_HV"),
    "WR" : os.getenv("MS_ACCOUNT_WR")

}


# GS
GS = {

    "emails": {e.strip() for e in os.getenv("GS_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("GS_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("GS_FILENAMES").split(";") if f.strip()}

}

GS_REQUIRED_COLUMNS = {

    "GS Entity" : pl.Utf8,
    "Trade Id" : pl.Utf8,
    "Customer Account Nummber" : pl.Int64,
    "Transaction Type" : pl.Utf8,
    "Buy/Sell" : pl.Utf8,
    #"Trade Date" : pl.Date,
    #"Effective Date" : pl.Date,
    "Notional(1)" : pl.Int64,
    "Not1Ccy" : pl.Utf8,
    "Notional(2)" : pl.Int64,
    "Not2Ccy" : pl.Utf8,
    "Notional (USD)" : pl.Int64,
    "Vega Notional" : pl.Float64,
    "Under" : pl.Utf8, # Not sure about the dtype
    "Strike Price" : pl.Float64,
    "Post/Held" : pl.Utf8,
    "Quantity" : pl.Float64,
    "Currency" : pl.Utf8,

}

GS_STOCKS = [os.getenv("GS_FX"), os.getenv("GS_EQ")]

GS_STOCKS_SHEETS = {

    os.getenv("GS_FX") :  [e.strip() for e in os.getenv("GS_FX_SHEETS").split(";") if e.strip()],
    os.getenv("GS_EQ") :  [e.strip() for e in os.getenv("GS_EQ_SHEETS").split(";") if e.strip()]

}

print(GS_STOCKS_SHEETS)

GS_FILENAMES = os.getenv("GS_FILENAMES")
GS_ENTITY = os.getenv("GS_ENTITY")

GS_ACCOUNTS = {

    "HV" : os.getenv("GS_ACCOUNT_HV"),
    "WR" : os.getenv("GS_ACCOUNT_WR")

}

GS_ATTACHMENT_DIR_ABS_PATH = os.getenv("GS_ATTACHMENT_DIR_ABS_PATH")

# SAXO
SAXO = {

    "emails": {e.strip() for e in os.getenv("SAXO_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("SAXO_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("SAXO_FILENAMES").split(";") if f.strip()}

}


SAXO_FILENAMES = os.getenv("SAXO_FILENAMES")
SAXO_ATTACHMENT_DIR_ABS_PATH = os.getenv("SAXO_ATTACHMENT_DIR_ABS_PATH")



# UBS
UBS = {

    "emails": {e.strip() for e in os.getenv("UBS_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("UBS_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("UBS_FILENAMES").split(";") if f.strip()}

}

UBS_REQUIRED_COLUMNS = {

    "Cusip/ISIN" : pl.Utf8,
    "Quantity" : pl.Float64,
    "CCY (Issue)" : pl.Utf8

}


UBS_ATTACHMENT_DIR_ABS_PATH = os.getenv("UBS_ATTACHMENT_DIR_ABS_PATH")

UBS_FILENAMES = os.getenv("UBS_FILENAMES")

# Counterparties
COUNTERPARTIES = {

    "MS" : MS,
    "GS" : GS,
    "SAXO" : SAXO,
    "UBS" : UBS

}

RAW_DIR_ABS_PATH = os.getenv("RAW_DIR_ABS_PATH")
ATTACHMENT_DIR_ABS_PATH = os.getenv("ATTACHMENT_DIR_ABS_PATH")
DATA_DIR_ABS_PATH = os.getenv("DATA_DIR_ABS_PATH")
CACHE_DIR_ABS_PATH = os.getenv("CACHE_DIR_ABS_PATH")


# Forex Pairs
PAIRS = ["EURUSD=X", "EURCHF=X", "EURGBP=X", "EURJPY=X", "EURAUD=X"]