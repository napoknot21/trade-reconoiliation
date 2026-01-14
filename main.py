from __future__ import annotations

import os
import json
import argparse
import polars as pl
import datetime as dt

from typing import Optional, List, Dict

from src.config import FUNDATIONS, COUNTERPARTIES, SHARED_MAILS, EMAIL_COLUMNS, RAW_DIR_ABS_PATH, ATTACHMENT_DIR_ABS_PATH, DATA_DIR_ABS_PATH
from src.msal import get_token, get_inbox_messages_by_date, download_attachments_for_message
from src.utils import str_to_date, date_to_str, generate_dates, previous_business_day, generate_download_dates
from src.extraction import split_by_counterparty
from src.export import export_trade_reconciliation, save_trades_by_date_parquet

from src.counterparties.ubs import ubs_trades
from src.counterparties.gs import gs_trades
from src.counterparties.saxo import saxo_trades
from src.counterparties.ms import ms_trades


COUNTERPARTIES = {

    "GS"    : gs_trades,
    "UBS"   : ubs_trades,
    "SAXO"  : saxo_trades,
    "MS"    : ms_trades,

}



def main (
        
        start_date: Optional[str | dt.datetime] = None,
        end_date: Optional[str | dt.datetime] = None,

        token : Optional[str] = None,
        shared_emails: Optional[List[str]] = None,

        fundations : Optional[List[str]] = None,
        counterparties : Optional[Dict[str]] = None,

        schema_overrides : Optional[Dict] = None,
        yesterday : Optional[bool] = True,

        raw_dir_abs : Optional[str] = None,
        attch_dir_abs : Optional[str] = None,
        data_dir_abs : Optional[str] = None,

    ) :
    """
    Docstring for main
    """

    # Normalize dates
    today = date_to_str(dt.date.today())

    if yesterday :

        start_date = date_to_str(previous_business_day(today))
        end_date = start_date

    else :
        
        start_date = date_to_str(start_date)
        end_date = date_to_str(end_date)

    asked_dates = generate_dates(start_date, end_date)
    asked_dates = [str_to_date(date) for date in asked_dates]

    download_dates = generate_download_dates(asked_dates)

    fundations = FUNDATIONS if fundations is None else [fundations]
    counterparties = COUNTERPARTIES if counterparties is None else counterparties

    raw_dir_abs = RAW_DIR_ABS_PATH if raw_dir_abs is None else raw_dir_abs
    attch_dir_abs = ATTACHMENT_DIR_ABS_PATH if attch_dir_abs is None else attch_dir_abs
    data_dir_abs = DATA_DIR_ABS_PATH if data_dir_abs is None else data_dir_abs

    token = get_token() if token is None else token
    shared_emails = SHARED_MAILS if shared_emails is None else shared_emails
    schema_overrides = EMAIL_COLUMNS if schema_overrides is None else schema_overrides


    # Set of dates for which we already called ensure_inputs_for_date
    for date in download_dates :
        
        print(f"\n[*] Donwloading date : {date_to_str(date)}\n")

        inbox_df = pl.DataFrame(schema=schema_overrides)

        for email in shared_emails :
            
            df = get_inbox_messages_by_date(date, token, email, with_attach=True)

            if isinstance(df, pl.DataFrame) and not df.is_empty() :
                inbox_df = pl.concat([inbox_df, df], how="vertical_relaxed")
        
        if inbox_df.is_empty() :

            print(f"\n[-] No inbox data on {date}.")
            continue
        
        rules_map = split_by_counterparty(inbox_df)

        # Here we will donwload all the files for seleceted dates
        for counterparty, df_cp in rules_map.items() :

            if counterparty == "UNMATCHED" or df_cp.is_empty() :
                continue
            
            os.makedirs(raw_dir_abs, exist_ok=True)
            raw_out = os.path.join(raw_dir_abs, f"{counterparty.lower()}_{date_to_str(date)}.xlsx")

            try :
                df_cp.write_excel(raw_out)
            
            except Exception as e :
                print(f"\n[-] Failed writing {raw_out}: {e}")

            for row in df_cp.to_dicts() :

                msg_id = row.get("Id")
                origin = row.get("Shared Email")

                if not msg_id :
                    continue

                try :

                    dest = os.path.join(attch_dir_abs, counterparty)
                    os.makedirs(dest, exist_ok=True)

                    # avoid re-writing if same file already exists
                    download_attachments_for_message(msg_id, token, dest, origin)

                except Exception as e :
                    print(f"\n[-] Attachment download failed for {counterparty} {date}: {e}")


    # Here we normally have already donwload all email / files 
        
    trades_by_date = {}

    for date in asked_dates :

        trades = {}

        for fundation in fundations :

            trades[fundation] = {

                ctpy : func(date, fundation)
                for ctpy, func in counterparties.items()
            }

        trades_by_date[date] = trades

    
    out_path = export_trade_reconciliation(trades_by_date=trades_by_date, asked_dates=asked_dates, output_dir=data_dir_abs,)
    print(f"\n[+] File saved at {out_path}")

    save_trades_by_date_parquet(trades_by_date, "./")

    return trades_by_date


if __name__ == "__main__" :
    """
    """
    parser = argparse.ArgumentParser(description="Process shared mailboxes")

    parser.add_argument(
        "--shared-emails", nargs="+", required=False, help="List of shared mailboxes to treat"
    )

    parser.add_argument(
        "--yesterday", action="store_true", required=False, help="Gives you the Trades for the previous bussiness day"
    )

    parser.add_argument(
        "--start-date", required=False, help="YYYY-MM-DD or ISO. Default: today 00:00Z"
    )

    parser.add_argument(
        "--end-date", required=False, help="YYYY-MM-DD or ISO. Default: same as start or next day"
    )

    parser.add_argument(
        "--fund", required=False, default=None, help="Fundation name initials."
    )

    args = parser.parse_args()

    main(
        shared_emails=args.shared_emails,
        start_date=args.start_date,
        end_date=args.end_date,
        fundations=args.fund,
        yesterday=args.yesterday
    )
    