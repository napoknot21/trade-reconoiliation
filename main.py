from __future__ import annotations

import argparse
import polars as pl
import datetime as dt

from typing import Optional, List, Dict

from src.config import FUNDATIONS, COUNTERPARTIES, SHARED_MAILS, EMAIL_COLUMNS
from src.msal import get_token, get_inbox_messages_by_date, download_attachments_for_message
from src.utils import str_to_date, date_to_str, generate_dates, previous_business_day, next_business_day
from src.extraction import split_by_counterparty

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

    ) :
    """
    Docstring for main
    """

    # Normalize dates
    today = date_to_str(dt.date.today())

    if yesterday :

        start_date = date_to_str(previous_business_day(today))
        end_date = start_date

        end_date_download = today

    else :
        
        start_date = date_to_str(start_date)
        end_date = date_to_str(end_date)

        if end_date != today :
            end_date_download = next_business_day(end_date)

        else :
            end_date_download = end_date
    
    asked_dates = generate_dates(start_date, end_date)
    asked_dates = [str_to_date(date) for date in asked_dates]

    download_dates = generate_dates(start_date, end_date_download)
    download_dates = [str_to_date(date) for date in download_dates]

    print(asked_dates)
    print(download_dates)

    fundations = FUNDATIONS if fundations is None else [fundations]
    counterparties = COUNTERPARTIES if counterparties is None else counterparties

    token = get_token() if token is None else token
    shared_emails = SHARED_MAILS if shared_emails is None else shared_emails
    schema_overrides = EMAIL_COLUMNS if schema_overrides is None else schema_overrides


    # Set of dates for which we already called ensure_inputs_for_date
    for date in download_dates :

        inbox_df = pl.DataFrame(schema=schema_overrides)

        for email in shared_emails :
            
            df = get_inbox_messages_by_date(date, token, email, with_attach=True)

            if isinstance(df, pl.DataFrame) and not df.is_empty() :
                inbox_df = pl.concat([inbox_df, df], how="vertical_relaxed")
        
        if inbox_df.is_empty() :

            print(f"\n[-] No inbox data on {date}.")
            return
        
        print(inbox_df)
        rules_map = split_by_counterparty(inbox_df)

        print(rules_map)


        # Here we will donwload all the files for seleceted dates
        # TODO


    # Here we normally have already donwload all email / files 
        
    trades = {}

    for date in asked_dates :

        for fundation in fundations :

            trades[fundation] = {

                ctpy : func(date, fundation)
                for ctpy, func in counterparties.items()
            }

    print(trades)


    return None



def args () :
    """
    Docstring for args
    """

    return None



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
    