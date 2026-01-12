from __future__ import annotations

import datetime as dt


from src.counterparties.gs import find_files_by_date_n_fundation



def main () :
    """
    Docstring for main
    """
    date = dt.datetime.now().date

    print(date)
    return None



if __name__ == "__main__" :
    """
    """
    main()
    