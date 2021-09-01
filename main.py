# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import shutil
import urllib.request as request
import pandas         as pd
import yfinance       as yf
import csv
import json

from contextlib             import closing

class EtfData:
    symbol:            str   = 'None'
    short_name:        str   = 'None'
    sector_weightings: dict  = {}
    holdings:          dict  = {}


def download_ftp_files(filenames_list, ftp_path):
    for filename in filenames_list:
        filename_to_download = filename
        if '/' in filename_to_download:
            filename_to_download = filename[filename.index('/')+1:]
        with closing(request.urlopen(ftp_path+filename_to_download.replace('.csv','.txt'))) as read_file:
            with open(filename, 'wb') as file_write:
                shutil.copyfileobj(read_file, file_write)


def scan_etfs():
    # All nasdaq and others: ftp://ftp.nasdaqtrader.com/symboldirectory/ -> Download automatically
    # Legend: http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs
    # ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt
    # ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt
    # ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt
    etf_list              = []
    nasdaq_filenames_list = ['Indices/nasdaqlisted.csv', 'Indices/otherlisted.csv', 'Indices/nasdaqtraded.csv']  # Checkout http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs for all symbol definitions (for instance - `$` in stock names, 5-letter stocks ending with `Y`)
    ticker_column_list     = [0,                          0,                         1                         ]  # nasdaqtraded.csv - 1st column is Y/N (traded or not) - so take row[1] instead!!!
    download_ftp_files(nasdaq_filenames_list, 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/')
    for index, filename in enumerate(nasdaq_filenames_list):
        with open(filename, mode='r', newline='') as engine:
            reader     = csv.reader(engine, delimiter='|')
            etf_column = None
            row_index  = 0
            for row in reader:
                if row_index == 0:
                    row_index += 1
                    etf_column = row.index('ETF')
                else:
                    row_index += 1
                    if 'File Creation Time' in row[0]:
                        continue
                    if etf_column >= 0 and row[etf_column] == 'Y':
                        etf_list.append(row[ticker_column_list[index]])
                        continue

    # Debug Mode:
    etf_list = ['QQQ', 'SPY', 'FDIS']

    etf_data_list = []
    for etf_name in etf_list:
        etf_data = EtfData()
        print("Processing {}".format(etf_name))
        symbol = yf.Ticker(etf_name)
        info   = symbol.get_info()
        etf_data.symbol     = etf_name
        etf_data.sector_weightings = info["sectorWeightings"]
        etf_data.short_name = info["shortName"]
        etf_data.holdings   = info['holdings']
        etf_data_list.append(etf_data)

    for etf_item in etf_data_list:
        print(etf_item.symbol, etf_item.short_name, etf_item.holdings)  # simple print

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    scan_etfs()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
