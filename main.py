#############################################################################
#
# Version 0.0.1 - Author: Asaf Ravid <asaf.rvd@gmail.com>
#
#    ETF Correlation  Scanner - based on yfinance
#    Copyright (C) 2021 Asaf Ravid
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#############################################################################

import shutil
import time
import urllib.request as request
import yfinance       as yf
import csv

from contextlib import closing

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
    # etf_list = ['QQQ', 'SPY', 'FDIS']

    etf_data_list = []
    for index, etf_name in enumerate(etf_list):
        etf_data = EtfData()
        print("{}/{}/%{} Processing {}".format(index+1, len(etf_list)-index-1, (index+1)/len(etf_list)*100 , etf_name))
        symbol = yf.Ticker(etf_name)
        info   = symbol.get_info()
        etf_data.symbol     = etf_name
        if 'sectorWeightings' in info: etf_data.sector_weightings = info["sectorWeightings"]
        if 'shortName'        in info: etf_data.short_name        = info["shortName"]
        if 'holdings'         in info: etf_data.holdings          = info['holdings']
        etf_data_list.append(etf_data)

    title_row = ['Symbol', 'Name', 'Stock0', 'Weight0', 'Stock1', 'Weight1', 'Stock2', 'Weight2', 'Stock3', 'Weight3', 'Stock4', 'Weight4', 'Stock5', 'Weight5', 'Stock6', 'Weight6', 'Stock7', 'Weight7', 'Stock8', 'Weight8', 'Stock9', 'Weight9']
    rows = [title_row]
    for etf_item in etf_data_list:
        row = []
        row.append(etf_item.symbol)
        row.append(etf_item.short_name)
        for key in etf_item.holdings:
            if 'symbol' in key and 'holdingPercent' in key:
                row.append(key['symbol'])
                row.append(key['holdingPercent'])
            else:
                continue
        rows.append(row)

    filename = 'etfs_db.csv'
    date_and_time_result_db_filename_and_path = time.strftime("Results/%Y%m%d-%H%M%S_{}".format(filename))

    with open(date_and_time_result_db_filename_and_path, mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(rows)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    scan_etfs()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
