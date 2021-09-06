#############################################################################
#
# Version 0.0.10 - Author: Asaf Ravid <asaf.rvd@gmail.com>
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
import os
import pdf_generator

from contextlib import closing

class EtfData:
    symbol:            str   = 'None'
    short_name:        str   = 'None'
    sector_weightings: dict  = {}
    holdings:          dict  = {}

g_title_row     = ['Symbol', 'Name', 'Stock0', 'Weight0', 'Stock1', 'Weight1', 'Stock2', 'Weight2', 'Stock3', 'Weight3', 'Stock4', 'Weight4', 'Stock5', 'Weight5', 'Stock6', 'Weight6', 'Stock7', 'Weight7', 'Stock8', 'Weight8', 'Stock9', 'Weight9']
g_stock0_index  = g_title_row.index('Stock0')
g_weight0_index = g_title_row.index('Weight0')
g_weight9_index = g_title_row.index('Weight9')

g_ftp_url               = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/'
g_nasdaq_filenames_list = ['Indices/nasdaqlisted.csv', 'Indices/otherlisted.csv', 'Indices/nasdaqtraded.csv']  # Checkout http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs for all symbol definitions (for instance - `$` in stock names, 5-letter stocks ending with `Y`)
g_ticker_column_list    = [0,                          0,                         1]  # nasdaqtraded.csv - 1st column is Y/N (traded or not) - so take row[1] instead!!!


def pad_row_if_required(row):
    if len(row) < len(g_title_row):
        for index in range(len(row), len(g_title_row),2):
            row.append('')
            row.append(0)



# All nasdaq and others: ftp://ftp.nasdaqtrader.com/symboldirectory/ -> Download automatically
# Legend: http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs
# ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt
# ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt
# ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt
def download_ftp_files():
    for filename in g_nasdaq_filenames_list:
        filename_to_download = filename
        if '/' in filename_to_download:
            filename_to_download = filename[filename.index('/')+1:]
        with closing(request.urlopen(g_ftp_url+filename_to_download.replace('.csv','.txt'))) as read_file:
            with open(filename, 'wb') as file_write:
                shutil.copyfileobj(read_file, file_write)


def extract_etf_list():
    etf_list = []
    for index, filename in enumerate(g_nasdaq_filenames_list):
        with open(filename, mode='r', newline='') as engine:
            reader = csv.reader(engine, delimiter='|')
            etf_column = None
            row_index = 0
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
    sorted_etf_list = sorted(list(set(etf_list)))
    return sorted_etf_list


def scan_etfs():
    if CUSTOM_ETF_LIST != None:
        sorted_etf_list = CUSTOM_ETF_LIST
    else:
        download_ftp_files()
        sorted_etf_list = extract_sorted_etf_list(nasdaq_filenames_list)

    print("Scanning {} ETFs: {}".format(len(sorted_etf_list), sorted_etf_list))

    elapsed_time_start_sec = time.time()

    etf_data_list = []
    for index, etf_name in enumerate(sorted_etf_list):
        etf_data = EtfData()

        elapsed_time_sample_sec = time.time()
        elapsed_time_sec        = round(elapsed_time_sample_sec - elapsed_time_start_sec, 0)
        average_sec_per_symbol  = round(elapsed_time_sec / (index+1),                     2)
        print("#/left/% : {}/{}/{:3.3f}, elapsed/left/avg : {:5}/{:5}/{:4} [sec], Processing {}".format(index+1, len(sorted_etf_list)-index-1, (index+1)/len(sorted_etf_list)*100, elapsed_time_sec, int(round(average_sec_per_symbol*(len(sorted_etf_list)-index-1),0)), average_sec_per_symbol, etf_name))
        symbol = yf.Ticker(etf_name)
        info   = symbol.get_info()
        etf_data.symbol     = etf_name
        if 'sectorWeightings' in info: etf_data.sector_weightings = info["sectorWeightings"]
        if 'shortName'        in info: etf_data.short_name        = info["shortName"]
        if 'holdings'         in info: etf_data.holdings          = info['holdings']
        etf_data_list.append(etf_data)

    rows = [g_title_row]
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
    results_date_and_time_path  = time.strftime("Results/%Y%m%d-%H%M%S/")
    result_db_filename_and_path = results_date_and_time_path + ("{}".format(filename))
    os.makedirs(os.path.dirname(results_date_and_time_path), exist_ok=True)

    with open(result_db_filename_and_path, mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(rows)


def update_appearances(row, symbol_appearances, symbol_appearances_with_weigths):
    for weight_index in range(g_weight0_index, min(g_weight9_index + 1, len(row)), 2):
        if row[weight_index - 1] != '':
            if row[weight_index - 1] in symbol_appearances:
                symbol_appearances[row[weight_index - 1]] += 1
            else:
                symbol_appearances[row[weight_index - 1]]  = 1

            if row[weight_index - 1] in symbol_appearances_with_weigths:
                symbol_appearances_with_weigths[row[weight_index - 1]] += float(row[weight_index])
            else:
                symbol_appearances_with_weigths[row[weight_index - 1]]  = float(row[weight_index])


def calc_weights_and_update_appearances(row, symbol_appearances, symbol_appearances_with_weigths):
    sum_weights_known   = 0
    sum_weights_unknown = 0
    for weight_index in range(g_weight0_index, min(g_weight9_index + 1, len(row)), 2):
        if row[weight_index - 1] == '':
            sum_weights_unknown += float(row[weight_index])
        else:
            sum_weights_known   += float(row[weight_index])

    update_appearances(row, symbol_appearances, symbol_appearances_with_weigths)
    return [sum_weights_known, sum_weights_unknown]


def is_empty_row(row):
    return not (len(row) >= g_weight0_index+1)


def save_stats_db(stats_filename, title_row, stats, sort_by_row):
    rows = []
    for item in stats:
        rows.append([item,stats[item]])
    sorted_rows = sorted(rows, key=lambda row: row[sort_by_row], reverse=True)
    sorted_rows.insert(0,title_row)

    with open(stats_filename, mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(sorted_rows)


def post_process_etfs(csv_db_path, date_time_path, csv_db_filename):
    db_rows_filtered_weighted                     = []  # non-empty rows with weight summary
    db_rows_filtered_weighted_non_levereged       = []  # non-empty rows with weight summary without leverage
    symbol_appearances                            = {}
    symbol_appearances_with_weigths               = {}
    title_row                                     = None

    with open(csv_db_path+date_time_path+csv_db_filename, mode='r', newline='') as engine:
        reader = csv.reader(engine, delimiter=',')
        row_index = 0
        for row in reader:
            if row_index == 0:  # first row is the title
                title_row = row
                if title_row != g_title_row:
                    printf("WARNING: g_title_row != title_row:\n")
                    printf("         title_row   = {}".format(  title_row))
                    printf("         g_title_row = {}".format(g_title_row))
                row_index += 1
                continue
            else:
                if is_empty_row(row):
                    row_index += 1
                    continue
                else:
                    pad_row_if_required(row)

                [sum_weights_known, sum_weights_unknown] = calc_weights_and_update_appearances(row, symbol_appearances, symbol_appearances_with_weigths)

                row.append(sum_weights_known)
                row.append(sum_weights_unknown)
                db_rows_filtered_weighted.append(row)

                if sum_weights_known+sum_weights_unknown <= 1:
                    db_rows_filtered_weighted_non_levereged.append(row)

                row_index += 1

    db_rows_filtered_weighted_sorted               = sorted(db_rows_filtered_weighted,               key=lambda row: row[len(title_row)], reverse=True)  # Sort by Known Weights
    db_rows_filtered_weighted_non_levereged_sorted = sorted(db_rows_filtered_weighted_non_levereged, key=lambda row: row[len(title_row)], reverse=True)  # Sort by Known Weights

    title_row.append('SumWeightsKnown')
    title_row.append('SumWeightsUnknown')
    db_rows_filtered_weighted_sorted.insert(              0, title_row)
    db_rows_filtered_weighted_non_levereged_sorted.insert(0, title_row)

    os.makedirs(os.path.dirname(csv_db_path+date_time_path), exist_ok=True)

    csv_db_filename_filtered_weighted_sorted_by_sum_weights_known = csv_db_path+date_time_path+csv_db_filename.replace('.csv','_filtered_weighted_sorted_by_sum_weights_known.csv')
    with open(csv_db_filename_filtered_weighted_sorted_by_sum_weights_known, mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(db_rows_filtered_weighted_sorted)

    csv_db_filename_filtered_weighted_non_leveraged_sorted_by_sum_weights_known = csv_db_path+date_time_path+csv_db_filename.replace('.csv','_filtered_weighted_non_leveraged_sorted_by_weights_known.csv')
    with open(csv_db_filename_filtered_weighted_non_leveraged_sorted_by_sum_weights_known, mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(db_rows_filtered_weighted_non_levereged_sorted)

    # Appearances_db:
    save_stats_db(csv_db_path+date_time_path+csv_db_filename.replace('.csv','_num_appearances.csv'),               ['Symbol', 'NumAppearances'], symbol_appearances,                            1)

    # Appearances_db with weights:
    save_stats_db(csv_db_path+date_time_path+csv_db_filename.replace('.csv','_sum_weights.csv'),                   ['Symbol', 'SumWeights'],     symbol_appearances_with_weigths,               1)

    # pdf_generator.csv_to_pdf(sorted_by_known_weights_csv_db_filename, appearances_csv_db_filename, appearances_csv_db_filename_with_weights)


SCAN_ETFS         = False
POST_PROCESS_ETFS = True
POST_PROCESS_PATH = '20210906-201422'
CUSTOM_ETF_LIST   = None  # ['QQQ', 'SPY', 'FDIS', 'SMH', 'SOXX']

if __name__ == '__main__':
    if SCAN_ETFS:         scan_etfs()
    if POST_PROCESS_ETFS: post_process_etfs('Results/', POST_PROCESS_PATH+'/', 'etfs_db.csv')



