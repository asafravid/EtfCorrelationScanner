#############################################################################
#
# Version 0.0.41 - Author: Asaf Ravid <asaf.rvd@gmail.com>
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


# TODO: ASFAR: 1. Add tri-grams searching - interesting

import shutil
import time
import urllib.request as request
import yfinance       as yf
import csv
import os
import pdf_generator
import itertools

from enum       import Enum
from contextlib import closing


# TODO: ASFAR: 1. Add the highest holder of symbol and bigram in the report table along for nice info
#              2. Add additional tables with a(n ascendingly) sorted list of the LOWEST entry increase - for weights and for appearances
#              3. Sort by increased percentage of weights

# Start of Run Configuration ###########
SCAN_ETFS                   = False
POST_PROCESS_ETFS           = True
POST_PROCESS_PATH_NEW       = '20211113-180330'
POST_PROCESS_PATH_REF       = '20211113-180330'
CUSTOM_ETF_LIST             = None  # ['QQQ', 'SPY', 'FDIS', 'SMH', 'SOXX']
NUM_REPORTED_ENTRIES        = 42
NUM_REPORTED_BIGRAM_ENTRIES = 77
NUM_HOLDERS_TO_INCLUDE      = 5
VERBOSE_LOGS                = 0
# End   of Run Configuration ###########


class ReportTableColumns(Enum):
    SYMBOL            = 0
    NAME              = 1
    VALUE             = 2
    HOLDERS           = 3
    DIFF_ENTRIES      = 4
    DIFF_VALUE        = 5
    LAST_COLUMN_INDEX = 5

class EtfData:
    symbol:            str   = 'None'
    short_name:        str   = 'None'
    sector_weightings: dict  = {}
    holdings:          dict  = {}


g_title_row               = ['EtfSymbol', 'EtfName', 'Holding0Symbol', 'Holding0Name', 'Holding0Weight', 'Holding1Symbol', 'Holding1Name', 'Holding1Weight', 'Holding2Symbol', 'Holding2Name', 'Holding2Weight', 'Holding3Symbol', 'Holding3Name', 'Holding3Weight', 'Holding4Symbol', 'Holding4Name', 'Holding4Weight', 'Holding5Symbol', 'Holding5Name', 'Holding5Weight', 'Holding6Symbol', 'Holding6Name', 'Holding6Weight', 'Holding7Symbol', 'Holding7Name', 'Holding7Weight', 'Holding8Symbol', 'Holding8Name', 'Holding8Weight', 'Holding9Symbol', 'Holding9Name', 'Holding9Weight']
g_etf_symbol_index        = g_title_row.index('EtfSymbol')
g_etf_name_index          = g_title_row.index('EtfName')

g_max_holding_index       = 9  # 10 Top holdings supported/provided currently
g_num_elements_in_holding = 3  # HoldingSymbolX, HoldingNameX, WeightX
g_holding_symbol_subindex = 0
g_holding_name_subindex   = 1
g_holding_weight_subindex = 2


def g_holding_get_start_index(index):
    if index < 0 or g_max_holding_index < index:
        return -1
    return g_title_row.index('Holding0Symbol')+index*g_num_elements_in_holding


g_ftp_url                                = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/'
g_nasdaq_filenames_list                  = ['Indices/nasdaqlisted.csv', 'Indices/otherlisted.csv', 'Indices/nasdaqtraded.csv']  # Checkout http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs for all symbol definitions (for instance - `$` in stock names, 5-letter stocks ending with `Y`)
g_nasdaq_filenames_symbol_column_list    = [0,                          0,                         1]  # nasdaqtraded.csv - 1st column is Y/N (traded or not) - so take row[1] instead!!!
g_nasdaq_filenames_name_column_list      = [1,                          1,                         2]  # nasdaqtraded.csv - 1st column is Y/N (traded or not) - so take row[1] instead!!!

g_weight_symbols_to_skip = ['FGXXX', 'C Z1', 'C K1', 'C N1', 'S X1', 'S K1', 'W Z1', 'W K1', 'S N1', 'W N1', 'FGTXX', 'FTIXX', 'DAPXX']
g_unified_stocks_pairs = ['GOOGL', 'GOOG', 'TM', '7203']


def pad_row_if_required(row):
    if len(row) < len(g_title_row):
        for index in range(len(row), len(g_title_row), g_num_elements_in_holding):
            row.append('')  # HoldingSymbol
            row.append('')  # HoldingName
            row.append(0)   # Weight


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
        with closing(request.urlopen(g_ftp_url+filename_to_download.replace('.csv', '.txt'))) as read_file:
            with open(filename, 'wb') as file_write:
                shutil.copyfileobj(read_file, file_write)


def extract_sorted_etf_list():
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
                        etf_list.append(row[g_nasdaq_filenames_symbol_column_list[index]])
                        continue
    sorted_etf_list = sorted(list(set(etf_list)))
    return sorted_etf_list


def extract_symbol_lookup_dict(csv_db_path, date_time_path, csv_db_filename):
    symbol_lookup_dict = {}

    # 1st, take ETFs and ETFs Holdings Symbols Names:
    csv_db_filename = csv_db_path+date_time_path+csv_db_filename
    with open(csv_db_filename, mode='r', newline='') as engine:
        reader = csv.reader(engine, delimiter=',')
        row_index = 0
        for row in reader:
            if row_index == 0:
                row_index += 1  # Skip Title
            else:
                row_index += 1
                if row[g_etf_symbol_index] not in symbol_lookup_dict:
                    symbol_lookup_dict[row[g_etf_symbol_index]] = row[g_etf_name_index]
                for symbol_index in range(g_holding_get_start_index(0), min(g_holding_get_start_index(g_max_holding_index) + g_num_elements_in_holding, len(row)), g_num_elements_in_holding):
                    if row[symbol_index] not in symbol_lookup_dict:
                        symbol_lookup_dict[row[symbol_index]] = row[symbol_index+g_holding_name_subindex]

    # Then, take All possible missing symbols names from the Nasdaq Files:
    for index, filename in enumerate(g_nasdaq_filenames_list):
        with open(filename, mode='r', newline='') as engine:
            reader = csv.reader(engine, delimiter='|')
            row_index = 0
            for row in reader:
                if row_index == 0:
                    row_index += 1
                else:
                    row_index += 1
                    if 'File Creation Time' in row[0]:
                        continue
                    if row[g_nasdaq_filenames_symbol_column_list[index]] not in symbol_lookup_dict:
                        symbol_lookup_dict[row[g_nasdaq_filenames_symbol_column_list[index]]] = row[g_nasdaq_filenames_name_column_list[index]]

    return symbol_lookup_dict


def scan_etfs():
    if CUSTOM_ETF_LIST != None:
        sorted_etf_list = CUSTOM_ETF_LIST
    else:
        download_ftp_files()
        sorted_etf_list = extract_sorted_etf_list()

    print("Scanning {} ETFs: {}".format(len(sorted_etf_list), sorted_etf_list))

    elapsed_time_start_sec = time.time()

    etf_data_list = []
    for index, etf_symbol in enumerate(sorted_etf_list):
        etf_data = EtfData()

        elapsed_time_sample_sec = time.time()
        elapsed_time_sec        = round(elapsed_time_sample_sec - elapsed_time_start_sec, 0)
        average_sec_per_symbol  = round(elapsed_time_sec / (index+1),                     2)
        print("#/left/% : {}/{}/{:3.3f}, elapsed/left/avg : {:5}/{:5}/{:4} [sec], Processing {}".format(index+1, len(sorted_etf_list)-index-1, (index+1)/len(sorted_etf_list)*100, elapsed_time_sec, int(round(average_sec_per_symbol*(len(sorted_etf_list)-index-1), 0)), average_sec_per_symbol, etf_symbol))
        symbol = yf.Ticker(etf_symbol)
        info   = symbol.get_info()
        etf_data.symbol     = etf_symbol
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
                row.append(key['holdingName'])
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

    print('Scan Result saved in {}'.format(results_date_and_time_path))


def update_appearances(row, symbols_appearances, symbols_appearances_with_weights, symbols_holders, bigrams_appearances, bigrams_appearances_with_weights, bigrams_holders):
    if VERBOSE_LOGS: print('[update_appearances] before processing: row = {}'.format(row))
    for symbol_index in range(g_holding_get_start_index(0), min(g_holding_get_start_index(g_max_holding_index) + g_num_elements_in_holding, len(row)), g_num_elements_in_holding):
        if row[symbol_index] != '':
            if row[symbol_index] in g_unified_stocks_pairs:
                row[symbol_index] = g_unified_stocks_pairs[int(int(g_unified_stocks_pairs.index(row[symbol_index])/2)*2)]

            if row[symbol_index] in symbols_appearances:
                symbols_appearances[row[symbol_index]] += 1
            else:
                symbols_appearances[row[symbol_index]]  = 1

            symbol_weight = round(float(row[symbol_index + g_holding_weight_subindex]),3)

            if row[symbol_index] in symbols_holders:
                symbols_holders[row[symbol_index]].append(             (row[g_etf_symbol_index], symbol_weight))
            else:
                symbols_holders[row[symbol_index]]                  = [(row[g_etf_symbol_index], symbol_weight)]

            if row[symbol_index] in g_weight_symbols_to_skip: continue

            if row[symbol_index] in symbols_appearances_with_weights:
                symbols_appearances_with_weights[row[symbol_index]] += symbol_weight
            else:
                symbols_appearances_with_weights[row[symbol_index]] = symbol_weight

    if VERBOSE_LOGS: print('[update_appearances] after processing:  row = {}'.format(row))

    symbols_for_combinations = row[g_holding_get_start_index(0):min(g_holding_get_start_index(g_max_holding_index) + g_num_elements_in_holding, len(row)):g_num_elements_in_holding]
    symbols_for_combinations = list(set(symbols_for_combinations))  # Compress the row
    for subset in itertools.combinations(symbols_for_combinations, 2):
        if '' in subset: continue

        if VERBOSE_LOGS: print('[update_appearances] subset: {}'.format(subset))
        sorted_subset = tuple(sorted(subset))  # Must sort since otherwise 2 same tupples will appear "differently" like ('AAPL', 'GOOGL') and ('GOOGL', 'APPL')

        if sorted_subset in bigrams_appearances:
            bigrams_appearances[sorted_subset] += 1
        else:
            bigrams_appearances[sorted_subset]  = 1

        gram0_index   = row[g_holding_get_start_index(0)::].index(sorted_subset[0])+g_holding_get_start_index(0)  # Start the search from the 1st symbol, since for instance VNM is an ETF name and also a holding name (weird bu thats the case here - VNM is also a stock name in Vietnam or something)
        gram1_index   = row[g_holding_get_start_index(0)::].index(sorted_subset[1])+g_holding_get_start_index(0)
        bigram_weight = round((float(row[gram0_index+g_holding_weight_subindex])+float(row[gram1_index+g_holding_weight_subindex])),3)

        if sorted_subset in bigrams_holders:
            bigrams_holders[sorted_subset].append(              (row[g_etf_symbol_index], bigram_weight))
        else:
            bigrams_holders[sorted_subset]                   = [(row[g_etf_symbol_index], bigram_weight)]

        if (sorted_subset[0] in g_weight_symbols_to_skip) or (sorted_subset[1] in g_weight_symbols_to_skip): continue

        if sorted_subset in bigrams_appearances_with_weights:
            bigrams_appearances_with_weights[sorted_subset] += bigram_weight
        else:
            bigrams_appearances_with_weights[sorted_subset]  = bigram_weight


def calc_weights_and_update_appearances(row, symbols_appearances, symbols_appearances_with_weights, symbols_holders, bigrams_appearances, bigrams_appearances_with_weights, bigrams_holders):
    sum_weights_known   = 0
    sum_weights_unknown = 0
    for symbol_index in range(g_holding_get_start_index(0), min(g_holding_get_start_index(g_max_holding_index) + g_num_elements_in_holding, len(row)), g_num_elements_in_holding):
        if row[symbol_index] == '':
            sum_weights_unknown += float(row[symbol_index+g_holding_weight_subindex])
        else:
            sum_weights_known   += float(row[symbol_index+g_holding_weight_subindex])

    update_appearances(row, symbols_appearances, symbols_appearances_with_weights, symbols_holders, bigrams_appearances, bigrams_appearances_with_weights, bigrams_holders)
    return [sum_weights_known, sum_weights_unknown]


def is_empty_row(row):
    return len(row) < g_holding_get_start_index(0)


def save_stats_db(stats_filename, title_row, stats, holders, sort_by_col, symbol_names_lookup_dict, bigrams):
    rows = []
    for item in stats:
        if bigrams:
            gram0 = symbol_names_lookup_dict[item[0]] if item[0] in symbol_names_lookup_dict else 'Unknown'
            gram1 = symbol_names_lookup_dict[item[1]] if item[1] in symbol_names_lookup_dict else 'Unknown'
            rows.append([item, (gram0,gram1), stats[item], holders[item][:NUM_HOLDERS_TO_INCLUDE]])
        else:
            rows.append([item, symbol_names_lookup_dict[item] if item in symbol_names_lookup_dict else 'Unknown', stats[item], holders[item][:NUM_HOLDERS_TO_INCLUDE]])
    sorted_rows = sorted(rows, key=lambda row: row[sort_by_col], reverse=True)
    sorted_rows.insert(0, title_row)

    with open(stats_filename, mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(sorted_rows)
        
    return sorted_rows


def load_stats_db(db_filename):
    read_rows = []
    with open(db_filename, mode='r', newline='') as engine:
        reader = csv.reader(engine, delimiter=',')
        for row in reader:
            read_rows.append(row)

    return read_rows


def add_diff_columns(table_new, table_ref, value_index_in_row, bigrams):
    table_with_diff_columns              = []
    symbol_ref_entry_and_pos_lookup_dict = {}
    key_index                            = 0
    for row_index, row_data in enumerate(table_ref):
        if row_index == 0:
            key_index = row_data.index('Bigram') if bigrams else row_data.index('Symbol')
        else:
            symbol_ref_entry_and_pos_lookup_dict[row_data[key_index]] = [row_index, row_data[value_index_in_row]]  # Entry in table, num appearances

    for row_index, row_data in enumerate(table_new):
        new_row = row_data
        if row_index == 0:  # title
            new_row.append('DiffEntries')  # Entries up/down vs ref
            new_row.append('DiffValue'  )  # Value   up/down vs ref

            key_index = new_row.index('Bigram') if bigrams else new_row.index('Symbol')
        else:
            current_symbol = str(row_data[key_index]) if bigrams else row_data[key_index]
            if current_symbol in symbol_ref_entry_and_pos_lookup_dict:
                diff_entries = symbol_ref_entry_and_pos_lookup_dict[current_symbol][0] - row_index
                diff_value   = row_data[value_index_in_row]                            - float(symbol_ref_entry_and_pos_lookup_dict[current_symbol][1])
            else:
                diff_entries = 'New'
                diff_value   = 'New'
            new_row.append(diff_entries)  # Entries up/down vs ref
            new_row.append(diff_value  )  # Value   up/down vs ref
        table_with_diff_columns.append(new_row)
    return table_with_diff_columns


# Some entries have 'New' In their sorted column of entry, so save those aside to a dedicated file
def sort_and_save_stats_no_lookup(stats_filename, stats, sort_by_col, reverse):
    stats_to_process = stats.copy()
    new_rows     = []
    rows_to_sort = []
    title_row    = stats_to_process[0]

    new_rows.append(title_row)
    del stats_to_process[0]

    for index,row in enumerate(stats_to_process):
        if row[sort_by_col] == 'New':
            new_rows.append(row)
        else:
            rows_to_sort.append(row)

    sorted_rows = sorted(rows_to_sort, key=lambda row: row[sort_by_col], reverse=reverse)
    sorted_rows.insert(0, title_row)

    with open(stats_filename, mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(sorted_rows)

    with open(stats_filename.replace('.csv', '_new.csv'), mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(new_rows)

    return [sorted_rows, new_rows]


def sort_holders_dict(holders_dict):
    for item in holders_dict:
        holders_dict[item].sort(key=lambda holder: holder[1], reverse=True)


def post_process_etfs(csv_db_path, date_time_path, csv_db_filename):
    db_rows_filtered_weighted                     = []  # non-empty rows with weight summary
    db_rows_filtered_weighted_non_levereged       = []  # non-empty rows with weight summary without leverage
    symbols_appearances                           = {}
    symbols_appearances_with_weights              = {}
    symbols_holders                               = {}
    bigrams_appearances                           = {}
    bigrams_appearances_with_weights              = {}
    bigrams_holders                               = {}
    symbol_names_lookup                           = extract_symbol_lookup_dict(csv_db_path, date_time_path, csv_db_filename)
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
                symbol_names_lookup[row[g_etf_symbol_index]] = row[g_etf_name_index]
                if is_empty_row(row):
                    row_index += 1
                    continue
                else:
                    pad_row_if_required(row)

                [sum_weights_known, sum_weights_unknown] = calc_weights_and_update_appearances(row, symbols_appearances, symbols_appearances_with_weights, symbols_holders, bigrams_appearances, bigrams_appearances_with_weights, bigrams_holders)

                row.append(sum_weights_known)
                row.append(sum_weights_unknown)
                db_rows_filtered_weighted.append(row)

                if sum_weights_known+sum_weights_unknown <= 1:
                    db_rows_filtered_weighted_non_levereged.append(row)

                row_index += 1

    db_rows_filtered_weighted_sorted               = sorted(db_rows_filtered_weighted,               key=lambda k: k[len(title_row)], reverse=True)  # Sort by Known Weights
    db_rows_filtered_weighted_non_levereged_sorted = sorted(db_rows_filtered_weighted_non_levereged, key=lambda k: k[len(title_row)], reverse=True)  # Sort by Known Weights

    sort_holders_dict(symbols_holders)
    sort_holders_dict(bigrams_holders)

    title_row.append('SumWeightsKnown')
    title_row.append('SumWeightsUnknown')
    db_rows_filtered_weighted_sorted.insert(              0, title_row)
    db_rows_filtered_weighted_non_levereged_sorted.insert(0, title_row)

    os.makedirs(os.path.dirname(csv_db_path+date_time_path), exist_ok=True)

    csv_db_filename_filtered_weighted_sorted_by_sum_weights_known = csv_db_path+date_time_path+csv_db_filename.replace('.csv', '_filtered_weighted_sorted_by_sum_weights_known.csv')
    with open(csv_db_filename_filtered_weighted_sorted_by_sum_weights_known, mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(db_rows_filtered_weighted_sorted)

    csv_db_filename_filtered_weighted_non_leveraged_sorted_by_sum_weights_known = csv_db_path+date_time_path+csv_db_filename.replace('.csv', '_filtered_weighted_non_leveraged_sorted_by_weights_known.csv')
    with open(csv_db_filename_filtered_weighted_non_leveraged_sorted_by_sum_weights_known, mode='w', newline='') as engine:
        writer = csv.writer(engine)
        writer.writerows(db_rows_filtered_weighted_non_levereged_sorted)

    results_path_date_time_base_filename = csv_db_path+date_time_path+csv_db_filename

    # Appearances_db, appearances_db with weights:
    num_appearances_table         = save_stats_db(stats_filename=results_path_date_time_base_filename.replace('.csv', '_num_appearances.csv'),               title_row=['Symbol', 'Name', 'NumAppearances', 'Holders'], stats=symbols_appearances,              holders=symbols_holders, sort_by_col=ReportTableColumns.VALUE.value, symbol_names_lookup_dict=symbol_names_lookup, bigrams=False)
    sum_weights_table             = save_stats_db(stats_filename=results_path_date_time_base_filename.replace('.csv', '_sum_weights.csv'),                   title_row=['Symbol', 'Name', 'SumWeights',     'Holders'], stats=symbols_appearances_with_weights, holders=symbols_holders, sort_by_col=ReportTableColumns.VALUE.value, symbol_names_lookup_dict=symbol_names_lookup, bigrams=False)

    # bigrams_db, bigrams_db with weights:
    num_bigrams_appearances_table = save_stats_db(stats_filename=results_path_date_time_base_filename.replace('.csv', '_num_bigrams_appearances.csv'), title_row=['Bigram', 'Name', 'NumAppearances', 'Holders'],  stats=bigrams_appearances,                   holders=bigrams_holders, sort_by_col=ReportTableColumns.VALUE.value, symbol_names_lookup_dict=symbol_names_lookup, bigrams=True)
    sum_bigrams_weights_table     = save_stats_db(stats_filename=results_path_date_time_base_filename.replace('.csv', '_sum_bigrams_weights.csv'    ), title_row=['Bigram', 'Name', 'SumWeights',      'Holders'], stats=bigrams_appearances_with_weights,      holders=bigrams_holders, sort_by_col=ReportTableColumns.VALUE.value, symbol_names_lookup_dict=symbol_names_lookup, bigrams=True)

    # Compare the appearances tables with the reference:
    if POST_PROCESS_PATH_REF != None:
        num_appearances_table_ref         = load_stats_db(csv_db_path+POST_PROCESS_PATH_REF+'/'+csv_db_filename.replace('.csv', '_num_appearances.csv'        ))
        sum_weights_table_ref             = load_stats_db(csv_db_path+POST_PROCESS_PATH_REF+'/'+csv_db_filename.replace('.csv', '_sum_weights.csv'            ))
        num_bigrams_appearances_table_ref = load_stats_db(csv_db_path+POST_PROCESS_PATH_REF+'/'+csv_db_filename.replace('.csv', '_num_bigrams_appearances.csv'))
        sum_bigrams_weights_table_ref     = load_stats_db(csv_db_path+POST_PROCESS_PATH_REF+'/'+csv_db_filename.replace('.csv', '_sum_bigrams_weights.csv'    ))
    else:
        num_appearances_table_ref         = num_appearances_table
        sum_weights_table_ref             = sum_weights_table
        num_bigrams_appearances_table_ref = num_bigrams_appearances_table
        sum_bigrams_weights_table_ref     = sum_bigrams_weights_table

    diff_num_appearances_table         = add_diff_columns(table_new=num_appearances_table,         table_ref=num_appearances_table_ref,         value_index_in_row=ReportTableColumns.VALUE.value, bigrams=False)
    diff_sum_weights_table             = add_diff_columns(table_new=sum_weights_table,             table_ref=sum_weights_table_ref,             value_index_in_row=ReportTableColumns.VALUE.value, bigrams=False)
    diff_bigrams_num_appearances_table = add_diff_columns(table_new=num_bigrams_appearances_table, table_ref=num_bigrams_appearances_table_ref, value_index_in_row=ReportTableColumns.VALUE.value, bigrams=True )
    diff_bigrams_sum_weights_table     = add_diff_columns(table_new=sum_bigrams_weights_table,     table_ref=sum_bigrams_weights_table_ref,     value_index_in_row=ReportTableColumns.VALUE.value, bigrams=True )

    [most_increased_appearancs_values_table,   new_most_increased_appearancs_values_table  ] = sort_and_save_stats_no_lookup(stats_filename=results_path_date_time_base_filename.replace('.csv', '_most_increased_appearances_values.csv' ), stats=diff_num_appearances_table, sort_by_col=ReportTableColumns.DIFF_VALUE.value,   reverse=True)
    [most_increased_appearances_entries_table, new_most_increased_appearances_entries_table] = sort_and_save_stats_no_lookup(stats_filename=results_path_date_time_base_filename.replace('.csv', '_most_increased_appearances_entries.csv'), stats=diff_num_appearances_table, sort_by_col=ReportTableColumns.DIFF_ENTRIES.value, reverse=True)
    [most_increased_weights_values_table,      new_most_increased_weights_values_table     ] = sort_and_save_stats_no_lookup(stats_filename=results_path_date_time_base_filename.replace('.csv', '_most_increased_weights_values.csv'     ), stats=diff_sum_weights_table,     sort_by_col=ReportTableColumns.DIFF_VALUE.value,   reverse=True)
    [most_increased_weights_entries_table,     new_most_increased_weights_entries_table    ] = sort_and_save_stats_no_lookup(stats_filename=results_path_date_time_base_filename.replace('.csv', '_most_increased_weights_entries.csv'    ), stats=diff_sum_weights_table,     sort_by_col=ReportTableColumns.DIFF_ENTRIES.value, reverse=True)

    [most_increased_bigrams_appearancs_values_table,   new_most_increased_bigrams_appearancs_values_table   ] = sort_and_save_stats_no_lookup(stats_filename=results_path_date_time_base_filename.replace('.csv', '_most_increased_bigrams_appearances_values.csv' ), stats=diff_bigrams_num_appearances_table, sort_by_col=ReportTableColumns.DIFF_VALUE.value,   reverse=True)
    [most_increased_bigrams_appearances_entries_table, new_most_increased_bigrams_appearances_entries_table ] = sort_and_save_stats_no_lookup(stats_filename=results_path_date_time_base_filename.replace('.csv', '_most_increased_bigrams_appearances_entries.csv'), stats=diff_bigrams_num_appearances_table, sort_by_col=ReportTableColumns.DIFF_ENTRIES.value, reverse=True)
    [most_increased_bigrams_weights_values_table,      new_most_increased_bigrams_weights_values_table      ] = sort_and_save_stats_no_lookup(stats_filename=results_path_date_time_base_filename.replace('.csv', '_most_increased_bigrams_weights_values.csv'     ), stats=diff_bigrams_sum_weights_table,     sort_by_col=ReportTableColumns.DIFF_VALUE.value,   reverse=True)
    [most_increased_bigrams_weights_entries_table,     new_most_increased_bigrams_weights_entries_table     ] = sort_and_save_stats_no_lookup(stats_filename=results_path_date_time_base_filename.replace('.csv', '_most_increased_bigrams_weights_entries.csv'    ), stats=diff_bigrams_sum_weights_table,     sort_by_col=ReportTableColumns.DIFF_ENTRIES.value, reverse=True)

    print('Generating diff_num_appearances_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=diff_num_appearances_table,                           post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='Appearances',                 reported_column_index=ReportTableColumns.VALUE.value,        reported_column_name='#',      append_to_pdf=None,          output=False, bigrams=False)
    print('Generating diff_sum_weights_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=diff_sum_weights_table,                               post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='Weight'     ,                 reported_column_index=ReportTableColumns.VALUE.value,        reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=False)
    print('Generating diff_bigrams_num_appearances_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=diff_bigrams_num_appearances_table,                   post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='Appearances',                 reported_column_index=ReportTableColumns.VALUE.value,        reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=True )
    print('Generating diff_bigrams_sum_weights_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=diff_bigrams_sum_weights_table,                       post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='Weight',                      reported_column_index=ReportTableColumns.VALUE.value,        reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=True )
    print('Generating most_increased_appearancs_values_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=most_increased_appearancs_values_table,               post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='Appearances_Values_Diff',     reported_column_index=ReportTableColumns.DIFF_VALUE.value,   reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=False)
    print('Generating new_most_increased_appearancs_values_table.\n')
    pdf_generator.csv_to_pdf(                report_table=new_most_increased_appearancs_values_table,           post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='New_Appearances_Values_Diff', reported_column_index=ReportTableColumns.DIFF_VALUE.value,   reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=False)
    print('Generating most_increased_appearances_entries_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=most_increased_appearances_entries_table,             post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='Appearances_Entries_Diff',    reported_column_index=ReportTableColumns.DIFF_ENTRIES.value, reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=False)
    print('Generating new_most_increased_appearances_entries_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=new_most_increased_appearances_entries_table,         post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='New_Appearances_Entries_Diff',reported_column_index=ReportTableColumns.DIFF_ENTRIES.value, reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=False)
    print('Generating most_increased_weights_values_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=most_increased_weights_values_table,                  post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='Weight_Values_Diff',          reported_column_index=ReportTableColumns.DIFF_VALUE.value,   reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=False)
    print('Generating new_most_increased_weights_values_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=new_most_increased_weights_values_table,              post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='New_Weight_Values_Diff',      reported_column_index=ReportTableColumns.DIFF_VALUE.value,   reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=False)
    print('Generating most_increased_weights_entries_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=most_increased_weights_entries_table,                 post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='Weight_Entries_Diff',         reported_column_index=ReportTableColumns.DIFF_ENTRIES.value, reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=False)
    print('Generating new_most_increased_weights_entries_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=new_most_increased_weights_entries_table,             post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='New_Weight_Entries_Diff',     reported_column_index=ReportTableColumns.DIFF_ENTRIES.value, reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=False)
    print('Generating most_increased_bigrams_appearancs_values_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=most_increased_bigrams_appearancs_values_table,       post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='Appearances_Values_Diff' ,    reported_column_index=ReportTableColumns.DIFF_VALUE.value,   reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=True )
    print('Generating new_most_increased_bigrams_appearancs_values_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=new_most_increased_bigrams_appearancs_values_table,   post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='New_Appearances_Values_Diff' ,reported_column_index=ReportTableColumns.DIFF_VALUE.value,   reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=True )
    print('Generating most_increased_bigrams_appearances_entries_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=most_increased_bigrams_appearances_entries_table,     post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='Appearances_Entries_Diff',    reported_column_index=ReportTableColumns.DIFF_ENTRIES.value, reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=True )
    print('Generating new_most_increased_bigrams_appearances_entries_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=new_most_increased_bigrams_appearances_entries_table, post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='New_Appearances_Entries_Diff',reported_column_index=ReportTableColumns.DIFF_ENTRIES.value, reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=True )
    print('Generating most_increased_bigrams_weights_values_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=most_increased_bigrams_weights_values_table,          post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='Weight_Values_Diff',          reported_column_index=ReportTableColumns.DIFF_VALUE.value,   reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=True )
    print('Generating new_most_increased_bigrams_weights_values_table.\n')
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=new_most_increased_bigrams_weights_values_table,      post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='New_Weight_Values_Diff',      reported_column_index=ReportTableColumns.DIFF_VALUE.value,   reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=True )
    print('Generating most_increased_bigrams_weights_entries_table.\n')
    pdf_generator.csv_to_pdf(                report_table=most_increased_bigrams_weights_entries_table,         post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='Weight_Entries_Diff',         reported_column_index=ReportTableColumns.DIFF_ENTRIES.value, reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=True )
    print('Generating new_most_increased_bigrams_weights_entries_table.\n')
    pdf_generator.csv_to_pdf(                report_table=new_most_increased_bigrams_weights_entries_table,     post_process_path_new=csv_db_path+date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='New_Weight_Entries_Diff',     reported_column_index=ReportTableColumns.DIFF_ENTRIES.value, reported_column_name='Weight', append_to_pdf=pdf_to_append, output=True , bigrams=True )



if __name__ == '__main__':
    if SCAN_ETFS:         scan_etfs()
    if POST_PROCESS_ETFS: post_process_etfs('Results/', POST_PROCESS_PATH_NEW+'/', 'etfs_db.csv')
