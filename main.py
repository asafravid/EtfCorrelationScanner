#############################################################################
#
# Version 0.0.27 - Author: Asaf Ravid <asaf.rvd@gmail.com>
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


# TODO: ASFAR: 1. Add bi-grams and tri-grams searching - interesting

import shutil
import time
import urllib.request as request
import yfinance       as yf
import csv
import os
import pdf_generator
import itertools

from contextlib import closing


# TODO: ASFAR: Add the highest holder of symbol and bigram in the report table along for nice info

# Start of Run Configuration ###########
SCAN_ETFS                   = False
POST_PROCESS_ETFS           = True
POST_PROCESS_PATH_NEW       = '20210914-031453'
POST_PROCESS_PATH_REF       = '20210907-215545'
CUSTOM_ETF_LIST             = None  # ['QQQ', 'SPY', 'FDIS', 'SMH', 'SOXX']
NUM_REPORTED_ENTRIES        = 35
NUM_REPORTED_BIGRAM_ENTRIES = 77
VERBOSE_LOGS                = 0
# End   of Run Configuration ###########


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


def update_appearances(row, symbol_appearances, symbol_appearances_with_weights, bigrams_appearances, bigrams_appearances_with_weights):
    weight_symbols_to_skip = ['FGXXX', 'C Z1', 'C K1', 'C N1', 'S X1', 'S K1', 'W Z1', 'W K1', 'S N1', 'W N1', 'FGTXX', 'FTIXX', 'DAPXX']
    unified_stocks_pairs   = ['GOOGL', 'GOOG']

    if VERBOSE_LOGS: print('[update_appearances] before processing: row = {}'.format(row))
    for symbol_index in range(g_holding_get_start_index(0), min(g_holding_get_start_index(g_max_holding_index) + g_num_elements_in_holding, len(row)), g_num_elements_in_holding):
        if row[symbol_index] != '':
            if row[symbol_index] in unified_stocks_pairs:
                row[symbol_index] = unified_stocks_pairs[0]

            if row[symbol_index] in symbol_appearances:
                symbol_appearances[row[symbol_index]] += 1
            else:
                symbol_appearances[row[symbol_index]]  = 1

            if row[symbol_index] in weight_symbols_to_skip: continue
            if row[symbol_index] in symbol_appearances_with_weights:
                symbol_appearances_with_weights[row[symbol_index]] += float(row[symbol_index+g_holding_weight_subindex])
            else:
                symbol_appearances_with_weights[row[symbol_index]]  = float(row[symbol_index+g_holding_weight_subindex])
    if VERBOSE_LOGS: print('[update_appearances] after processing:  row = {}'.format(row))

    symbols_for_combinations = row[g_holding_get_start_index(0):min(g_holding_get_start_index(g_max_holding_index) + g_num_elements_in_holding, len(row)):g_num_elements_in_holding]
    symbols_for_combinations = list(set(symbols_for_combinations))  # Compress the row
    for subset in itertools.combinations(symbols_for_combinations, 2):
        if '' in subset: continue
        sorted_subset = tuple(sorted(subset))  # Must sort since otherwise 2 same tupples will appear "differently" like ('AAPL', 'GOOGL') and ('GOOGL', 'APPL')
        if sorted_subset in bigrams_appearances: bigrams_appearances[sorted_subset] += 1
        else:                                    bigrams_appearances[sorted_subset]  = 1

        if (subset[0] in weight_symbols_to_skip) or (subset[1] in weight_symbols_to_skip): continue
        if VERBOSE_LOGS: print('[update_appearances] row = {}'.format(row))
        gram0_index = row[g_holding_get_start_index(0)::].index(subset[0])+g_holding_get_start_index(0)  # Start the search from the 1st symbol, since for instance VNM is an ETF name and also a holding name (weird bu thats the case here - VNM is also a stock name in Vietnam or something)
        gram1_index = row[g_holding_get_start_index(0)::].index(subset[1])+g_holding_get_start_index(0)
        if VERBOSE_LOGS: print('[update_appearances] subset = {}, gram0_index = {}, gram1_index = {}'.format(subset, gram0_index, gram1_index))
        if subset in bigrams_appearances_with_weights: bigrams_appearances_with_weights[subset] += (float(row[gram0_index+g_holding_weight_subindex])+float(row[gram1_index+g_holding_weight_subindex]))
        else:                                          bigrams_appearances_with_weights[subset]  = (float(row[gram0_index+g_holding_weight_subindex])+float(row[gram1_index+g_holding_weight_subindex]))


def calc_weights_and_update_appearances(row, symbol_appearances, symbol_appearances_with_weights, bigrams_appearances, bigrams_appearances_with_weights):
    sum_weights_known   = 0
    sum_weights_unknown = 0
    for symbol_index in range(g_holding_get_start_index(0), min(g_holding_get_start_index(g_max_holding_index) + g_num_elements_in_holding, len(row)), g_num_elements_in_holding):
        if row[symbol_index] == '':
            sum_weights_unknown += float(row[symbol_index+g_holding_weight_subindex])
        else:
            sum_weights_known   += float(row[symbol_index+g_holding_weight_subindex])

    update_appearances(row, symbol_appearances, symbol_appearances_with_weights, bigrams_appearances, bigrams_appearances_with_weights)
    return [sum_weights_known, sum_weights_unknown]


def is_empty_row(row):
    return len(row) < g_holding_get_start_index(0)


def save_stats_db(stats_filename, title_row, stats, sort_by_col, symbol_names_lookup_dict, bigrams):
    rows = []
    for item in stats:
        if bigrams:
            gram0 = symbol_names_lookup_dict[item[0]] if item[0] in symbol_names_lookup_dict else 'Unknown'
            gram1 = symbol_names_lookup_dict[item[1]] if item[1] in symbol_names_lookup_dict else 'Unknown'
            rows.append([item, (gram0,gram1), stats[item]])
        else:
            rows.append([item, symbol_names_lookup_dict[item] if item in symbol_names_lookup_dict else 'Unknown', stats[item]])
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


def post_process_etfs(csv_db_path, date_time_path, csv_db_filename):
    db_rows_filtered_weighted                     = []  # non-empty rows with weight summary
    db_rows_filtered_weighted_non_levereged       = []  # non-empty rows with weight summary without leverage
    symbol_appearances                            = {}
    symbol_appearances_with_weights               = {}
    bigrams_appearances                           = {}
    bigrams_appearances_with_weights              = {}
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

                [sum_weights_known, sum_weights_unknown] = calc_weights_and_update_appearances(row, symbol_appearances, symbol_appearances_with_weights, bigrams_appearances, bigrams_appearances_with_weights)

                row.append(sum_weights_known)
                row.append(sum_weights_unknown)
                db_rows_filtered_weighted.append(row)

                if sum_weights_known+sum_weights_unknown <= 1:
                    db_rows_filtered_weighted_non_levereged.append(row)

                row_index += 1

    db_rows_filtered_weighted_sorted               = sorted(db_rows_filtered_weighted,               key=lambda k: k[len(title_row)], reverse=True)  # Sort by Known Weights
    db_rows_filtered_weighted_non_levereged_sorted = sorted(db_rows_filtered_weighted_non_levereged, key=lambda k: k[len(title_row)], reverse=True)  # Sort by Known Weights

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

    # Appearances_db, appearances_db with weights:
    num_appearances_table         = save_stats_db(stats_filename=csv_db_path+date_time_path+csv_db_filename.replace('.csv', '_num_appearances.csv'),               title_row=['Symbol', 'Name', 'NumAppearances'], stats=symbol_appearances,              sort_by_col=2, symbol_names_lookup_dict=symbol_names_lookup, bigrams=False)
    sum_weights_table             = save_stats_db(stats_filename=csv_db_path+date_time_path+csv_db_filename.replace('.csv', '_sum_weights.csv'),                   title_row=['Symbol', 'Name', 'SumWeights'],     stats=symbol_appearances_with_weights, sort_by_col=2, symbol_names_lookup_dict=symbol_names_lookup, bigrams=False)

    # bigrams_db, bigrams_db with weights:
    num_bigrams_appearances_table = save_stats_db(stats_filename=csv_db_path+date_time_path+csv_db_filename.replace('.csv', '_num_bigrams_appearances.csv'), title_row=['Bigram', 'Name', 'NumAppearances'], stats=bigrams_appearances,                   sort_by_col=2, symbol_names_lookup_dict=symbol_names_lookup, bigrams=True)
    sum_bigrams_weights_table     = save_stats_db(stats_filename=csv_db_path+date_time_path+csv_db_filename.replace('.csv', '_sum_bigrams_weights.csv'    ), title_row=['Bigram', 'Name', 'SumWeights'    ], stats=bigrams_appearances_with_weights,      sort_by_col=2, symbol_names_lookup_dict=symbol_names_lookup, bigrams=True)

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

    diff_num_appearances_table         = add_diff_columns(table_new=num_appearances_table,         table_ref=num_appearances_table_ref,         value_index_in_row=2, bigrams=False)
    diff_sum_weights_table             = add_diff_columns(table_new=sum_weights_table,             table_ref=sum_weights_table_ref,             value_index_in_row=2, bigrams=False)
    diff_num_bigrams_appearances_table = add_diff_columns(table_new=num_bigrams_appearances_table, table_ref=num_bigrams_appearances_table_ref, value_index_in_row=2, bigrams=True )
    diff_sum_bigrams_weights_table     = add_diff_columns(table_new=sum_bigrams_weights_table,     table_ref=sum_bigrams_weights_table_ref,     value_index_in_row=2, bigrams=True )

    pdf_to_append = pdf_generator.csv_to_pdf(report_table=diff_num_appearances_table,         post_process_path_new=csv_db_path + date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='Appearances', reported_column_name='#',      append_to_pdf=None,          output=False, bigrams=False)
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=diff_sum_weights_table,             post_process_path_new=csv_db_path + date_time_path, limit_num_rows=NUM_REPORTED_ENTRIES,        report_title='Weight'     , reported_column_name='Weight', append_to_pdf=pdf_to_append, output=False, bigrams=False)
    pdf_to_append = pdf_generator.csv_to_pdf(report_table=diff_num_bigrams_appearances_table, post_process_path_new=csv_db_path + date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='Appearances', reported_column_name='#',      append_to_pdf=pdf_to_append, output=False, bigrams=True )
    pdf_generator.csv_to_pdf(                report_table=diff_sum_bigrams_weights_table,     post_process_path_new=csv_db_path + date_time_path, limit_num_rows=NUM_REPORTED_BIGRAM_ENTRIES, report_title='Weight',      reported_column_name='Weight', append_to_pdf=pdf_to_append, output=True,  bigrams=True )


if __name__ == '__main__':
    if SCAN_ETFS:         scan_etfs()
    if POST_PROCESS_ETFS: post_process_etfs('Results/', POST_PROCESS_PATH_NEW+'/', 'etfs_db.csv')
