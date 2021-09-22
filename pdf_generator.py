#############################################################################
#
# Version 0.0.38 - Author: Asaf Ravid <asaf.rvd@gmail.com>
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

#!/usr/bin/env python
# -*- coding: utf8 -*-

from fpdf import FPDF, HTMLMixin

import csv
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
from   main import ReportTableColumns


VERBOSE_LOGS = 0


def csv_to_pdf(report_table, post_process_path_new, limit_num_rows, report_title, reported_column_index, reported_column_name, append_to_pdf, output, bigrams):
    title_for_figures = post_process_path_new.replace('/','') + ' ' + ('Bigrams ' if bigrams else '') + report_title + ' ' + ']כתב ויתור: תוצאות הסריקה אינן המלצה בשום צורה, אלא אך ורק בסיס למחקר.['[::-1]

    csv_rows = report_table

    class MyFPDF(FPDF, HTMLMixin): pass

    if append_to_pdf != None: pdf = append_to_pdf
    else:                     pdf = MyFPDF(format='letter')
    pdf.add_page()
    # Access DejaVuSansCondensed.ttf on the machine. This font supports practically all languages.
    # Install it via https://fonts2u.com/dejavu-sans-condensed.font
    pdf.add_font('DejaVu', '', 'DejaVuSansCondensed.ttf', uni=True)
    pdf.set_font('DejaVu', '', 5)

    pdf.set_text_color(0, 0, 200)  # blue
    pdf.cell(200, 8, txt=title_for_figures, ln=1, align="C")  # http://fpdf.org/en/doc/cell.htm

    names = []
    bars  = []
    for row_index, row in enumerate(csv_rows):
        if row_index > limit_num_rows: break
        if row_index > 0:  # row 0 is title
            if bigrams:
                names_list = list(row[ReportTableColumns.NAME.value])
                for name_index, name in enumerate(names_list): names_list[name_index] = names_list[name_index][0:10]
            names.append(' | '.join(names_list) if bigrams else row[ReportTableColumns.NAME.value][0:28])

            if   reported_column_index == ReportTableColumns.VALUE.value:        bars.append(int(          row[ReportTableColumns.VALUE.value]       ) if reported_column_name == '#' else float(row[ReportTableColumns.VALUE.value]))
            elif reported_column_index == ReportTableColumns.DIFF_ENTRIES.value: bars.append(int(str(      row[ReportTableColumns.DIFF_ENTRIES.value]).replace('+','').replace('-','')))
            elif reported_column_index == ReportTableColumns.DIFF_VALUE.value:   bars.append(int(float(str(row[ReportTableColumns.DIFF_VALUE.value]  ).replace('+','').replace('-',''))) if reported_column_name == '#' else float(str(row[ReportTableColumns.DIFF_VALUE.value]).replace('+','').replace('-','')))
        if row_index == 0:
            row = ['Bigram' if bigrams else 'Symbol', 'Name', reported_column_name, 'Highest % (Exposure) in (Holdings of) ETFs', 'Diff Entry', 'Diff '+reported_column_name]

        if VERBOSE_LOGS: print('[pdf_generator.csv_to_pdf] row({})={}'.format(row_index, row))

        for col_index, col in enumerate(row):
            if   col_index == ReportTableColumns.SYMBOL.value:       w=(28 if bigrams else 14) # Symbol/Bigram
            elif col_index == ReportTableColumns.NAME.value:         w=(70 if bigrams else 56) # Name(s)
            elif col_index == ReportTableColumns.VALUE.value:        w=7  if reported_column_name == '#' else 21  # reported_column_name
            elif col_index == ReportTableColumns.HOLDERS.value:      w=55
            elif col_index == ReportTableColumns.DIFF_ENTRIES.value: w=14                                         # Diff Entry
            elif col_index == ReportTableColumns.DIFF_VALUE.value:   w=14

            pdf.set_text_color(0, 0, 200 if row_index == 0 else 0)  # blue for title and black otherwise

            if (col_index == ReportTableColumns.VALUE.value or col_index == ReportTableColumns.DIFF_VALUE.value) and row_index > 0 and 'New' not in str(col):
                if reported_column_name == '#': col = int(float(str(col) if col_index == ReportTableColumns.DIFF_VALUE.value else int(col)))
                else:                           col = round(float(col), 3)

            row_col_index = row[col_index]
            if col_index >= ReportTableColumns.DIFF_ENTRIES.value and row_index > 0:
                if 'New' not in str(row_col_index) and '+' not in str(row_col_index) and float(row_col_index) > 0:
                    row_col_index = '+{}'.format(row_col_index)
                    col           = '+{}'.format(str(col))  # TODO: ASAFR: -> was col only
                if 'New' in str(row_col_index): pdf.set_text_color(  0,   0, 200)  # blue
                elif '-' in str(row_col_index): pdf.set_text_color(200,   0,   0)  # red
                elif '+' in str(row_col_index): pdf.set_text_color(  0, 200,   0)  # green
                else:                           pdf.set_text_color(  0,   0,   0)  # black

            if   col_index == ReportTableColumns.SYMBOL.value  and row_index and bigrams: col = str(col).replace("('","").replace("', '"," | ").replace("')", "")[:56]
            elif col_index == ReportTableColumns.NAME.value    and row_index and bigrams: col = str(col).replace("('","").replace("', '"," | ").replace("')", "")[:56]
            elif col_index == ReportTableColumns.HOLDERS.value and row_index:             col = str(col).replace("[",'').replace("('",'').replace("', ",':').replace('),',',').replace(")]",'')

            pdf.cell(w=w, h=3, txt=str(col), border=1, ln=0 if col_index < ReportTableColumns.LAST_COLUMN_INDEX.value else 1, align="C" if row_index == 0 else "L")
        if VERBOSE_LOGS: print('\n')
    pdf.cell(200, 4, txt='', ln=1, align="L")
    fig, ax = plt.subplots(figsize=(15, 10))
    y_pos = np.arange(len(names))

    ax.barh(y_pos, bars, align='center')
    ax.set_yticks(y_pos)
    ax.tick_params(axis='y', labelsize=8)
    ax.set_yticklabels(names)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.set_xlabel(reported_column_name+' '+report_title)
    ax.set_title(title_for_figures, color='blue')

    # plt.show()
    plt.savefig(post_process_path_new+report_title+"_fig{}.png".format('_bigrams' if bigrams else ''))

    if bigrams: pdf.add_page()

    tase_mode = False
    if tase_mode:
        telegram_channel_description          = 'ערוץ ערך מוסף'[::-1]
        telegram_discussion_group_description = 'עדכונים, תמיכה טכנית ודיונים'[::-1]
        open_source_description               = 'קוד פתוח'[::-1]

        pdf.set_text_color(0, 0, 200)  # blue
        pdf.cell(30, 4, txt=telegram_channel_description,          ln=0, align="C", border=1)
        pdf.cell(39, 4, txt=telegram_discussion_group_description, ln=0, align="C", border=1)
        pdf.cell(55, 4, txt=open_source_description,               ln=0, align="C", border=1)

        html_telegram_channel_description          = "<A HREF=""https://t.me/investorsIL"">t.me/investorsIL</A><"
        pdf.write_html(text=html_telegram_channel_description)

        html_telegram_discussion_group_description = "   <A HREF=""http://t.me/StockScannerIL"">t.me/StockScannerIL</A>"
        pdf.write_html(text=html_telegram_discussion_group_description)

        html_open_source_description               = " <A HREF=""https://bit.ly/EtfCorrelationScanner"">bit.ly/EtfCorrelationScanner</A>"
        pdf.write_html(text=html_open_source_description)

        pdf.cell(200, 4, txt='', ln=1, align="R")
        html_telegram_channel_description     = "<p><img src=""{}"" width=""600"" height=""250""></p>".format(post_process_path_new+report_title+"_fig.png")
        
        pdf.write_html(text=html_telegram_channel_description)
    else:
        html="<p>Added-Value Channel in Telegram: <A HREF=""https://t.me/investorsIL"">https://t.me/investorsIL</A></p>" \
             "<p>Updates, Discussions and Technical Support on Telegram: <A HREF=""https://t.me/StockScannerIL"">https://t.me/StockScannerIL</A></p>" \
             "<p>This Scanner is Open Source. fork() here: <A HREF=""http://bit.ly/EtfCorrelationScanner"">http://bit.ly/EtfCorrelationScanner</A></p>" \
             "<p>Disclaimer: Scan Results are not recommendations! They only represent a basis for Research and Analysis.</p>" \
             "<p><img src=""{}"" width=""600"" height=""250""></p>".format(post_process_path_new+report_title+"_fig{}.png".format('_bigrams' if bigrams else ''))
        pdf.write_html(text=html)

    output_filename = post_process_path_new+post_process_path_new.replace('/','_')+'combined'+'.pdf'
    if output: pdf.output(output_filename, 'F')
    return pdf
