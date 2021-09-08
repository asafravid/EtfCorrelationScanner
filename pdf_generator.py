#############################################################################
#
# Version 0.0.15 - Author: Asaf Ravid <asaf.rvd@gmail.com>
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


def csv_to_pdf(num_appearances_table, sum_weights_table, post_process_path_new, limit_num_rows):
    title_for_figures = post_process_path_new.replace('/','') + ' ' + '# Appearances' + ']כתב ויתור: תוצאות הסריקה אינן המלצה בשום צורה, אלא אך ורק בסיס למחקר.['[::-1]

    # Read CSV file:
    csv_rows = num_appearances_table

    class MyFPDF(FPDF, HTMLMixin):
        pass

    pdf = MyFPDF(format='letter')
    pdf.add_page()
    # Access DejaVuSansCondensed.ttf on the machine. This font supports practically all languages.
    # Install it via https://fonts2u.com/dejavu-sans-condensed.font
    pdf.add_font('DejaVu', '', 'DejaVuSansCondensed.ttf', uni=True)
    pdf.set_font('DejaVu', '', 7)

    # pdf.set_font("Arial", size=8, style='B')
    pdf.set_text_color(0, 0, 200)  # blue
    pdf.cell(200, 8, txt=title_for_figures, ln=1, align="C")  # http://fpdf.org/en/doc/cell.htm

    names       = []
    appearances = []
    for row_index, row in enumerate(csv_rows):
        if row_index > limit_num_rows: break
        if row_index > 0:  # row 0 is title
            names.append(row[1][0:28])
            appearances.append(int(row[2]))
        if row_index == 0:
            row = ['Symbol', 'Name', '# Appearances', 'Diff Entry', 'Diff Value']
        for col_index, col in enumerate(row):
            if   col_index == 0: w=16 # Symbol
            elif col_index == 1: w=42 # Name
            elif col_index == 2: w=14 # # Appearances
            elif col_index == 3: w=28 # Diff Entry
            elif col_index == 4: w=28 # Diff Value

            pdf.set_text_color(0, 0, 200 if row_index == 0 else 0)  # blue for title and black otherwise
            pdf.cell(w=w, h=4, txt=str(col), border=1, ln=0, align="C" if row_index == 0 else "L")

            if col_index >= 3 and row_index > 0:
                if 'New' in str(row[col_index]): pdf.set_text_color(  0,   0, 200)  # blue
                elif '-' in str(row[col_index]): pdf.set_text_color(200,   0,   0)  # red
                elif '+' in str(row[col_index]): pdf.set_text_color(  0, 200,   0)  # green
                else:                            pdf.set_text_color(  0,   0,   0)  # black
                pdf.cell(w=w, h=4, txt=str(row[col_index]), border=1, ln=1, align="L")
    pdf.cell(200, 4, txt='', ln=1, align="L")
    fig, ax = plt.subplots(figsize=(15, 10))
    y_pos = np.arange(len(names))

    ax.barh(y_pos, appearances, align='center')
    ax.set_yticks(y_pos)
    ax.tick_params(axis='y', labelsize=8)
    ax.set_yticklabels(names)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.set_xlabel('Appearance')
    ax.set_title(title_for_figures, color='blue')

    # plt.show()
    plt.savefig(post_process_path_new+"num_appearances_fig.png")

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
        html_telegram_channel_description     = "<p><img src=""{}"" width=""600"" height=""250""></p>".format(post_process_path_new+"num_appearances_fig.png")
        
        pdf.write_html(text=html_telegram_channel_description)
    else:
        html="<p>Added-Value Channel in Telegram: <A HREF=""https://t.me/investorsIL"">https://t.me/investorsIL</A></p>" \
             "<p>Updates, Discussions and Technical Support on Telegram: <A HREF=""https://t.me/StockScannerIL"">https://t.me/StockScannerIL</A></p>" \
             "<p>This Scanner is Open Source. fork() here: <A HREF=""http://bit.ly/EtfCorrelationScanner"">http://bit.ly/EtfCorrelationScanner</A></p>" \
             "<p>Disclaimer: Scan Results are not recommendations! They only represent a basis for Research and Analysis.</p>" \
             "<p><img src=""{}"" width=""600"" height=""250""></p>".format(post_process_path_new+"num_appearances_fig.png")
        pdf.write_html(text=html)

    output_filename = post_process_path_new+' Results.pdf'
    pdf.output(output_filename, 'F')

