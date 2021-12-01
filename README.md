# Overview
- ETF Correlation Scanner - based on yfinance
- Scan the top holdings of all traded ETFs and all Traded Stocks on the NASDAQ Stock exchange
- Finds, Sorts, Compares (to a reference run) and Presents in a PDF document:
  - Total appearances of each stock in All ETFs
  - Total appearances of each stock-pair combinations (named `bigram`s) in All ETFs
  - Total weigths of each stock in All ETFs
  - Total weigths of each stock-pair in All ETFs
  - New Appearances and their weights
  - Removed Appearances and their weights
  - Highest moves (entries, weights and appearances) up/down compared with reference run

# Prerequesits
- Please use either https://github.com/asafravid/yfinance or apply https://github.com/ranaroussi/yfinance/pull/830 over https://github.com/ranaroussi/yfinance for ETF support (`yfinance` has yet to merge my pull request which upgrades it to support ETF holdings information)

# License
- Copyright (C) 2021 Asaf Ravid

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.


