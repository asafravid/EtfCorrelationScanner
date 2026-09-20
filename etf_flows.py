#!/usr/bin/env python3
"""ETF holdings flow report — which underlying stocks ETFs moved into and out of.

Diffs two etfs_db.csv scans produced by main.py and reports, per underlying stock:
  * aggregate weight across all scanned ETFs, and how it changed
  * how many ETFs hold it, and how that changed
  * outright entries (newly in someone's top-10) and exits (dropped out)

    python etf_flows.py --new Results/20260920-081933 --ref Results/20220227-104907
    python etf_flows.py --new <dir> --ref <dir> --top 25 --pdf

READ THIS BEFORE TRADING ON THE OUTPUT
--------------------------------------
Yahoo exposes only each fund's TOP 10 holdings, so this sees a fund's head, not its
book. Two consequences:

1. For a PASSIVE, market-cap-weighted index ETF (QQQ, SOXX, XLE, SPY...) a holding
   entering or leaving the top 10 is almost always a PRICE move, not a decision. The
   manager sold nothing. Aggregate weight falling for a stock across many passive
   funds means the stock underperformed its index peers -- that is the same
   information as the price chart, arriving later.
2. The signal the premise assumes -- "a fund sold a lot of X, someone knows something"
   -- only exists for ACTIVELY managed funds. Separate those before reading anything
   into a move.

A stock can also leave the top 10 because the other nine grew, with its own position
untouched. Treat every number here as "changed rank/weight", never as "shares sold".
"""
import argparse
import csv
import os
from collections import defaultdict

ETF_SYMBOL, ETF_NAME, FIRST_HOLDING = 0, 1, 2
FIELDS_PER_HOLDING = 3


import re

# Money-market / cash-sweep share classes show up as "holdings" with nonsense weights
# (FGXXX aggregated to 2316 across 24 funds). They are where a fund parks cash, not a
# position in a company, and they swamp every ranking. 5-letter tickers ending in XX
# are the standard US money-market convention.
MONEY_MARKET = re.compile(r"^[A-Z]{3,4}XX$")
# Non-US lines: HK numerics (00700, 03690), dotted foreign listings (SE.SI), futures.
NON_US = re.compile(r"(^\d|[.=]|^[A-Z]{1,6}\d{2}$)")


def is_us_equity_like(sym):
    if not sym or MONEY_MARKET.match(sym) or NON_US.search(sym):
        return False
    return True


def load(path, us_only=True):
    """etfs_db.csv -> {etf: (etf_name, {stock: weight})}

    Weights are normalised to fractions. Yahoo is inconsistent: some funds report
    holdingPercent as 0-1, others as 0-100. Anything above 1.0 is treated as a
    percentage and divided, which is why an un-normalised run showed aggregate
    "weights" in the thousands.
    """
    f = os.path.join(path, "etfs_db.csv")
    out = {}
    with open(f, mode="r", newline="", encoding="utf-8", errors="replace") as fh:
        for i, row in enumerate(csv.reader(fh)):
            if i == 0 or len(row) <= FIRST_HOLDING:
                continue
            holdings = {}
            for j in range(FIRST_HOLDING, len(row) - 2, FIELDS_PER_HOLDING):
                sym = (row[j] or "").strip().upper()
                try:
                    w = float(row[j + 2])
                except (ValueError, IndexError):
                    continue
                if not sym:
                    continue
                if us_only and not is_us_equity_like(sym):
                    continue
                if w > 1.0:            # reported as 0-100, not 0-1
                    w = w / 100.0
                if w <= 0 or w > 1.0:  # still implausible -> drop
                    continue
                holdings[sym] = holdings.get(sym, 0.0) + w
            out[row[ETF_SYMBOL].strip().upper()] = (row[ETF_NAME], holdings)
    return out


def aggregate(db):
    weight, count, holders = defaultdict(float), defaultdict(int), defaultdict(list)
    for etf, (_, hold) in db.items():
        for sym, w in hold.items():
            weight[sym] += w
            count[sym] += 1
            holders[sym].append((etf, w))
    return weight, count, holders


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--new", required=True)
    ap.add_argument("--ref", required=True)
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--min-etfs", type=int, default=2,
                    help="ignore stocks held by fewer than this many ETFs in either scan")
    ap.add_argument("--pdf", action="store_true")
    ap.add_argument("--all-symbols", action="store_true",
                    help="keep money-market and non-US lines (default: drop them)")
    a = ap.parse_args()

    dn, dr = load(a.new, not a.all_symbols), load(a.ref, not a.all_symbols)
    common = sorted(set(dn) & set(dr))
    print("=" * 104)
    print("ETF HOLDINGS FLOW REPORT")
    print("=" * 104)
    print("  new : {}  ({} ETFs)".format(a.new, len(dn)))
    print("  ref : {}  ({} ETFs)".format(a.ref, len(dr)))
    print("  comparable ETFs present in both scans: {}".format(len(common)))
    if not common:
        print("\n  No overlap - nothing to diff.")
        return
    dn = {k: dn[k] for k in common}
    dr = {k: dr[k] for k in common}

    # ---- staleness check ---------------------------------------------------
    # Yahoo does not refresh fund holdings on any fixed schedule. Measured on the
    # repo's own 2022-02-06 -> 2022-02-27 pair, QQQ's row is byte-identical: three
    # weeks apart, zero change reported. Any flow reading is worthless without
    # knowing what fraction of funds actually moved, so say so up front.
    changed = [e for e in common if dn[e][1] != dr[e][1]]
    frac = len(changed) / float(len(common)) if common else 0.0
    print("  ETFs whose top-10 changed at all: {} of {} ({:.1%})".format(
        len(changed), len(common), frac))
    if frac < 0.25:
        print("  *** {:.0%} of funds report IDENTICAL top-10s. Yahoo's holdings data is".format(1 - frac))
        print("      refreshed too rarely to read flows over this interval -- the deltas below")
        print("      come from the {} funds that did move, not from the market.".format(len(changed)))

    wn, cn, hn = aggregate(dn)
    wr, cr, _ = aggregate(dr)
    syms = set(wn) | set(wr)

    rows = []
    for s in syms:
        a_w, b_w = wr.get(s, 0.0), wn.get(s, 0.0)
        a_c, b_c = cr.get(s, 0), cn.get(s, 0)
        if max(a_c, b_c) < a.min_etfs:
            continue
        rows.append({"sym": s, "w_ref": a_w, "w_new": b_w, "dw": b_w - a_w,
                     "c_ref": a_c, "c_new": b_c, "dc": b_c - a_c})

    moved_w = [r for r in rows if abs(r["dw"]) > 1e-9]
    moved_c = [r for r in rows if r["dc"] != 0]
    print("  stocks with any weight change: {} of {} tracked".format(len(moved_w), len(rows)))

    def show(title, key, reverse, note="", src=None):
        print("\n" + "-" * 104)
        print(title)
        if note:
            print("  " + note)
        print("-" * 104)
        print("  {:>8s} {:>10s} {:>10s} {:>10s} {:>7s} {:>7s} {:>6s}".format(
            "symbol", "wt ref", "wt new", "delta wt", "#ref", "#new", "d#"))
        pool = rows if src is None else src
        if not pool:
            print("  (no stock changed on this axis)")
            return
        for r in sorted(pool, key=key, reverse=reverse)[:a.top]:
            print("  {:>8s} {:>10.4f} {:>10.4f} {:>+10.4f} {:>7d} {:>7d} {:>+6d}".format(
                r["sym"], r["w_ref"], r["w_new"], r["dw"], r["c_ref"], r["c_new"], r["dc"]))

    show("ACCUMULATED — largest rise in aggregate top-10 weight across the ETFs",
         lambda r: r["dw"], True,
         "rising weight = the stock grew relative to the other top-10 names in those funds",
         src=moved_w)
    show("REDUCED — largest fall in aggregate top-10 weight",
         lambda r: r["dw"], False,
         "for passive funds this is a price move, not a sale",
         src=moved_w)
    show("BROADENED — now in more ETFs' top 10", lambda r: r["dc"], True, src=moved_c)
    show("NARROWED — now in fewer ETFs' top 10", lambda r: r["dc"], False, src=moved_c)

    entries = sorted([r for r in rows if r["c_ref"] == 0 and r["c_new"] > 0],
                     key=lambda r: -r["w_new"])[:a.top]
    exits = sorted([r for r in rows if r["c_new"] == 0 and r["c_ref"] > 0],
                   key=lambda r: -r["w_ref"])[:a.top]
    print("\n" + "-" * 104)
    print("ENTRIES — absent from every top-10 in the reference scan, present now")
    print("-" * 104)
    for r in entries:
        print("  {:>8s}  weight {:.4f} across {} ETFs".format(r["sym"], r["w_new"], r["c_new"]))
    if not entries:
        print("  none")
    print("\n" + "-" * 104)
    print("EXITS — in a top-10 then, in none now")
    print("-" * 104)
    for r in exits:
        print("  {:>8s}  was {:.4f} across {} ETFs".format(r["sym"], r["w_ref"], r["c_ref"]))
    if not exits:
        print("  none")

    print("\n" + "=" * 104)
    print("HOW TO READ THIS")
    print("=" * 104)
    print("  * Yahoo gives only each fund's top 10, so this is the head of the book, not the book.")
    print("  * In a passive cap-weighted ETF, entering/leaving the top 10 is a PRICE move, not a")
    print("    manager decision. Aggregate weight falling across many passive funds tells you the")
    print("    stock underperformed its peers -- the price chart already told you that, sooner.")
    print("  * The 'someone sold, they may know something' reading only applies to ACTIVE funds.")
    print("  * A name can drop out because the other nine grew, with its own position untouched.")

    if a.pdf:
        try:
            from fpdf import FPDF
            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 13)
            pdf.cell(0, 8, "ETF Holdings Flow Report", new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "", 8)
            pdf.cell(0, 5, "new: {}   ref: {}   comparable ETFs: {}".format(
                os.path.basename(a.new.rstrip("/")), os.path.basename(a.ref.rstrip("/")), len(common)),
                new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)
            pdf.set_font("Helvetica", "", 7)
            for line in ("Yahoo exposes only each fund's top 10 holdings.",
                         "For passive cap-weighted ETFs, a top-10 entry/exit is a PRICE move, not a sale.",
                         "The 'a fund sold, they know something' reading applies to ACTIVE funds only.",
                         "A name can drop out because the other nine grew, its own position untouched."):
                pdf.cell(0, 4, "- " + line, new_x="LMARGIN", new_y="NEXT")
            pdf.ln(3)

            def table(title, key, reverse):
                pdf.set_font("Helvetica", "B", 9)
                pdf.cell(0, 6, title, new_x="LMARGIN", new_y="NEXT")
                pdf.set_font("Courier", "", 7)
                pdf.cell(0, 4, "{:>8s} {:>10s} {:>10s} {:>10s} {:>6s} {:>6s}".format(
                    "symbol", "wt ref", "wt new", "delta", "#ref", "#new"),
                    new_x="LMARGIN", new_y="NEXT")
                for r in sorted(moved_w if "weight" in title.lower() else moved_c,
                                key=key, reverse=reverse)[:a.top]:
                    pdf.cell(0, 4, "{:>8s} {:>10.4f} {:>10.4f} {:>+10.4f} {:>6d} {:>6d}".format(
                        r["sym"], r["w_ref"], r["w_new"], r["dw"], r["c_ref"], r["c_new"]),
                        new_x="LMARGIN", new_y="NEXT")
                pdf.ln(2)

            table("Accumulated - largest rise in aggregate top-10 weight", lambda r: r["dw"], True)
            table("Reduced - largest fall in aggregate top-10 weight", lambda r: r["dw"], False)
            table("Broadened - now in more ETF top-10s", lambda r: r["dc"], True)
            table("Narrowed - now in fewer ETF top-10s", lambda r: r["dc"], False)
            out = os.path.join(a.new, "etf_flow_report.pdf")
            pdf.output(out)
            print("\n  PDF written: {}".format(out))
        except Exception as exc:
            print("\n  PDF failed: {}: {}".format(type(exc).__name__, str(exc)[:120]))


if __name__ == "__main__":
    main()
