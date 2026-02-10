# ============================================================
# Factory Status Analyzer — AUTOPILOT PRO (Medica Scientific) ✅
# Single-file Streamlit app (Ctrl+A -> Ctrl+V)
#
# ✅ Snapshot analysis (ครบ/แน่น เหมือนเวอร์ชันแรก)
# ✅ Robust import (alias columns + tolerant missing sheets)
# ✅ Per-user session isolation (แชร์ลิงก์กันได้ ไม่ทับค่า)
# ✅ Full-file trends (timeseries)
# ✅ History parser (ดึง "ราคาที่ตั้งเอง", buy/sell machines, loan, repay, ROP/ROQ ฯลฯ)
# ✅ Pricing intelligence:
#    - Reconstruct Product Price series from History
#    - Measure effect (Product-Market)=PriceDiff on Demand + Deliveries (with lag)
#    - Recommend price (conservative + capacity-aware)
# ✅ Forecast simulator ≥ 100 days (demand->capacity->inventory->cash->debt)
# ✅ Autopilot recommender (Daily actions + Why + Expected impact)
#
# Requirements:
#   streamlit
#   pandas
#   numpy
#   openpyxl
#
# Run:
#   streamlit run app.py
# ============================================================

import io
import re
import math
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st


# ============================================================
# UI config
# ============================================================
st.set_page_config(page_title="Factory Status Analyzer — Autopilot PRO", layout="wide")


# ============================================================
# Constants (from your cheat sheet)
# ============================================================
LEAD_TIME_DAYS = 4.0                     # order day -> arrives on day+4 (as you said, arrives day 5)
RAW_PART_COST = 45.0
RAW_ORDER_FEE = 1500.0
HOLDING_COST_PER_UNIT_PER_DAY = 1.0      # raw + queues
STD_PARTS_PER_UNIT = 2.0
CUS_PARTS_PER_UNIT = 1.0
CUS_LINE_MAX_ORDERS = 450.0
CUS_S2_PASSES_PER_UNIT = 2.0

ROOKIE_TO_EXPERT_DAYS = 15.0
ROOKIE_PRODUCTIVITY = 0.40
SALARY_ROOKIE = 80.0
SALARY_EXPERT = 150.0
OVERTIME_MULT = 1.50

NORMAL_DEBT_APR = 0.365                  # 36.5% per year prorated daily
LOAN_COMMISSION_RATE = 0.02              # 2% commission per loan
CASH_INTEREST_DAILY = 0.0005             # 0.05% daily interest on cash

MACHINE_BUY = {"S1": 18000.0, "S2": 12000.0, "S3": 10000.0}
MACHINE_SELL = {"S1": 8000.0, "S2": 6000.0, "S3": 5000.0}


# ============================================================
# Robust Column Aliases
# ============================================================
COL = {
    "DAY": ["Day", "day", "DAY"],

    # Inventory
    "INV_LEVEL": ["Inventory-Level", "Inventory Level", "Inventory_Level", "Raw Inventory", "Raw Inventory-Level"],

    # Finance
    "CASH": ["Finance-Cash On Hand", "Cash On Hand", "Finance Cash On Hand", "Cash"],
    "DEBT": ["Finance-Debt", "Debt", "Finance Debt"],

    # Workforce
    "ROOKIES": ["WorkForce-Rookies", "Workforce-Rookies", "Rookies", "Work Force-Rookies"],
    "EXPERTS": ["WorkForce-Experts", "Workforce-Experts", "Experts", "Work Force-Experts"],

    # Standard Orders / Deliveries
    "STD_ACCEPT": [
        "Standard Orders-Accepted Orders",
        "Standard Orders - Accepted Orders",
        "Standard Accepted Orders",
        "Standard Accepted",
        "Accepted Orders",
    ],
    "STD_ACCUM": ["Standard Orders-Accumulated Orders", "Standard Accumulated Orders", "Accumulated Orders"],
    "STD_DELIV": ["Standard Deliveries-Deliveries", "Standard Deliveries", "Deliveries", "Deliveries Out"],

    # Standard Price in Standard sheet often only has Market Price (product price NOT here)
    "STD_MKT": ["Standard Deliveries-Market Price", "Market Price", "Standard Market Price"],

    # Standard Queues
    "STD_Q1": ["Standard Queue 1-Level", "Standard Q1-Level", "Queue 1-Level", "Queue1 Level"],
    "STD_Q2": ["Standard Queue 2-Level", "Standard Q2-Level", "Queue 2-Level", "Queue2 Level"],
    "STD_Q3": ["Standard Queue 3-Level", "Standard Q3-Level", "Queue 3-Level", "Queue3 Level"],
    "STD_Q4": ["Standard Queue 4-Level", "Standard Q4-Level", "Queue 4-Level", "Queue4 Level"],
    "STD_Q5": ["Standard Queue 5-Level", "Standard Q5-Level", "Queue 5-Level", "Queue5 Level"],

    # Standard Capacity / Manual
    "STD_S1_MACH": ["Standard Station 1-Number of Machines", "Station 1-Number of Machines", "Number of Machines"],
    "STD_S1_OUT": ["Standard Station 1-Output", "Station 1-Output", "Output"],
    "STD_IB_OUT": ["Standard Initial Batching-Output", "Initial Batching-Output"],
    "STD_MP_OUT": ["Standard Manual Processing-Output", "Manual Processing-Output"],
    "STD_FB_OUT": ["Standard Final Batching-Output", "Final Batching-Output"],
    "STD_EWL": ["Standard Manual Processing-Effective Work Load (%)", "Effective Work Load (%)", "Effective Work Load"],

    # Custom Orders / Deliveries
    "CUS_DEMAND": ["Custom Orders-Demand", "Daily Demand", "Demand", "Custom Demand"],
    "CUS_ACCEPT": ["Custom Orders-Accepted Orders", "Custom Accepted Orders", "Accepted Orders"],
    "CUS_ACCUM": ["Custom Orders-Accumulated Orders", "Custom Accumulated Orders", "Accumulated Orders"],
    "CUS_DELIV": ["Custom Deliveries-Deliveries", "Deliveries", "Deliveries Out"],
    "CUS_LT": ["Custom Deliveries-Average Lead Time", "Average Lead Time", "Lead Time"],
    "CUS_PRICE": ["Custom Deliveries-Actual Price", "Actual Price"],

    # Custom Queues
    "CUS_Q1": ["Custom Queue 1-Level", "Queue 1-Level", "Level", "Queue1 Level"],
    "CUS_Q2_1": ["Custom Queue 2-Level First Pass", "Level First Pass", "Q2 First Pass", "Custom Q2 First Pass"],
    "CUS_Q2_2": ["Custom Queue 2-Level Second Pass", "Level Second Pass", "Q2 Second Pass", "Custom Q2 Second Pass"],
    "CUS_Q3": ["Custom Queue 3-Level", "Queue 3-Level", "Level", "Queue3 Level"],

    # Custom Capacity
    "CUS_S1_OUT": ["Custom Station 1-Output", "Output"],
    "CUS_S2_MACH": ["Custom Station 2-Number of Machines", "Number of Machines"],
    "CUS_S2_OUT_1": ["Custom Station 2-Output First Pass", "Output First Pass"],
    "CUS_S3_MACH": ["Custom Station 3-Number of Machines", "Number of Machines"],
    "CUS_S3_OUT": ["Custom Station 3-Output", "Output"],

    # Finance *To Date for profit proxy
    "FIN_SALES_STD_TD": ["Finance-Sales Standard *To Date", "Finance-Sales Standard To Date", "Sales Standard *To Date"],
    "FIN_SALES_CUS_TD": ["Finance-Sales Custom *To Date", "Finance-Sales Custom To Date", "Sales Custom *To Date"],
    "FIN_SALARIES_TD": ["Finance-Salaries *To Date", "Finance-Salaries To Date", "Salaries *To Date"],
    "FIN_HOLD_RAW_TD": ["Finance-Raw Inventory Holding Costs *To Date", "Raw Inventory Holding Costs *To Date"],
    "FIN_HOLD_CUS_TD": ["Finance-Custom Queues Holding Costs *To Date", "Custom Queues Holding Costs *To Date"],
    "FIN_HOLD_STD_TD": ["Finance-Standard Queues Holding Costs *To Date", "Standard Queues Holding Costs *To Date"],
    "FIN_DEBT_INT_TD": ["Finance-Debt Interest Paid *To Date", "Debt Interest Paid *To Date"],
    "FIN_LOAN_COM_TD": ["Finance-Loan Commission Paid *To Date", "Loan Commission Paid *To Date"],
}


# ============================================================
# Helpers
# ============================================================
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b not in (0, 0.0) else default

def money(x: float) -> str:
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "$0.00"

def num(x: float) -> str:
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return "0.00"

def to_float(x, default=0.0) -> float:
    try:
        if pd.isna(x):
            return float(default)
        return float(x)
    except Exception:
        return float(default)

def excel_file_from_bytes(xbytes: bytes) -> pd.ExcelFile:
    return pd.ExcelFile(io.BytesIO(xbytes))

def read_sheet(xl: pd.ExcelFile, *names: str) -> Optional[pd.DataFrame]:
    for n in names:
        if n in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=n)
            df.columns = [str(c).strip() for c in df.columns]
            return df
    return None

def pick_col(df: Optional[pd.DataFrame], aliases: List[str]) -> Optional[str]:
    if df is None:
        return None
    cols = list(df.columns)
    for a in aliases:
        if a in cols:
            return a
    lower_map = {str(c).lower(): c for c in cols}
    for a in aliases:
        k = str(a).lower()
        if k in lower_map:
            return lower_map[k]
    return None

def as_numeric_series(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

def norm_day(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    dcol = pick_col(df, COL["DAY"])
    if not dcol:
        return None
    out = df.copy()
    out["Day"] = pd.to_numeric(out[dcol], errors="coerce").fillna(-1).astype(int)
    out = out[out["Day"] >= 0].sort_values("Day")
    return out

def latest_day_from(*dfs: Optional[pd.DataFrame]) -> int:
    days = []
    for df in dfs:
        if df is not None and "Day" in df.columns and not df.empty:
            days.append(int(df["Day"].max()))
    return int(max(days)) if days else 0


# ============================================================
# Dataclasses (Snapshot)
# ============================================================
@dataclass
class InventoryInputs:
    inventory_level_parts: float = 0.0
    lead_time_days: float = LEAD_TIME_DAYS
    cost_per_part: float = RAW_PART_COST
    order_fee: float = RAW_ORDER_FEE
    reorder_point: float = 0.0
    reorder_quantity: float = 0.0

@dataclass
class FinancialInputs:
    cash_on_hand: float = 0.0
    debt: float = 0.0
    normal_debt_apr: float = NORMAL_DEBT_APR
    loan_commission_rate: float = LOAN_COMMISSION_RATE
    cash_interest_daily: float = CASH_INTEREST_DAILY

@dataclass
class WorkforceInputs:
    rookies: float = 0.0
    experts: float = 0.0
    days_to_become_expert: float = ROOKIE_TO_EXPERT_DAYS
    rookie_productivity_vs_expert: float = ROOKIE_PRODUCTIVITY
    salary_rookie_per_day: float = SALARY_ROOKIE
    salary_expert_per_day: float = SALARY_EXPERT
    overtime_cost_multiplier: float = OVERTIME_MULT

@dataclass
class StandardLineInputs:
    accepted_orders: float = 0.0
    accumulated_orders: float = 0.0
    deliveries: float = 0.0
    market_price: float = 0.0
    product_price: float = 0.0  # from History (reconstructed)
    # queues
    q1: float = 0.0
    q2: float = 0.0
    q3: float = 0.0
    q4: float = 0.0
    q5: float = 0.0
    # capacity
    s1_machines: float = 0.0
    s1_out: float = 0.0
    ib_out: float = 0.0
    mp_out: float = 0.0
    fb_out: float = 0.0
    ewl: float = 0.0

@dataclass
class CustomLineInputs:
    demand: float = 0.0
    accepted_orders: float = 0.0
    accumulated_orders: float = 0.0
    deliveries: float = 0.0
    avg_lead_time: float = 0.0
    actual_price: float = 0.0
    # queues
    q1: float = 0.0
    q2_first: float = 0.0
    q2_second: float = 0.0
    q3: float = 0.0
    # capacity
    s1_out: float = 0.0
    s2_machines: float = 0.0
    s2_out_first: float = 0.0
    s3_machines: float = 0.0
    s3_out: float = 0.0
    # control
    s2_alloc_first_pct: float = 50.0


# ============================================================
# Per-user session isolation
# ============================================================
def get_sid() -> str:
    if "sid" not in st.session_state:
        st.session_state.sid = str(uuid.uuid4())
    return st.session_state.sid

SID = get_sid()

if "sessions" not in st.session_state:
    st.session_state.sessions = {}

if SID not in st.session_state.sessions:
    st.session_state.sessions[SID] = {
        "last_uploaded_bytes": None,
        "import_day": None,
        "inventory": InventoryInputs(),
        "finance": FinancialInputs(),
        "workforce": WorkforceInputs(),
        "std": StandardLineInputs(),
        "cus": CustomLineInputs(),
        "cache": {},  # store parsed dfs
    }

S = st.session_state.sessions[SID]


# ============================================================
# History Parser (KEY!)
# ============================================================
_history_re_price = re.compile(r"Updated product price to\s*\$(\d+(?:\.\d+)?)", re.IGNORECASE)
_history_re_rop = re.compile(r"Updated reorder point value to\s*(\d+(?:\.\d+)?)\s*units", re.IGNORECASE)
_history_re_roq = re.compile(r"Updated reorder quantity value to\s*(\d+(?:\.\d+)?)\s*units", re.IGNORECASE)
_history_re_buy = re.compile(r"Bought one station\s*([123])\s*machine", re.IGNORECASE)
_history_re_sell = re.compile(r"Sold one station\s*([123])\s*machine", re.IGNORECASE)
_history_re_loan = re.compile(r"Requested loan of\s*\$(\d+(?:\.\d+)?)", re.IGNORECASE)
_history_re_repay = re.compile(r"Paid\s*\$(\d+(?:\.\d+)?)\s*of debt", re.IGNORECASE)
_history_re_alloc_std = re.compile(r"Updated station 1 capacity allocation to standard line % to\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_history_re_alloc_cus_s2 = re.compile(r"Updated station 2 capacity allocation to first pass % to\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_history_re_emp = re.compile(r"Updated the number of required employees to\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_history_re_std_order_size = re.compile(r"Updated value of order size to\s*(\d+(?:\.\d+)?)\s*units", re.IGNORECASE)
_history_re_std_order_freq = re.compile(r"Updated value of order frequency to\s*(\d+(?:\.\d+)?)\s*days", re.IGNORECASE)
_history_re_std_ib = re.compile(r"Updated initial standard batch size to\s*(\d+(?:\.\d+)?)\s*units", re.IGNORECASE)
_history_re_std_fb = re.compile(r"Updated final standard batch size to\s*(\d+(?:\.\d+)?)\s*units", re.IGNORECASE)

def parse_history(history_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Output columns:
      Day, event, value, station, user, description
    """
    if history_df is None or history_df.empty:
        return pd.DataFrame(columns=["Day", "event", "value", "station", "user", "description"])

    df = history_df.copy()
    # Normalize columns
    df.columns = [str(c).strip() for c in df.columns]
    dcol = pick_col(df, COL["DAY"]) or ("Day" if "Day" in df.columns else None)
    if not dcol:
        return pd.DataFrame(columns=["Day", "event", "value", "station", "user", "description"])
    df["Day"] = pd.to_numeric(df[dcol], errors="coerce").fillna(-1).astype(int)
    df = df[df["Day"] >= 0].sort_values("Day")

    user_col = "User" if "User" in df.columns else None
    desc_col = "Description" if "Description" in df.columns else None
    if desc_col is None:
        # try find something similar
        for c in df.columns:
            if str(c).lower() in ("description", "desc", "action"):
                desc_col = c
                break
    if desc_col is None:
        return pd.DataFrame(columns=["Day", "event", "value", "station", "user", "description"])

    rows = []
    for _, r in df.iterrows():
        day = int(r["Day"])
        user = str(r[user_col]) if user_col and not pd.isna(r.get(user_col, "")) else ""
        desc = str(r[desc_col]) if not pd.isna(r.get(desc_col, "")) else ""
        station = ""

        def add(event: str, value: float, station_: str = ""):
            rows.append({
                "Day": day,
                "event": event,
                "value": float(value),
                "station": station_,
                "user": user,
                "description": desc,
            })

        m = _history_re_price.search(desc)
        if m: add("STD_PRODUCT_PRICE", float(m.group(1)))

        m = _history_re_rop.search(desc)
        if m: add("RAW_ROP", float(m.group(1)))

        m = _history_re_roq.search(desc)
        if m: add("RAW_ROQ", float(m.group(1)))

        m = _history_re_buy.search(desc)
        if m:
            s = f"S{m.group(1)}"
            add("BUY_MACHINE", 1.0, s)

        m = _history_re_sell.search(desc)
        if m:
            s = f"S{m.group(1)}"
            add("SELL_MACHINE", 1.0, s)

        m = _history_re_loan.search(desc)
        if m: add("LOAN_TAKE", float(m.group(1)))

        m = _history_re_repay.search(desc)
        if m: add("LOAN_REPAY", float(m.group(1)))

        m = _history_re_alloc_std.search(desc)
        if m: add("ALLOC_S1_TO_STD_PCT", float(m.group(1)))

        m = _history_re_alloc_cus_s2.search(desc)
        if m: add("ALLOC_S2_FIRST_PASS_PCT", float(m.group(1)))

        m = _history_re_emp.search(desc)
        if m: add("EMP_REQUIRED", float(m.group(1)))

        m = _history_re_std_order_size.search(desc)
        if m: add("STD_ORDER_SIZE", float(m.group(1)))

        m = _history_re_std_order_freq.search(desc)
        if m: add("STD_ORDER_FREQ", float(m.group(1)))

        m = _history_re_std_ib.search(desc)
        if m: add("STD_INITIAL_BATCH", float(m.group(1)))

        m = _history_re_std_fb.search(desc)
        if m: add("STD_FINAL_BATCH", float(m.group(1)))

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(columns=["Day", "event", "value", "station", "user", "description"])
    return out


def reconstruct_series_from_events(events: pd.DataFrame, days: pd.Index) -> pd.DataFrame:
    """
    Build step-function daily series from History events.
    Returns columns (all indexed by Day):
      StdProductPrice, RawROP, RawROQ,
      Machines_S1/2/3 (net from buy/sell),
      LoanTake, LoanRepay,
      AllocS1ToStdPct, AllocS2FirstPassPct, EmpRequired,
      StdOrderSize, StdOrderFreq, StdInitialBatch, StdFinalBatch
    """
    idx = pd.Index(sorted(set(int(d) for d in days)))
    out = pd.DataFrame(index=idx)

    def step_fill(event_name: str, col_name: str, default: float = 0.0):
        s = pd.Series(default, index=idx, dtype=float)
        if events is not None and not events.empty:
            e = events[events["event"] == event_name]
            if not e.empty:
                last = None
                for d in idx:
                    ee = e[e["Day"] == d]
                    if not ee.empty:
                        last = float(ee.iloc[-1]["value"])
                    if last is not None:
                        s.loc[d] = last
        out[col_name] = s

    step_fill("STD_PRODUCT_PRICE", "StdProductPrice", default=0.0)
    step_fill("RAW_ROP", "RawROP", default=0.0)
    step_fill("RAW_ROQ", "RawROQ", default=0.0)
    step_fill("ALLOC_S1_TO_STD_PCT", "AllocS1ToStdPct", default=np.nan)
    step_fill("ALLOC_S2_FIRST_PASS_PCT", "AllocS2FirstPassPct", default=np.nan)
    step_fill("EMP_REQUIRED", "EmpRequired", default=np.nan)
    step_fill("STD_ORDER_SIZE", "StdOrderSize", default=np.nan)
    step_fill("STD_ORDER_FREQ", "StdOrderFreq", default=np.nan)
    step_fill("STD_INITIAL_BATCH", "StdInitialBatch", default=np.nan)
    step_fill("STD_FINAL_BATCH", "StdFinalBatch", default=np.nan)

    # Machines: cumulative buys - sells
    for s in ["S1", "S2", "S3"]:
        out[f"Machines_{s}"] = 0.0

    if events is not None and not events.empty:
        ev = events.copy()
        # create per-day net changes
        delta = pd.DataFrame(index=idx, data={f"Machines_{s}": 0.0 for s in ["S1", "S2", "S3"]})
        buys = ev[ev["event"] == "BUY_MACHINE"]
        sells = ev[ev["event"] == "SELL_MACHINE"]
        for _, r in buys.iterrows():
            d = int(r["Day"]); s = r.get("station", "")
            if s in ("S1", "S2", "S3") and d in delta.index:
                delta.loc[d, f"Machines_{s}"] += 1.0
        for _, r in sells.iterrows():
            d = int(r["Day"]); s = r.get("station", "")
            if s in ("S1", "S2", "S3") and d in delta.index:
                delta.loc[d, f"Machines_{s}"] -= 1.0

        # cumulative
        for s in ["S1", "S2", "S3"]:
            out[f"Machines_{s}"] = delta[f"Machines_{s}"].cumsum()

    # Loan flows: cashflow events (not step)
    out["LoanTake"] = 0.0
    out["LoanRepay"] = 0.0
    if events is not None and not events.empty:
        lt = events[events["event"] == "LOAN_TAKE"].groupby("Day")["value"].sum()
        rp = events[events["event"] == "LOAN_REPAY"].groupby("Day")["value"].sum()
        out.loc[out.index.intersection(lt.index), "LoanTake"] = lt.reindex(out.index).fillna(0.0)
        out.loc[out.index.intersection(rp.index), "LoanRepay"] = rp.reindex(out.index).fillna(0.0)

    return out


# ============================================================
# Build Full-file State (merge all sheets + history)
# ============================================================
def build_full_state(xbytes: bytes) -> Dict[str, pd.DataFrame]:
    xl = excel_file_from_bytes(xbytes)
    std = norm_day(read_sheet(xl, "Standard"))
    cus = norm_day(read_sheet(xl, "Custom"))
    inv = norm_day(read_sheet(xl, "Inventory"))
    fin = norm_day(read_sheet(xl, "Finance", "Financial"))
    wf = norm_day(read_sheet(xl, "WorkForce", "Workforce"))
    hist = read_sheet(xl, "History")

    # ensure day index universe
    max_day = latest_day_from(std, cus, inv, fin, wf)
    day_index = pd.Index(range(0, max_day + 1))

    events = parse_history(hist)
    hist_series = reconstruct_series_from_events(events, day_index)

    # base state df
    state = pd.DataFrame(index=day_index)
    state.index.name = "Day"

    # ----- Standard -----
    if std is not None and not std.empty:
        std = std.set_index("Day")
        state["StdAccepted"] = as_numeric_series(std, pick_col(std, COL["STD_ACCEPT"]))
        state["StdAccum"] = as_numeric_series(std, pick_col(std, COL["STD_ACCUM"]))
        state["StdDeliv"] = as_numeric_series(std, pick_col(std, COL["STD_DELIV"]))
        state["StdMktPrice"] = as_numeric_series(std, pick_col(std, COL["STD_MKT"]))
        state["StdQ1"] = as_numeric_series(std, pick_col(std, COL["STD_Q1"]))
        state["StdQ2"] = as_numeric_series(std, pick_col(std, COL["STD_Q2"]))
        state["StdQ3"] = as_numeric_series(std, pick_col(std, COL["STD_Q3"]))
        state["StdQ4"] = as_numeric_series(std, pick_col(std, COL["STD_Q4"]))
        state["StdQ5"] = as_numeric_series(std, pick_col(std, COL["STD_Q5"]))
        state["StdS1Machines"] = as_numeric_series(std, pick_col(std, COL["STD_S1_MACH"]))
        state["StdS1Out"] = as_numeric_series(std, pick_col(std, COL["STD_S1_OUT"]))
        state["StdIBOut"] = as_numeric_series(std, pick_col(std, COL["STD_IB_OUT"]))
        state["StdMPOut"] = as_numeric_series(std, pick_col(std, COL["STD_MP_OUT"]))
        state["StdFBOut"] = as_numeric_series(std, pick_col(std, COL["STD_FB_OUT"]))
        state["StdEWL"] = as_numeric_series(std, pick_col(std, COL["STD_EWL"]))
    else:
        # defaults
        for c in ["StdAccepted","StdAccum","StdDeliv","StdMktPrice","StdQ1","StdQ2","StdQ3","StdQ4","StdQ5",
                  "StdS1Machines","StdS1Out","StdIBOut","StdMPOut","StdFBOut","StdEWL"]:
            state[c] = 0.0

    # ----- Custom -----
    if cus is not None and not cus.empty:
        cus = cus.set_index("Day")
        state["CusDemand"] = as_numeric_series(cus, pick_col(cus, COL["CUS_DEMAND"]))
        state["CusAccepted"] = as_numeric_series(cus, pick_col(cus, COL["CUS_ACCEPT"]))
        state["CusAccum"] = as_numeric_series(cus, pick_col(cus, COL["CUS_ACCUM"]))
        state["CusDeliv"] = as_numeric_series(cus, pick_col(cus, COL["CUS_DELIV"]))
        state["CusLeadTime"] = as_numeric_series(cus, pick_col(cus, COL["CUS_LT"]))
        state["CusPrice"] = as_numeric_series(cus, pick_col(cus, COL["CUS_PRICE"]))
        state["CusQ1"] = as_numeric_series(cus, pick_col(cus, COL["CUS_Q1"]))
        state["CusQ2First"] = as_numeric_series(cus, pick_col(cus, COL["CUS_Q2_1"]))
        state["CusQ2Second"] = as_numeric_series(cus, pick_col(cus, COL["CUS_Q2_2"]))
        state["CusQ3"] = as_numeric_series(cus, pick_col(cus, COL["CUS_Q3"]))
        state["CusS1Out"] = as_numeric_series(cus, pick_col(cus, COL["CUS_S1_OUT"]))
        state["CusS2Machines"] = as_numeric_series(cus, pick_col(cus, COL["CUS_S2_MACH"]))
        state["CusS2OutFirst"] = as_numeric_series(cus, pick_col(cus, COL["CUS_S2_OUT_1"]))
        state["CusS3Machines"] = as_numeric_series(cus, pick_col(cus, COL["CUS_S3_MACH"]))
        state["CusS3Out"] = as_numeric_series(cus, pick_col(cus, COL["CUS_S3_OUT"]))
    else:
        for c in ["CusDemand","CusAccepted","CusAccum","CusDeliv","CusLeadTime","CusPrice",
                  "CusQ1","CusQ2First","CusQ2Second","CusQ3","CusS1Out","CusS2Machines","CusS2OutFirst","CusS3Machines","CusS3Out"]:
            state[c] = 0.0

    # ----- Inventory -----
    if inv is not None and not inv.empty:
        inv = inv.set_index("Day")
        state["RawInv"] = as_numeric_series(inv, pick_col(inv, COL["INV_LEVEL"]))
    else:
        state["RawInv"] = 0.0

    # ----- Finance -----
    if fin is not None and not fin.empty:
        fin = fin.set_index("Day")
        state["Cash"] = as_numeric_series(fin, pick_col(fin, COL["CASH"]))
        state["Debt"] = as_numeric_series(fin, pick_col(fin, COL["DEBT"]))

        # To-date (profit proxy)
        def td(col_alias):
            return as_numeric_series(fin, pick_col(fin, COL[col_alias]))

        sales_td = td("FIN_SALES_STD_TD") + td("FIN_SALES_CUS_TD")
        cost_td = td("FIN_SALARIES_TD") + td("FIN_HOLD_RAW_TD") + td("FIN_HOLD_CUS_TD") + td("FIN_HOLD_STD_TD") + td("FIN_DEBT_INT_TD") + td("FIN_LOAN_COM_TD")
        state["SalesPerDay_proxy"] = sales_td.reindex(state.index).fillna(method="ffill").fillna(0.0).diff().fillna(0.0)
        state["CostPerDay_proxy"] = cost_td.reindex(state.index).fillna(method="ffill").fillna(0.0).diff().fillna(0.0)
        state["ProfitPerDay_proxy"] = state["SalesPerDay_proxy"] - state["CostPerDay_proxy"]
    else:
        state["Cash"] = 0.0
        state["Debt"] = 0.0
        state["SalesPerDay_proxy"] = 0.0
        state["CostPerDay_proxy"] = 0.0
        state["ProfitPerDay_proxy"] = 0.0

    # ----- Workforce -----
    if wf is not None and not wf.empty:
        wf = wf.set_index("Day")
        state["Rookies"] = as_numeric_series(wf, pick_col(wf, COL["ROOKIES"]))
        state["Experts"] = as_numeric_series(wf, pick_col(wf, COL["EXPERTS"]))
    else:
        state["Rookies"] = 0.0
        state["Experts"] = 0.0

    # ----- History reconstructed series (merge) -----
    hist_series = hist_series.reindex(state.index).fillna(0.0)
    state["StdProductPrice"] = hist_series["StdProductPrice"].replace(0.0, np.nan)
    # If never set in history -> use market price as fallback (neutral)
    state["StdProductPrice"] = state["StdProductPrice"].fillna(state["StdMktPrice"]).fillna(0.0)

    state["RawROP_hist"] = hist_series["RawROP"]
    state["RawROQ_hist"] = hist_series["RawROQ"]

    state["HistMachines_S1"] = hist_series["Machines_S1"]
    state["HistMachines_S2"] = hist_series["Machines_S2"]
    state["HistMachines_S3"] = hist_series["Machines_S3"]
    state["LoanTake"] = hist_series["LoanTake"]
    state["LoanRepay"] = hist_series["LoanRepay"]
    state["AllocS1ToStdPct_hist"] = hist_series["AllocS1ToStdPct"]
    state["AllocS2FirstPassPct_hist"] = hist_series["AllocS2FirstPassPct"]
    state["EmpRequired_hist"] = hist_series["EmpRequired"]
    state["StdOrderSize_hist"] = hist_series["StdOrderSize"]
    state["StdOrderFreq_hist"] = hist_series["StdOrderFreq"]
    state["StdInitialBatch_hist"] = hist_series["StdInitialBatch"]
    state["StdFinalBatch_hist"] = hist_series["StdFinalBatch"]

    # Derived
    state["StdBacklogProxy"] = (state["StdAccum"] - state["StdDeliv"]).clip(lower=0.0)
    state["CusBacklogProxy"] = (state["CusAccum"] - state["CusDeliv"]).clip(lower=0.0)

    state["StdWIPProxy"] = (state["StdQ1"] + state["StdQ2"] + state["StdQ3"] + state["StdQ4"] + state["StdQ5"]).clip(lower=0.0)
    state["CusWIPProxy"] = (state["CusQ1"] + state["CusQ2First"] + state["CusQ2Second"] + state["CusQ3"]).clip(lower=0.0)

    state["StdDemandProxy"] = state["StdAccepted"].clip(lower=0.0)  # best available
    state["CusDemandProxy"] = state["CusDemand"].clip(lower=0.0)

    state["StdFillRateProxy"] = (state["StdDeliv"] / state["StdDemandProxy"].replace(0, np.nan)).fillna(1.0).clip(0.0, 2.0)
    state["CusFillRateProxy"] = (state["CusDeliv"] / state["CusDemandProxy"].replace(0, np.nan)).fillna(1.0).clip(0.0, 2.0)

    state["StdPriceDiff"] = state["StdProductPrice"] - state["StdMktPrice"]

    # Some sanity flags
    state["CapacityWarning_std"] = ((state["StdEWL"] >= 95) | (state["StdFillRateProxy"] < 0.98) | (state["StdBacklogProxy"] > 0)).astype(int)

    return {
        "state": state,
        "events": events,
    }


# ============================================================
# Snapshot loader (from full state day)
# ============================================================
def load_snapshot_from_state(state: pd.DataFrame, day: int) -> Tuple[InventoryInputs, FinancialInputs, WorkforceInputs, StandardLineInputs, CustomLineInputs]:
    day = int(day)
    if day not in state.index:
        day = int(state.index.max()) if len(state.index) else 0
    r = state.loc[day]

    inv = InventoryInputs(
        inventory_level_parts=float(r.get("RawInv", 0.0)),
        lead_time_days=LEAD_TIME_DAYS,
        cost_per_part=RAW_PART_COST,
        order_fee=RAW_ORDER_FEE,
        reorder_point=float(r.get("RawROP_hist", 0.0)),
        reorder_quantity=float(r.get("RawROQ_hist", 0.0)),
    )
    fin = FinancialInputs(
        cash_on_hand=float(r.get("Cash", 0.0)),
        debt=float(r.get("Debt", 0.0)),
        normal_debt_apr=NORMAL_DEBT_APR,
        loan_commission_rate=LOAN_COMMISSION_RATE,
        cash_interest_daily=CASH_INTEREST_DAILY,
    )
    wf = WorkforceInputs(
        rookies=float(r.get("Rookies", 0.0)),
        experts=float(r.get("Experts", 0.0)),
    )
    std = StandardLineInputs(
        accepted_orders=float(r.get("StdAccepted", 0.0)),
        accumulated_orders=float(r.get("StdAccum", 0.0)),
        deliveries=float(r.get("StdDeliv", 0.0)),
        market_price=float(r.get("StdMktPrice", 0.0)),
        product_price=float(r.get("StdProductPrice", 0.0)),
        q1=float(r.get("StdQ1", 0.0)),
        q2=float(r.get("StdQ2", 0.0)),
        q3=float(r.get("StdQ3", 0.0)),
        q4=float(r.get("StdQ4", 0.0)),
        q5=float(r.get("StdQ5", 0.0)),
        s1_machines=float(r.get("StdS1Machines", 0.0)),
        s1_out=float(r.get("StdS1Out", 0.0)),
        ib_out=float(r.get("StdIBOut", 0.0)),
        mp_out=float(r.get("StdMPOut", 0.0)),
        fb_out=float(r.get("StdFBOut", 0.0)),
        ewl=float(r.get("StdEWL", 0.0)),
    )
    cus = CustomLineInputs(
        demand=float(r.get("CusDemand", 0.0)),
        accepted_orders=float(r.get("CusAccepted", 0.0)),
        accumulated_orders=float(r.get("CusAccum", 0.0)),
        deliveries=float(r.get("CusDeliv", 0.0)),
        avg_lead_time=float(r.get("CusLeadTime", 0.0)),
        actual_price=float(r.get("CusPrice", 0.0)),
        q1=float(r.get("CusQ1", 0.0)),
        q2_first=float(r.get("CusQ2First", 0.0)),
        q2_second=float(r.get("CusQ2Second", 0.0)),
        q3=float(r.get("CusQ3", 0.0)),
        s1_out=float(r.get("CusS1Out", 0.0)),
        s2_machines=float(r.get("CusS2Machines", 0.0)),
        s2_out_first=float(r.get("CusS2OutFirst", 0.0)),
        s3_machines=float(r.get("CusS3Machines", 0.0)),
        s3_out=float(r.get("CusS3Out", 0.0)),
        s2_alloc_first_pct=float(r.get("AllocS2FirstPassPct_hist", 50.0)) if not pd.isna(r.get("AllocS2FirstPassPct_hist", np.nan)) else 50.0,
    )
    return inv, fin, wf, std, cus


# ============================================================
# Snapshot Recommendations (core)
# ============================================================
def recommend_inventory_policy(inv: InventoryInputs, std: StandardLineInputs, cus: CustomLineInputs) -> Dict[str, float]:
    std_parts_per_day = std.accepted_orders * STD_PARTS_PER_UNIT
    cus_parts_per_day = cus.demand * CUS_PARTS_PER_UNIT
    parts_per_day = std_parts_per_day + cus_parts_per_day
    rop = parts_per_day * inv.lead_time_days
    # EOQ (no safety)
    h = HOLDING_COST_PER_UNIT_PER_DAY
    roq = math.sqrt((2.0 * max(parts_per_day, 0.0) * inv.order_fee) / max(h, 1e-9)) if parts_per_day > 0 else 0.0
    coverage = safe_div(inv.inventory_level_parts, parts_per_day, default=0.0)
    return {
        "parts_per_day": parts_per_day,
        "coverage_days": coverage,
        "rop": rop,
        "roq": roq,
        "std_parts_per_day": std_parts_per_day,
        "cus_parts_per_day": cus_parts_per_day,
    }

def recommend_s2_allocation(cus: CustomLineInputs) -> Dict[str, float]:
    q1 = max(cus.q2_first, 0.0)
    q2 = max(cus.q2_second, 0.0)
    total = q1 + q2 + 1e-9
    imbalance = (q1 - q2) / total
    # if second pass bigger => shift allocation toward second pass (lower first pass %)
    suggested = 50.0 + imbalance * 25.0
    suggested = clamp(suggested, 10.0, 90.0)
    return {"suggested_first_pass_pct": suggested, "imbalance": imbalance}

def custom_bottleneck_heuristic(cus: CustomLineInputs) -> str:
    # Rule: if second pass queue dominates -> S2
    if cus.q2_second > cus.q2_first * 1.2 and cus.q2_second > 5:
        return "S2"
    # Else choose the smallest positive output stage as bottleneck proxy
    candidates = []
    if cus.s1_out > 0: candidates.append((cus.s1_out, "S1"))
    if cus.s2_out_first > 0: candidates.append((cus.s2_out_first, "S2"))
    if cus.s3_out > 0: candidates.append((cus.s3_out, "S3"))
    return min(candidates, key=lambda x: x[0])[1] if candidates else "S2"

def recommend_capacity_actions(cus: CustomLineInputs, wf: WorkforceInputs) -> Dict[str, float]:
    demand = max(cus.demand, 0.0)
    deliv = max(cus.deliveries, 0.0)
    gap = max(0.0, demand - deliv)

    bottleneck = custom_bottleneck_heuristic(cus)

    # per-machine productivity inferred from current day
    s2_per_machine = safe_div(cus.s2_out_first, max(cus.s2_machines, 1.0), default=0.0)
    s3_per_machine = safe_div(cus.s3_out, max(cus.s3_machines, 1.0), default=0.0)

    add_s1 = add_s2 = add_s3 = 0
    if gap > 0:
        if bottleneck == "S2":
            add_s2 = int(math.ceil(gap / max(s2_per_machine, 1e-9))) if s2_per_machine > 0 else 1
        elif bottleneck == "S3":
            add_s3 = int(math.ceil(gap / max(s3_per_machine, 1e-9))) if s3_per_machine > 0 else 1
        else:
            add_s1 = 0

    # hiring: need more effective workers (conservative proxy)
    rookie_prod = max(wf.rookie_productivity_vs_expert, 0.40)
    base = max(1.0, {"S1": cus.s1_out, "S2": cus.s2_out_first, "S3": cus.s3_out}.get(bottleneck, cus.s2_out_first))
    expert_equiv_needed = gap / base
    hire_rookies = int(math.ceil(expert_equiv_needed / rookie_prod)) if gap > 0 else 0
    hire_rookies = max(0, hire_rookies)

    capex = add_s1 * MACHINE_BUY["S1"] + add_s2 * MACHINE_BUY["S2"] + add_s3 * MACHINE_BUY["S3"]
    return {
        "gap": gap,
        "bottleneck": bottleneck,
        "add_s1": add_s1,
        "add_s2": add_s2,
        "add_s3": add_s3,
        "hire_rookies": hire_rookies,
        "capex": capex,
    }

def build_snapshot_checklist(inv: InventoryInputs, fin: FinancialInputs, wf: WorkforceInputs, std: StandardLineInputs, cus: CustomLineInputs) -> Tuple[str, List[Dict[str, str]], List[str], Dict[str, float]]:
    invrec = recommend_inventory_policy(inv, std, cus)
    s2rec = recommend_s2_allocation(cus)
    caprec = recommend_capacity_actions(cus, wf)

    checklist = []
    reasons = []
    severity = 0

    # Inventory
    if invrec["parts_per_day"] > 0 and invrec["coverage_days"] < inv.lead_time_days:
        severity += 2
        reasons.append("Raw parts coverage < lead time → เสี่ยง stockout → ส่งของไม่ทัน → backlog โต")
        checklist.append({
            "area": "Inventory",
            "finding": f"coverage ≈ {num(invrec['coverage_days'])} days (< LT {num(inv.lead_time_days)})",
            "action": "ตั้ง ROP ให้พอช่วง lead time (ยังไม่กัน safety)",
            "recommended_value": f"ROP≈{num(invrec['rop'])} | ROQ≈{num(invrec['roq'])}",
        })

    # Standard
    std_demand = max(std.accepted_orders, 0.0)
    std_gap = max(0.0, std_demand - max(std.deliveries, 0.0))
    if std_demand > 0 and std_gap > 0:
        severity += 1
        reasons.append("Standard ส่งไม่ทัน demand → ถ้า EWL สูง แปลว่าติด capacity (ขึ้นราคาอาจไม่ช่วยยอดขาย)")
        checklist.append({
            "area": "Standard Line",
            "finding": f"gap ≈ {num(std_gap)}/day | EWL≈{num(std.ewl)}%",
            "action": "ถ้า EWL>95% → แก้คอขวดก่อน (manual/initial/final) แล้วค่อย optimize ราคา",
            "recommended_value": f"WIP≈{num(std.q1+std.q2+std.q3+std.q4+std.q5)}",
        })

    # Custom
    if caprec["gap"] > 0:
        severity += 2
        reasons.append("Custom gap > 0 → backlog + lead time พุ่ง (Q2 imbalance / bottleneck stage)")
        checklist.append({
            "area": "Custom Line",
            "finding": f"gap ≈ {num(caprec['gap'])}/day | bottleneck≈{caprec['bottleneck']}",
            "action": "ปรับ Station2 allocation + เพิ่มกำลังผลิตเฉพาะจุดตัน",
            "recommended_value": f"S2 FirstPass≈{num(s2rec['suggested_first_pass_pct'])}% | CapEx≈{money(caprec['capex'])} | Hire≈{caprec['hire_rookies']}",
        })

    if cus.avg_lead_time >= 10:
        severity += 1
        reasons.append("Lead time สูง = WIP/คิวค้างสะสม (โดยเฉพาะ Q2 second pass)")
        checklist.append({
            "area": "Custom Lead Time",
            "finding": f"Avg LT ≈ {num(cus.avg_lead_time)} days",
            "action": "ลดคิวที่พองที่สุดก่อน (Q2 second pass มักทำให้ lead time พุ่ง)",
            "recommended_value": f"Q2(first)={num(cus.q2_first)} | Q2(second)={num(cus.q2_second)}",
        })

    status = "CRITICAL" if severity >= 5 else ("WARNING" if severity >= 2 else "OK")
    if not reasons:
        reasons = ["ไม่พบสัญญาณผิดปกติเด่นจาก snapshot (หรือ demand เป็น 0)"]

    metrics = {
        "inv_parts_per_day": invrec["parts_per_day"],
        "inv_coverage_days": invrec["coverage_days"],
        "inv_rop": invrec["rop"],
        "inv_roq": invrec["roq"],
        "s2_first_pct": s2rec["suggested_first_pass_pct"],
        "custom_gap": caprec["gap"],
        "custom_bottleneck": caprec["bottleneck"],
        "capex": caprec["capex"],
        "hire_rookies": caprec["hire_rookies"],
        "std_gap": std_gap,
    }
    return status, checklist, reasons, metrics


# ============================================================
# Pricing Intelligence
#  - Effect of PriceDiff on Demand/Deliveries with lag
#  - Recommend price (conservative + capacity-aware)
# ============================================================
def _ols_simple(x: np.ndarray, y: np.ndarray) -> Optional[Tuple[float, float, float]]:
    """Return (a, b, r2) for y = a + b*x"""
    if len(x) < 12:
        return None
    if np.nanstd(x) < 1e-9 or np.nanstd(y) < 1e-9:
        return None
    xm = np.nanmean(x); ym = np.nanmean(y)
    b = np.nansum((x - xm) * (y - ym)) / (np.nansum((x - xm) ** 2) + 1e-12)
    a = ym - b * xm
    yhat = a + b * x
    ss_res = np.nansum((y - yhat) ** 2)
    ss_tot = np.nansum((y - ym) ** 2) + 1e-12
    r2 = 1.0 - ss_res / ss_tot
    return float(a), float(b), float(r2)

def analyze_price_diff_effect(state: pd.DataFrame, lag: int = 1, window: int = 120) -> Dict[str, object]:
    """
    Model:
      Demand(t) ~ a + b * PriceDiff(t-lag)
      Deliveries(t) ~ a2 + b2 * PriceDiff(t-lag)
    """
    df = state.copy()
    if df.empty:
        return {"ok": False, "reason": "empty state"}

    df = df.tail(max(window, 60))

    # lagged price diff
    df["PD_lag"] = df["StdPriceDiff"].shift(lag)

    # targets
    y_demand = df["StdDemandProxy"].values.astype(float)
    y_deliv = df["StdDeliv"].values.astype(float)
    x = df["PD_lag"].values.astype(float)

    # usable rows
    m = np.isfinite(x) & np.isfinite(y_demand) & np.isfinite(y_deliv)
    x = x[m]; yd = y_demand[m]; yl = y_deliv[m]
    if len(x) < 12:
        return {"ok": False, "reason": "not enough usable rows (need >=12)"}

    fit_d = _ols_simple(x, yd)
    fit_l = _ols_simple(x, yl)

    return {
        "ok": True,
        "lag": lag,
        "n": int(len(x)),
        "fit_demand": fit_d,      # (a,b,r2)
        "fit_deliv": fit_l,       # (a,b,r2)
    }

def suggest_std_price_autopilot(state: pd.DataFrame, lookback: int = 120) -> Dict[str, object]:
    """
    Conservative policy:
      - If capacity constrained (EWL high or fill rate low or backlog>0), price suggestions should be gentle.
      - Use PriceDiff elasticity from history if strong enough; else heuristic.
    """
    df = state.copy().tail(max(lookback, 60))
    if df.empty:
        return {"ok": False, "reason": "empty state"}

    last = df.iloc[-1]
    market = float(last.get("StdMktPrice", 0.0))
    price = float(last.get("StdProductPrice", 0.0))
    backlog = float(last.get("StdBacklogProxy", 0.0))
    fill = float(last.get("StdFillRateProxy", 1.0))
    ewl = float(last.get("StdEWL", 0.0))

    cap_constrained = (ewl >= 95) or (fill < 0.98) or (backlog > 0)

    # Try find best lag (0..3) by demand r2
    best = None
    for lag in [0, 1, 2, 3]:
        res = analyze_price_diff_effect(df, lag=lag, window=lookback)
        if not res.get("ok"):
            continue
        fit = res.get("fit_demand")
        if fit is None:
            continue
        a, b, r2 = fit
        # We expect b negative: price higher than market -> demand down
        score = r2 if b < 0 else (r2 - 0.2)  # penalize wrong sign
        if best is None or score > best["score"]:
            best = {"lag": lag, "a": a, "b": b, "r2": r2, "score": score}

    # Heuristic bounds
    if market > 0:
        lo, hi = 0.80 * market, 1.20 * market
    else:
        lo, hi = 0.80 * max(price, 1.0), 1.20 * max(price, 1.0)

    method = "heuristic"
    suggested = price if price > 0 else market

    if best and best["r2"] >= 0.05 and best["b"] < 0:
        # If we are not capacity constrained, chase revenue-max (approx).
        # Demand model: Q = a + b*PD where PD = (P - Market)
        # Q(P) = a + b*(P - M) = (a - b*M) + b*P
        # Revenue R = P*Q = P*((a - b*M)+bP) => quadratic => optimum P* = -(a - b*M)/(2b)
        a = best["a"]; b = best["b"]; M = market
        denom = 2.0 * b
        if abs(denom) > 1e-9:
            p_star = - (a - b * M) / denom
            # conservative: if capacity constrained, damp movement
            p_star = float(clamp(p_star, lo, hi))
            if cap_constrained:
                # move only 30% toward p_star
                suggested = float(price + 0.30 * (p_star - price))
                method = f"elasticity(damped) lag={best['lag']} r2={best['r2']:.2f}"
            else:
                suggested = p_star
                method = f"elasticity(rev-max) lag={best['lag']} r2={best['r2']:.2f}"
        else:
            suggested = float(clamp(market, lo, hi))
            method = "elasticity(degenerate)->market"
    else:
        # heuristic using backlog/fill
        base = market if market > 0 else max(price, 1.0)
        if cap_constrained:
            # if constrained, raising price doesn't fix deliveries; keep near market and avoid demand spikes if backlog already high
            if backlog > 0:
                suggested = base * 1.03  # tiny raise to soften demand
            else:
                suggested = base * 1.00
            method = "heuristic(cap-constrained)"
        else:
            # not constrained: if fill high + backlog low -> reduce price a bit to grow volume; else keep near market
            if fill > 1.05 and backlog <= 0:
                suggested = base * 0.97
                method = "heuristic(grow-volume)"
            else:
                suggested = base * 1.00
                method = "heuristic(near-market)"

        suggested = float(clamp(suggested, lo, hi))

    return {
        "ok": True,
        "market": market,
        "current_price": price,
        "suggested_price": float(suggested),
        "cap_constrained": bool(cap_constrained),
        "backlog": backlog,
        "fill": fill,
        "ewl": ewl,
        "method": method,
        "bounds": (lo, hi),
        "best_elasticity": best,
    }


# ============================================================
# Forecast Simulator (>= 100 days)
#  - Uses learned elasticity if available, else heuristic
#  - Constrains by capacity + raw inventory
#  - Tracks cash/debt with interest + commission
# ============================================================
@dataclass
class ForecastPolicy:
    # Controls
    std_price: float
    raw_rop: float
    raw_roq: float
    s2_first_pct: float
    hire_rookies: int
    buy_s1: int
    buy_s2: int
    buy_s3: int
    loan_take: float
    loan_repay: float

def _infer_capacity_per_machine(state: pd.DataFrame) -> Dict[str, float]:
    # infer using last ~30 days median output per machine (custom S2/S3)
    df = state.tail(60).copy()
    s2_per = []
    s3_per = []
    if "CusS2OutFirst" in df.columns and "CusS2Machines" in df.columns:
        m = (df["CusS2Machines"] > 0) & (df["CusS2OutFirst"] > 0)
        if m.any():
            s2_per = (df.loc[m, "CusS2OutFirst"] / df.loc[m, "CusS2Machines"]).replace([np.inf, -np.inf], np.nan).dropna().values.tolist()
    if "CusS3Out" in df.columns and "CusS3Machines" in df.columns:
        m = (df["CusS3Machines"] > 0) & (df["CusS3Out"] > 0)
        if m.any():
            s3_per = (df.loc[m, "CusS3Out"] / df.loc[m, "CusS3Machines"]).replace([np.inf, -np.inf], np.nan).dropna().values.tolist()

    # fallback minimal positive defaults if no data
    cap = {
        "S2_per_machine": float(np.median(s2_per)) if len(s2_per) else max(1.0, float(df["CusS2OutFirst"].tail(1).values[0]) / max(1.0, float(df["CusS2Machines"].tail(1).values[0])) if len(df) else 1.0),
        "S3_per_machine": float(np.median(s3_per)) if len(s3_per) else max(1.0, float(df["CusS3Out"].tail(1).values[0]) / max(1.0, float(df["CusS3Machines"].tail(1).values[0])) if len(df) else 1.0),
    }
    return cap

def simulate_100_days(state: pd.DataFrame, policy: ForecastPolicy, horizon: int = 100) -> pd.DataFrame:
    """
    This is conservative: it won't magically create profits.
    It will show direction and constraints clearly.
    """
    df = state.copy()
    if df.empty:
        return pd.DataFrame()

    last_day = int(df.index.max())
    start = df.loc[last_day].copy()

    # initial conditions
    cash = float(start.get("Cash", 0.0))
    debt = float(start.get("Debt", 0.0))
    raw_inv = float(start.get("RawInv", 0.0))

    rookies = float(start.get("Rookies", 0.0))
    experts = float(start.get("Experts", 0.0))

    # machine counts (prefer History net if available)
    s2_m = float(start.get("HistMachines_S2", start.get("CusS2Machines", 0.0)))
    s3_m = float(start.get("HistMachines_S3", start.get("CusS3Machines", 0.0)))
    # S1 is messy across std/custom; we keep it for capex/logic only
    s1_m = float(start.get("HistMachines_S1", start.get("StdS1Machines", 0.0)))

    # market price assumption: last market stays as baseline
    market = float(start.get("StdMktPrice", 0.0))

    # elasticity model
    sugg = suggest_std_price_autopilot(df, lookback=120)
    best = sugg.get("best_elasticity") or {}
    b = float(best.get("b", 0.0))
    lag = int(best.get("lag", 1))
    # If no elasticity, use heuristic elasticity: demand changes -0.25 per $1 of price_diff (scaled by typical demand)
    use_elasticity = bool(best) and best.get("r2", 0.0) >= 0.05 and b < 0

    # demand baseline
    base_demand = float(start.get("StdDemandProxy", 0.0))
    if base_demand <= 0:
        base_demand = float(np.median(df["StdDemandProxy"].tail(60).values)) if "StdDemandProxy" in df.columns else 0.0
        base_demand = max(base_demand, 0.0)

    # capacity inference (for custom bottleneck)
    cap = _infer_capacity_per_machine(df)
    s2_per = max(cap["S2_per_machine"], 1e-6)
    s3_per = max(cap["S3_per_machine"], 1e-6)

    # queues (proxies)
    std_backlog = float(start.get("StdBacklogProxy", 0.0))
    cus_backlog = float(start.get("CusBacklogProxy", 0.0))

    # inbound raw orders pipeline (lead time)
    pipeline = [0.0] * int(LEAD_TIME_DAYS)  # arrives after 4 days => index 3 arrives on day+4
    # if policy uses ROQ/ROP, we place orders when raw_inv + on_order - usage <= ROP
    # We start with no explicit on_order from file (game has it) -> approximate as 0

    rows = []
    for t in range(1, horizon + 1):
        day = last_day + t

        # Apply policy actions at day 1 only (autopilot step)
        if t == 1:
            # loan
            if policy.loan_take > 0:
                cash += float(policy.loan_take) * (1.0 - LOAN_COMMISSION_RATE)
                debt += float(policy.loan_take)
            if policy.loan_repay > 0:
                pay = min(float(policy.loan_repay), cash)
                cash -= pay
                debt = max(0.0, debt - pay)

            # buy machines
            capex = (policy.buy_s1 * MACHINE_BUY["S1"]) + (policy.buy_s2 * MACHINE_BUY["S2"]) + (policy.buy_s3 * MACHINE_BUY["S3"])
            if capex > 0:
                spend = min(capex, cash)
                # if not enough cash, buy proportionally (conservative)
                ratio = spend / capex if capex > 0 else 0.0
                s1_m += policy.buy_s1 * ratio
                s2_m += policy.buy_s2 * ratio
                s3_m += policy.buy_s3 * ratio
                cash -= spend

            # hire
            if policy.hire_rookies > 0:
                rookies += float(policy.hire_rookies)

        # Arrivals
        arrivals = pipeline.pop(0) if pipeline else 0.0
        raw_inv += arrivals

        # Demand (standard) from price
        price = float(policy.std_price)
        pdiff = price - market

        if use_elasticity:
            # Q = base + b*(pdiff - base_pdiff) approx
            # We'll anchor around last pdiff:
            last_pdiff = float(start.get("StdPriceDiff", 0.0))
            demand_std = max(0.0, base_demand + b * (pdiff - last_pdiff))
        else:
            # heuristic: if pdiff positive, demand down; if negative, demand up
            # scale sensitivity with base_demand
            sensitivity = max(0.02 * base_demand, 0.5)  # units per $1
            demand_std = max(0.0, base_demand - sensitivity * pdiff / 10.0)  # $10 diff -> sensitivity

        # Custom demand: keep last observed (or median) as baseline (you can extend to learn custom pricing later)
        demand_cus = float(start.get("CusDemandProxy", 0.0))
        if demand_cus <= 0:
            demand_cus = float(np.median(df["CusDemandProxy"].tail(60).values)) if "CusDemandProxy" in df.columns else 0.0
        demand_cus = max(demand_cus, 0.0)

        # Capacity constraint (very simplified)
        # Standard deliveries: limited by manual capacity proxy (if EWL high -> use last deliveries as cap)
        std_cap = float(start.get("StdDeliv", 0.0))
        if std_cap <= 0:
            std_cap = float(np.median(df["StdDeliv"].tail(30).values)) if "StdDeliv" in df.columns else 0.0
        std_cap = max(std_cap, 0.0)

        sold_std = min(demand_std + std_backlog, std_cap)
        std_backlog = max(0.0, std_backlog + demand_std - sold_std)

        # Custom deliveries: bottleneck around S2 & S3 (S2 has 2 passes / unit)
        # Effective S2 units/day = (S2 machines * per_machine_output_firstpass) / 2 passes
        cus_cap_s2 = (s2_m * s2_per) / CUS_S2_PASSES_PER_UNIT
        cus_cap_s3 = (s3_m * s3_per)
        cus_cap = max(0.0, min(cus_cap_s2, cus_cap_s3))
        sold_cus = min(demand_cus + cus_backlog, cus_cap)
        cus_backlog = max(0.0, cus_backlog + demand_cus - sold_cus)

        # Raw usage (standard uses 2 parts per unit, custom uses 1)
        need_parts = sold_std * STD_PARTS_PER_UNIT + sold_cus * CUS_PARTS_PER_UNIT
        # If not enough raw, scale down deliveries proportionally (stockout)
        if need_parts > raw_inv and need_parts > 0:
            scale = raw_inv / need_parts
            sold_std *= scale
            sold_cus *= scale
            need_parts = raw_inv
            # backlog increases because you couldn't ship
            # (we already added backlog; scaling shipments up/down effectively increases backlog)
            # We'll adjust backlog after scaling:
            std_backlog = max(0.0, std_backlog + (demand_std - sold_std))
            cus_backlog = max(0.0, cus_backlog + (demand_cus - sold_cus))

        raw_inv -= need_parts

        # Inventory policy: place order if position <= ROP
        # Approx position = raw_inv + sum(pipeline) (on order)
        position = raw_inv + sum(pipeline)
        if policy.raw_roq > 0 and position <= policy.raw_rop:
            pipeline.append(float(policy.raw_roq))
        else:
            pipeline.append(0.0)

        # Costs
        eff_workers = experts + rookies * ROOKIE_PRODUCTIVITY
        # Salaries paid on headcount (real game uses required employees and overtime etc.)
        # Conservative: pay all employees as they exist
        salary_cost = rookies * SALARY_ROOKIE + experts * SALARY_EXPERT

        # Holding costs proxy: raw + backlog (queues)
        holding = (max(raw_inv, 0.0) + std_backlog + cus_backlog) * HOLDING_COST_PER_UNIT_PER_DAY

        # Interest
        debt_interest = debt * (NORMAL_DEBT_APR / 365.0)
        cash_interest = cash * CASH_INTEREST_DAILY

        # Revenue
        revenue = sold_std * price  # custom revenue omitted because we don't model custom price/demand fully here
        # (You can add custom price later: revenue += sold_cus * cus_price)

        profit = revenue - salary_cost - holding - debt_interest + cash_interest
        cash += profit
        debt += debt_interest  # interest accrues

        # Rookie -> expert progression (approx): after 15 days, proportion becomes experts gradually
        # We'll use a simple flow: each day, rookies_convert = rookies / 15
        convert = rookies / max(ROOKIE_TO_EXPERT_DAYS, 1.0)
        rookies = max(0.0, rookies - convert)
        experts += convert

        rows.append({
            "Day": day,
            "StdPrice": price,
            "Market": market,
            "PriceDiff": pdiff,
            "DemandStd": demand_std,
            "SoldStd": sold_std,
            "StdBacklog": std_backlog,
            "DemandCus": demand_cus,
            "SoldCus": sold_cus,
            "CusBacklog": cus_backlog,
            "RawInv": raw_inv,
            "RawPosition": position,
            "RawArrivals": arrivals,
            "RawOrderPlaced": pipeline[-1] if pipeline else 0.0,
            "Cash": cash,
            "Debt": debt,
            "Revenue": revenue,
            "SalaryCost": salary_cost,
            "HoldingCost": holding,
            "DebtInterest": debt_interest,
            "CashInterest": cash_interest,
            "Profit": profit,
            "S2Machines": s2_m,
            "S3Machines": s3_m,
            "Rookies": rookies,
            "Experts": experts,
            "EffWorkers": eff_workers,
            "CusCap": cus_cap,
            "StdCap": std_cap,
            "Stockout": 1 if (need_parts > 0 and need_parts >= raw_inv + 1e-9 and (sold_std + sold_cus) < (min(demand_std+std_backlog, std_cap) + min(demand_cus+cus_backlog, cus_cap))) else 0,
        })

    out = pd.DataFrame(rows).set_index("Day")
    return out


# ============================================================
# Header
# ============================================================
with st.container():
    c1, c2 = st.columns([2, 1])
    with c1:
        st.title("🏭 Factory Status Analyzer — Autopilot PRO")
        st.caption("Upload Excel → Learn from History → Forecast 100 days → Autopilot actions + Why + Expected Impact")
    with c2:
        st.markdown("### Session")
        st.code(SID[:8])
        if st.button("🔄 Reset (เฉพาะฉัน)"):
            st.session_state.pop("sid", None)
            st.rerun()


tabs = st.tabs([
    "0) Import Excel",
    "1) Snapshot Input Override",
    "2) Dashboard (Snapshot)",
    "3) Trends (Full-file)",
    "4) Pricing + PriceDiff→Demand/Deliveries",
    "5) Forecast 100 Days",
    "6) Autopilot (One-click plan)",
])


# ============================================================
# Tab 0: Import
# ============================================================
with tabs[0]:
    st.subheader("Import Excel (.xlsx)")
    st.write("✅ ต้องมีอย่างน้อย: Standard/Custom/Inventory/Finance/Workforce/History (มีเท่าไหร่ก็ได้ แต่ History สำคัญมาก)")
    up = st.file_uploader("Upload .xlsx", type=["xlsx"])

    if up is not None:
        try:
            xbytes = up.getvalue()
            S["last_uploaded_bytes"] = xbytes
            st.session_state.pop("fullfile_day_range", None)

            parsed = build_full_state(xbytes)
            S["cache"]["state"] = parsed["state"]
            S["cache"]["events"] = parsed["events"]

            state = parsed["state"]
            max_day = int(state.index.max()) if not state.empty else 0

            st.success(f"Loaded ✅  Days: 0..{max_day}")
            st.caption("Tip: Product Price ที่คุณตั้งเองจะถูก reconstruct จาก History (Updated product price...)")

            # Choose import day (default latest)
            day = st.number_input("Select Snapshot Day", min_value=0, max_value=max_day, value=max_day, step=1)
            if st.button("✅ Load Snapshot Day"):
                inv, fin, wf, std, cus = load_snapshot_from_state(state, int(day))
                S["inventory"], S["finance"], S["workforce"], S["std"], S["cus"] = inv, fin, wf, std, cus
                S["import_day"] = int(day)
                st.success(f"Snapshot loaded: Day {int(day)}")

            with st.expander("Show parsed History events (first 50)"):
                ev = parsed["events"]
                st.dataframe(ev.head(50), use_container_width=True)

        except Exception as e:
            st.error("Import failed")
            st.exception(e)


# ============================================================
# Tab 1: Snapshot Input Override
# ============================================================
with tabs[1]:
    st.subheader("Snapshot Input Override")
    st.caption("ปรับค่าแบบ manual เพื่อทดลอง what-if (ไม่แก้ไฟล์จริง)")

    inv: InventoryInputs = S["inventory"]
    fin: FinancialInputs = S["finance"]
    wf: WorkforceInputs = S["workforce"]
    std: StandardLineInputs = S["std"]
    cus: CustomLineInputs = S["cus"]

    A, B, C = st.columns([1.1, 1.1, 1.1])

    with A:
        st.markdown("### 📦 Inventory")
        inv.inventory_level_parts = st.number_input("Raw Inventory (parts)", value=float(inv.inventory_level_parts), step=1.0)
        inv.reorder_point = st.number_input("Current ROP", value=float(inv.reorder_point), step=1.0)
        inv.reorder_quantity = st.number_input("Current ROQ", value=float(inv.reorder_quantity), step=1.0)
        st.markdown("### 💰 Finance")
        fin.cash_on_hand = st.number_input("Cash On Hand", value=float(fin.cash_on_hand), step=1000.0)
        fin.debt = st.number_input("Debt", value=float(fin.debt), step=1000.0)

    with B:
        st.markdown("### 👷 Workforce")
        wf.rookies = st.number_input("Rookies", value=float(wf.rookies), step=1.0)
        wf.experts = st.number_input("Experts", value=float(wf.experts), step=1.0)

        st.markdown("### 🧱 Standard")
        std.accepted_orders = st.number_input("Std Accepted Orders", value=float(std.accepted_orders), step=1.0)
        std.accumulated_orders = st.number_input("Std Accumulated Orders", value=float(std.accumulated_orders), step=1.0)
        std.deliveries = st.number_input("Std Deliveries", value=float(std.deliveries), step=1.0)
        std.market_price = st.number_input("Std Market Price", value=float(std.market_price), step=1.0)
        std.product_price = st.number_input("Std Product Price (from History)", value=float(std.product_price), step=1.0)

    with C:
        st.markdown("### 🧩 Custom")
        cus.demand = st.number_input("Custom Daily Demand", value=float(cus.demand), step=1.0)
        cus.accumulated_orders = st.number_input("Custom Accumulated Orders", value=float(cus.accumulated_orders), step=1.0)
        cus.deliveries = st.number_input("Custom Deliveries", value=float(cus.deliveries), step=1.0)
        cus.avg_lead_time = st.number_input("Custom Avg Lead Time", value=float(cus.avg_lead_time), step=0.1)
        cus.q2_first = st.number_input("Custom Q2 First Pass", value=float(cus.q2_first), step=1.0)
        cus.q2_second = st.number_input("Custom Q2 Second Pass", value=float(cus.q2_second), step=1.0)
        cus.s2_machines = st.number_input("Custom S2 Machines", value=float(cus.s2_machines), step=1.0)
        cus.s3_machines = st.number_input("Custom S3 Machines", value=float(cus.s3_machines), step=1.0)
        cus.s2_alloc_first_pct = st.number_input("Station2 Allocation to First Pass (%)", value=float(cus.s2_alloc_first_pct), step=1.0)


# ============================================================
# Tab 2: Dashboard Snapshot
# ============================================================
with tabs[2]:
    st.subheader("Dashboard (Snapshot)")

    inv: InventoryInputs = S["inventory"]
    fin: FinancialInputs = S["finance"]
    wf: WorkforceInputs = S["workforce"]
    std: StandardLineInputs = S["std"]
    cus: CustomLineInputs = S["cus"]

    status, checklist, reasons, metrics = build_snapshot_checklist(inv, fin, wf, std, cus)

    tag = f"(Imported Day {S['import_day']})" if S["import_day"] is not None else "(No import yet)"
    icon = {"OK": "🟢", "WARNING": "🟠", "CRITICAL": "🔴"}[status]
    st.markdown(f"## {icon} STATUS: **{status}** {tag}")

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Parts/day", num(metrics["inv_parts_per_day"]))
    k2.metric("Coverage (days)", num(metrics["inv_coverage_days"]))
    k3.metric("ROP / ROQ", f"{num(metrics['inv_rop'])} / {num(metrics['inv_roq'])}")
    k4.metric("Custom gap/day", num(metrics["custom_gap"]))
    k5.metric("Std gap/day", num(metrics["std_gap"]))
    k6.metric("Cash / Debt", f"{money(fin.cash_on_hand)} / {money(fin.debt)}")

    with st.expander("📌 Why (เหตุผลหลัก)", expanded=True):
        for r in reasons:
            st.write(f"- {r}")

    st.markdown("### ✅ Checklist")
    st.dataframe(pd.DataFrame(checklist), use_container_width=True)

    st.markdown("### 📌 Recommended Settings (copy to game)")
    rec = {
        "Inventory: ROP (no safety)": float(metrics["inv_rop"]),
        "Inventory: ROQ (EOQ, no safety)": float(metrics["inv_roq"]),
        "Custom Station2: First Pass %": float(metrics["s2_first_pct"]),
        "Custom Bottleneck (heuristic)": str(metrics["custom_bottleneck"]),
        "Hire Rookies (proxy)": int(metrics["hire_rookies"]),
        "CapEx Estimate": float(metrics["capex"]),
        "Std Product Price (current)": float(std.product_price),
        "Std Market Price": float(std.market_price),
    }
    st.json(rec)


# ============================================================
# Tab 3: Trends
# ============================================================
with tabs[3]:
    st.subheader("Trends (Full-file)")

    if S["last_uploaded_bytes"] is None:
        st.info("Upload file in Import tab first.")
    else:
        state = S["cache"].get("state")
        if state is None or state.empty:
            st.warning("No state cached — re-import")
        else:
            st.markdown("#### 💵 Cash / Debt")
            st.line_chart(state[["Cash", "Debt"]], height=220)

            st.markdown("#### 📈 Profit/day (proxy) — if finance *to date columns exist")
            if "ProfitPerDay_proxy" in state.columns:
                st.line_chart(state[["ProfitPerDay_proxy"]], height=200)

            st.markdown("#### 📦 Raw Inventory")
            st.line_chart(state[["RawInv"]], height=200)

            st.markdown("#### 🧱 Standard: Demand vs Deliveries")
            st.line_chart(state[["StdDemandProxy", "StdDeliv"]], height=220)

            st.markdown("#### 🧱 Standard: Product Price (from History) vs Market Price")
            st.line_chart(state[["StdProductPrice", "StdMktPrice"]], height=220)

            st.markdown("#### 🧩 Custom: Demand vs Deliveries")
            st.line_chart(state[["CusDemandProxy", "CusDeliv"]], height=220)

            st.markdown("#### 🧩 Custom: Q2 First vs Second (imbalance)")
            cols = [c for c in ["CusQ2First", "CusQ2Second"] if c in state.columns]
            if cols:
                st.line_chart(state[cols], height=220)


# ============================================================
# Tab 4: Pricing + PriceDiff effect
# ============================================================
with tabs[4]:
    st.subheader("Pricing (Full-file) — Suggest Standard Product Price + PriceDiff→Demand/Deliveries")

    if S["last_uploaded_bytes"] is None:
        st.info("Upload file in Import tab first.")
    else:
        state = S["cache"].get("state")
        if state is None or state.empty:
            st.warning("No state cached — re-import")
        else:
            # Range slider
            min_d = int(state.index.min())
            max_d = int(state.index.max())
            if min_d == max_d:
                r0, r1 = min_d, max_d
                st.info(f"Only one day: {min_d}")
            else:
                r0, r1 = st.slider("Select day range", min_value=min_d, max_value=max_d, value=(max(0, max_d-200), max_d), step=1)

            dfR = state.loc[r0:r1].copy()
            if dfR.empty:
                st.warning("No rows in range")
            else:
                sugg = suggest_std_price_autopilot(dfR, lookback=min(120, len(dfR)))

                if not sugg.get("ok"):
                    st.warning(f"Pricing suggest failed: {sugg.get('reason')}")
                else:
                    method = sugg["method"]
                    st.markdown("### ✅ Suggested Standard Product Price")
                    st.info(
                        f"Suggested: **{money(sugg['suggested_price'])}**  | "
                        f"Current: {money(sugg['current_price'])}  | "
                        f"Market: {money(sugg['market'])}  | "
                        f"Method: {method}"
                    )
                    if sugg["cap_constrained"]:
                        st.warning("⚠️ Capacity constrained (EWL high / backlog / low fill) → price model may be misleading. Fix bottleneck first.")

                st.markdown("### 📌 PriceDiff vs Demand/Deliveries (with lag)")
                lag = st.slider("Lag days (PriceDiff affects t+lag)", 0, 5, 1, 1)
                effect = analyze_price_diff_effect(dfR, lag=lag, window=len(dfR))

                if not effect.get("ok"):
                    st.warning(f"Effect analysis failed: {effect.get('reason')}")
                else:
                    fd = effect.get("fit_demand")
                    fl = effect.get("fit_deliv")

                    c1, c2, c3 = st.columns(3)
                    if fd:
                        a, b, r2 = fd
                        c1.metric("Demand model slope b", num(b))
                        c2.metric("Demand model R²", num(r2))
                    if fl:
                        a2, b2, r22 = fl
                        c3.metric("Deliveries slope b", num(b2))

                    st.caption("Interpretation: b < 0 => ตั้งราคาแพงกว่า market (PriceDiff+) → demand/deliveries ลดลง (โดยเฉพาะถ้าไม่ติด capacity).")

                st.markdown("### 📈 Time series (Price, Market, Demand, Deliveries)")
                st.line_chart(dfR[["StdProductPrice", "StdMktPrice", "StdDemandProxy", "StdDeliv"]], height=260)

                st.markdown("### 🧠 Debug: ถ้าไม่เห็น Product Price")
                st.write("- Product Price ไม่อยู่ในชีท Standard → อยู่ในชีท **History** (Updated product price...)")
                st.write("- ถ้า History ไม่มีเหตุการณ์ตั้งราคาเลย → script จะ fallback = market price")


# ============================================================
# Tab 5: Forecast 100 days
# ============================================================
with tabs[5]:
    st.subheader("Forecast (100 Days) — Demand→Capacity→Inventory→Cash→Debt")

    if S["last_uploaded_bytes"] is None:
        st.info("Upload file in Import tab first.")
    else:
        state = S["cache"].get("state")
        if state is None or state.empty:
            st.warning("No state cached — re-import")
        else:
            sugg = suggest_std_price_autopilot(state, lookback=120)
            inv: InventoryInputs = S["inventory"]
            wf: WorkforceInputs = S["workforce"]
            cus: CustomLineInputs = S["cus"]

            invrec = recommend_inventory_policy(inv, S["std"], cus)
            s2rec = recommend_s2_allocation(cus)
            caprec = recommend_capacity_actions(cus, wf)

            st.markdown("### Policy inputs (editable)")
            col1, col2, col3 = st.columns(3)
            with col1:
                std_price = st.number_input("Std Price", value=float(sugg.get("suggested_price", float(state.iloc[-1]["StdProductPrice"]))), step=1.0)
                raw_rop = st.number_input("Raw ROP", value=float(invrec["rop"]), step=1.0)
                raw_roq = st.number_input("Raw ROQ", value=float(invrec["roq"]), step=1.0)
            with col2:
                s2_first = st.number_input("S2 First Pass %", value=float(s2rec["suggested_first_pass_pct"]), step=1.0)
                hire = st.number_input("Hire Rookies (day1)", value=int(caprec["hire_rookies"]), step=1)
            with col3:
                buy_s2 = st.number_input("Buy S2 machines (day1)", value=0, step=1)
                loan_take = st.number_input("Loan take (day1)", value=0.0, step=1000.0)
                loan_repay = st.number_input("Loan repay (day1)", value=0.0, step=1000.0)

            policy = ForecastPolicy(
                std_price=float(std_price),
                raw_rop=float(raw_rop),
                raw_roq=float(raw_roq),
                s2_first_pct=float(s2_first),
                hire_rookies=int(hire),
                buy_s1=0,
                buy_s2=int(buy_s2),
                buy_s3=0,
                loan_take=float(loan_take),
                loan_repay=float(loan_repay),
            )

            horizon = st.slider("Horizon days", 30, 200, 100, 10)

            if st.button("▶ Run forecast"):
                fc = simulate_100_days(state, policy, horizon=int(horizon))
                if fc.empty:
                    st.warning("Forecast produced no rows")
                else:
                    st.success("Forecast done ✅")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Final Cash", money(float(fc["Cash"].iloc[-1])))
                    c2.metric("Final Debt", money(float(fc["Debt"].iloc[-1])))
                    c3.metric("Avg Profit/day", money(float(fc["Profit"].mean())))
                    c4.metric("Avg Stockout days", num(float(fc["Stockout"].mean())))

                    st.markdown("#### Cash / Debt")
                    st.line_chart(fc[["Cash", "Debt"]], height=240)

                    st.markdown("#### Profit / Revenue / Costs")
                    st.line_chart(fc[["Profit", "Revenue", "SalaryCost", "HoldingCost"]], height=260)

                    st.markdown("#### Backlogs")
                    st.line_chart(fc[["StdBacklog", "CusBacklog"]], height=240)

                    st.markdown("#### Raw Inventory & Position")
                    st.line_chart(fc[["RawInv", "RawPosition"]], height=240)

                    with st.expander("Show forecast table (first 50)"):
                        st.dataframe(fc.head(50), use_container_width=True)


# ============================================================
# Tab 6: Autopilot (One-click plan)
# ============================================================
with tabs[6]:
    st.subheader("Autopilot — One-click plan (Action + Why + Expected impact)")

    if S["last_uploaded_bytes"] is None:
        st.info("Upload file in Import tab first.")
    else:
        state = S["cache"].get("state")
        if state is None or state.empty:
            st.warning("No state cached — re-import")
        else:
            inv: InventoryInputs = S["inventory"]
            fin: FinancialInputs = S["finance"]
            wf: WorkforceInputs = S["workforce"]
            std: StandardLineInputs = S["std"]
            cus: CustomLineInputs = S["cus"]

            status, checklist, reasons, metrics = build_snapshot_checklist(inv, fin, wf, std, cus)
            sugg = suggest_std_price_autopilot(state, lookback=120)
            invrec = recommend_inventory_policy(inv, std, cus)
            s2rec = recommend_s2_allocation(cus)
            caprec = recommend_capacity_actions(cus, wf)

            # Autopilot chooses actions based on your constraints: "ไม่มีเงินซื้อเครื่องแล้ว"
            # We'll default: zero machine buys, use loan helper if needed.
            st.markdown("### ✅ Autopilot Actions (today)")
            actions = {
                "Std Product Price": float(sugg.get("suggested_price", std.product_price)),
                "Raw ROP": float(invrec["rop"]),
                "Raw ROQ": float(invrec["roq"]),
                "S2 First Pass %": float(s2rec["suggested_first_pass_pct"]),
                "Hire Rookies": int(caprec["hire_rookies"]),
                "Buy S1": 0,
                "Buy S2": 0,
                "Buy S3": 0,
                "Loan Take": 0.0,
                "Loan Repay": 0.0,
            }

            # If capex needed but user has no cash, propose loan only if it likely pays (simple rule)
            need_capex = float(caprec["capex"])
            if need_capex > 0 and fin.cash_on_hand < need_capex:
                # simple: propose loan = gap + commission buffer
                gap = max(0.0, need_capex - fin.cash_on_hand)
                actions["Loan Take"] = float(math.ceil(gap / 1000.0) * 1000.0)

            st.json(actions)

            st.markdown("### 🧠 Why this plan")
            for r in reasons:
                st.write(f"- {r}")
            st.write(f"- Pricing method: **{sugg.get('method','n/a')}**  | cap_constrained={sugg.get('cap_constrained', False)}")
            st.write("- Inventory ROP/ROQ ถูกตั้งเพื่อให้ coverage ≥ lead time ลด stockout ที่ทำให้ยอดขายไม่ขึ้น")

            st.markdown("### 🔮 Expected impact (forecast 100 days, conservative)")
            policy = ForecastPolicy(
                std_price=float(actions["Std Product Price"]),
                raw_rop=float(actions["Raw ROP"]),
                raw_roq=float(actions["Raw ROQ"]),
                s2_first_pct=float(actions["S2 First Pass %"]),
                hire_rookies=int(actions["Hire Rookies"]),
                buy_s1=int(actions["Buy S1"]),
                buy_s2=int(actions["Buy S2"]),
                buy_s3=int(actions["Buy S3"]),
                loan_take=float(actions["Loan Take"]),
                loan_repay=float(actions["Loan Repay"]),
            )

            fc = simulate_100_days(state, policy, horizon=100)
            if fc.empty:
                st.warning("Forecast failed (empty)")
            else:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Final Cash", money(float(fc["Cash"].iloc[-1])))
                c2.metric("Final Debt", money(float(fc["Debt"].iloc[-1])))
                c3.metric("Avg Profit/day", money(float(fc["Profit"].mean())))
                c4.metric("Stockout days%", f"{num(float(fc['Stockout'].mean()*100))}%")

                st.line_chart(fc[["Cash", "Debt"]], height=230)

                with st.expander("Autopilot forecast details"):
                    st.dataframe(fc.tail(30), use_container_width=True)


# ============================================================
# Footer / Diagnostics
# ============================================================
st.caption("If something looks wrong: check that Sheet 'History' exists and has columns Day/User/Description.")
