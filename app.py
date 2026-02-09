# ============================================================
# Factory Autopilot Analyzer — Medica Scientific (PRO v1.0) ✅
# ------------------------------------------------------------
# Goal:
#  - คนไม่รู้เกม: "อัปโหลด Excel → ทำตาม Suggest → เล่นเก่ง/รวยขึ้น"
#  - เน้น Profit/day + Cash endgame + คุม Risk (Debt/Backlog/Stockout)
#
# What you get:
#  ✅ Robust import (alias columns, auto-detect day range)
#  ✅ Snapshot Analyzer (ครบแน่น เหมือนสไตล์เดิม) + Reasons
#  ✅ Full-file Trends (Cash/Debt/Profit proxy/Inventory/Queues/EWL)
#  ✅ Pricing Intelligence:
#       - Learn demand response from PriceGap% = (Price-Market)/Market
#       - Estimate impact on Accepted Orders & Deliveries
#       - Capacity-aware pricing (อย่าลดราคาจน backlog ระเบิด)
#  ✅ Autopilot Plan:
#       - Suggest TODAY settings
#       - Simulate/Forecast 100 days (policy-driven)
#       - “What-if” + loan break-even (commission each loan)
#
# Requirements:
#   streamlit
#   pandas
#   openpyxl
#   numpy
#
# Notes:
# - This is a "smart conservative" autopilot: ไม่เดา game rules เกินข้อมูล
# - ถ้าบางคอลัมน์ในไฟล์ไม่ตรง alias → Script จะยังรันและแสดงสิ่งที่หาได้
# ============================================================

import io
import math
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# -----------------------------
# Helpers
# -----------------------------
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b not in (0, 0.0) else default

def num(x: float) -> str:
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return "0.00"

def money(x: float) -> str:
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "$0.00"

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
    if (not col) or (col not in df.columns):
        return pd.Series([0.0] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

def safe_day_series(df: Optional[pd.DataFrame], day_aliases: List[str]) -> pd.Series:
    if df is None:
        return pd.Series([], dtype=int)
    dcol = pick_col(df, day_aliases)
    if not dcol:
        return pd.Series([], dtype=int)
    vals = pd.to_numeric(df[dcol], errors="coerce").dropna()
    if vals.empty:
        return pd.Series([], dtype=int)
    return vals.astype(int)

def getv(row: pd.Series, df: pd.DataFrame, aliases: List[str], default=0.0) -> float:
    col = pick_col(df, aliases)
    if not col:
        return float(default)
    return to_float(row.get(col, default), default)

# -----------------------------
# Column aliases (robust import)
# -----------------------------
COL = {
    "DAY": ["Day", "day", "DAY"],

    # Inventory
    "INV_LEVEL": ["Inventory-Level", "Inventory Level", "Inventory_Level", "Raw Inventory", "Raw Inventory-Level"],

    # Finance (point-in-time)
    "CASH": ["Finance-Cash On Hand", "Cash On Hand", "Finance Cash On Hand", "Cash"],
    "DEBT": ["Finance-Debt", "Debt", "Finance Debt"],

    # Finance (to-date)
    "FIN_SALES_STD_TD": ["Finance-Sales Standard *To Date", "Finance-Sales Standard To Date", "Sales Standard *To Date"],
    "FIN_SALES_CUS_TD": ["Finance-Sales Custom *To Date", "Finance-Sales Custom To Date", "Sales Custom *To Date"],
    "FIN_SALARIES_TD": ["Finance-Salaries *To Date", "Finance-Salaries To Date", "Salaries *To Date"],
    "FIN_HOLD_RAW_TD": ["Finance-Raw Inventory Holding Costs *To Date", "Raw Inventory Holding Costs *To Date"],
    "FIN_HOLD_CUS_TD": ["Finance-Custom Queues Holding Costs *To Date", "Custom Queues Holding Costs *To Date"],
    "FIN_HOLD_STD_TD": ["Finance-Standard Queues Holding Costs *To Date", "Standard Queues Holding Costs *To Date"],
    "FIN_DEBT_INT_TD": ["Finance-Debt Interest Paid *To Date", "Debt Interest Paid *To Date"],
    "FIN_LOAN_COM_TD": ["Finance-Loan Commission Paid *To Date", "Loan Commission Paid *To Date"],

    # Workforce
    "ROOKIES": ["WorkForce-Rookies", "Workforce-Rookies", "Rookies", "Work Force-Rookies"],
    "EXPERTS": ["WorkForce-Experts", "Workforce-Experts", "Experts", "Work Force-Experts"],

    # Standard
    "STD_ACCEPT": ["Standard Orders-Accepted Orders", "Standard Accepted Orders", "Standard Accepted", "Accepted Orders"],
    "STD_ACCUM": ["Standard Orders-Accumulated Orders", "Standard Accumulated Orders", "Standard Accumulated", "Accumulated Orders"],
    "STD_DELIV": ["Standard Deliveries-Deliveries", "Standard Deliveries", "Deliveries", "Deliveries Out", "Deliveries_Out"],
    "STD_PRICE": ["Standard Deliveries-Product Price", "Product Price", "Std Product Price"],
    "STD_MKT": ["Standard Deliveries-Market Price", "Market Price", "Standard Market Price"],

    "STD_Q1": ["Standard Queue 1-Level", "Standard Q1-Level", "Queue 1-Level", "Queue1 Level"],
    "STD_Q2": ["Standard Queue 2-Level", "Standard Q2-Level", "Queue 2-Level", "Queue2 Level"],
    "STD_Q3": ["Standard Queue 3-Level", "Standard Q3-Level", "Queue 3-Level", "Queue3 Level"],
    "STD_Q4": ["Standard Queue 4-Level", "Standard Q4-Level", "Queue 4-Level", "Queue4 Level"],
    "STD_Q5": ["Standard Queue 5-Level", "Standard Q5-Level", "Queue 5-Level", "Queue5 Level"],

    "STD_MACHINES": ["Standard Station 1-Number of Machines", "Station 1-Number of Machines", "Number of Machines"],
    "STD_S1_OUT": ["Standard Station 1-Output", "Station 1-Output", "Output"],
    "STD_IB_OUT": ["Standard Initial Batching-Output", "Initial Batching-Output"],
    "STD_MP_OUT": ["Standard Manual Processing-Output", "Manual Processing-Output"],
    "STD_FB_OUT": ["Standard Final Batching-Output", "Final Batching-Output"],
    "STD_EWL": ["Standard Manual Processing-Effective Work Load (%)", "Effective Work Load (%)", "Effective Work Load"],

    # Custom (optional)
    "CUS_DEMAND": ["Custom Orders-Demand", "Daily Demand", "Demand"],
    "CUS_ACCEPT": ["Custom Orders-Accepted Orders", "Custom Accepted Orders", "Accepted Orders"],
    "CUS_ACCUM": ["Custom Orders-Accumulated Orders", "Custom Accumulated Orders", "Accumulated Orders"],
    "CUS_DELIV": ["Custom Deliveries-Deliveries", "Deliveries", "Deliveries Out"],
    "CUS_LT": ["Custom Deliveries-Average Lead Time", "Average Lead Time", "Lead Time"],
    "CUS_PRICE": ["Custom Deliveries-Actual Price", "Actual Price"],

    "CUS_Q1": ["Custom Queue 1-Level", "Queue 1-Level", "Level", "Queue1 Level"],
    "CUS_Q2_1": ["Custom Queue 2-Level First Pass", "Level First Pass", "Q2 First Pass"],
    "CUS_Q2_2": ["Custom Queue 2-Level Second Pass", "Level Second Pass", "Q2 Second Pass"],
    "CUS_Q3": ["Custom Queue 3-Level", "Queue 3-Level", "Level", "Queue3 Level"],

    "CUS_S1_OUT": ["Custom Station 1-Output", "Output"],
    "CUS_S2_MACH": ["Custom Station 2-Number of Machines", "Number of Machines"],
    "CUS_S2_OUT_1": ["Custom Station 2-Output First Pass", "Output First Pass"],
    "CUS_S3_MACH": ["Custom Station 3-Number of Machines", "Number of Machines"],
    "CUS_S3_OUT": ["Custom Station 3-Output", "Output"],
}

# -----------------------------
# Cheat constants / Defaults
# (ปรับเองได้ใน UI)
# -----------------------------
@dataclass
class GameConstants:
    lead_time_days: float = 4.0            # D: lead time = 4 วัน ของมาถึงวันที่ 5
    cost_per_part: float = 45.0
    order_fee: float = 1500.0
    holding_cost_per_part_day: float = 1.0

    std_parts_per_unit: float = 2.0
    cus_parts_per_unit: float = 1.0

    normal_debt_apr: float = 0.365
    cash_interest_daily: float = 0.0005
    loan_commission_rate: float = 0.02     # E: commission ทุกครั้งที่กู้

    days_to_expert: float = 15.0
    rookie_prod_vs_expert: float = 0.40
    salary_rookie_per_day: float = 80.0
    salary_expert_per_day: float = 150.0

@dataclass
class MachinePrices:
    s1_buy: float = 18000.0
    s2_buy: float = 12000.0
    s3_buy: float = 10000.0

# -----------------------------
# Per-user session isolation
# -----------------------------
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
        "constants": GameConstants(),
        "machine_prices": MachinePrices(),
        "ui_goal": {"profit": 1.0, "cash_end": 1.0, "risk": 1.0, "debt": 1.0},
    }
S = st.session_state.sessions[SID]

# -----------------------------
# Read & normalize timeseries
# -----------------------------
def normalize_day(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None:
        return None
    dcol = pick_col(df, COL["DAY"])
    if not dcol:
        return None
    out = df.copy()
    out["Day"] = pd.to_numeric(out[dcol], errors="coerce").fillna(-1).astype(int)
    out = out[out["Day"] >= 0].sort_values("Day")
    out = out.reset_index(drop=True)
    return out

def make_timeseries(xbytes: bytes):
    xl = excel_file_from_bytes(xbytes)
    std_df = normalize_day(read_sheet(xl, "Standard"))
    cus_df = normalize_day(read_sheet(xl, "Custom"))
    inv_df = normalize_day(read_sheet(xl, "Inventory"))
    fin_df = normalize_day(read_sheet(xl, "Finance", "Financial"))
    wf_df  = normalize_day(read_sheet(xl, "WorkForce", "Workforce"))
    return std_df, cus_df, inv_df, fin_df, wf_df

def finance_daily_delta(fin_ts: pd.DataFrame) -> pd.DataFrame:
    df = fin_ts.sort_values("Day").copy()

    def s(aliases): return as_numeric_series(df, pick_col(df, aliases))

    sales_std_td = s(COL["FIN_SALES_STD_TD"])
    sales_cus_td = s(COL["FIN_SALES_CUS_TD"])
    salaries_td  = s(COL["FIN_SALARIES_TD"])
    h_raw_td     = s(COL["FIN_HOLD_RAW_TD"])
    h_cus_td     = s(COL["FIN_HOLD_CUS_TD"])
    h_std_td     = s(COL["FIN_HOLD_STD_TD"])
    int_td       = s(COL["FIN_DEBT_INT_TD"])
    com_td       = s(COL["FIN_LOAN_COM_TD"])

    out = pd.DataFrame({"Day": df["Day"]})
    out["Sales_per_Day"] = (sales_std_td + sales_cus_td).diff().fillna(0.0)
    out["Costs_Proxy_per_Day"] = (salaries_td + h_raw_td + h_cus_td + h_std_td + int_td + com_td).diff().fillna(0.0)
    out["Profit_Proxy_per_Day"] = out["Sales_per_Day"] - out["Costs_Proxy_per_Day"]

    cash_col = pick_col(df, COL["CASH"])
    debt_col = pick_col(df, COL["DEBT"])
    if cash_col:
        out["Cash_On_Hand"] = as_numeric_series(df, cash_col)
    if debt_col:
        out["Debt"] = as_numeric_series(df, debt_col)

    # daily interest proxy from debt changes is not reliable; we keep it simple.
    return out

# -----------------------------
# Build Standard intelligence dataset
# -----------------------------
def build_standard_df(std_ts: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"Day": std_ts["Day"]})

    price_c = pick_col(std_ts, COL["STD_PRICE"])
    mkt_c   = pick_col(std_ts, COL["STD_MKT"])
    acc_c   = pick_col(std_ts, COL["STD_ACCEPT"])
    del_c   = pick_col(std_ts, COL["STD_DELIV"])
    accum_c = pick_col(std_ts, COL["STD_ACCUM"])
    ewl_c   = pick_col(std_ts, COL["STD_EWL"])
    mpout_c = pick_col(std_ts, COL["STD_MP_OUT"])

    df["Price"]       = as_numeric_series(std_ts, price_c)
    df["Market"]      = as_numeric_series(std_ts, mkt_c)
    df["Accepted"]    = as_numeric_series(std_ts, acc_c)
    df["Deliveries"]  = as_numeric_series(std_ts, del_c)
    df["Accumulated"] = as_numeric_series(std_ts, accum_c)
    df["EWL"]         = as_numeric_series(std_ts, ewl_c)
    df["MP_Out"]      = as_numeric_series(std_ts, mpout_c)

    # PriceGap% (safe)
    df["MarketSafe"] = df["Market"].replace(0, np.nan)
    df["PriceGapPct"] = (df["Price"] - df["MarketSafe"]) / df["MarketSafe"]
    df["PriceGapPct"] = df["PriceGapPct"].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Backlog proxy
    df["BacklogProxy"] = (df["Accumulated"] - df["Deliveries"]).clip(lower=0.0)

    # Fill rate proxy
    df["FillRate"] = df["Deliveries"] / df["Accepted"].replace(0, np.nan)
    df["FillRate"] = df["FillRate"].fillna(1.0).clip(0.0, 2.0)

    # Minimal usable rows
    df = df[(df["Price"] > 0) & (df["Accepted"] >= 0)]
    df = df.reset_index(drop=True)
    return df

# -----------------------------
# Learn demand response:
# Accepted(t) ~ a + b*PriceGapPct(t-lag)
# Also learn deliveries sensitivity:
# Deliveries(t) ~ c + d*PriceGapPct(t-lag)  (usually weaker)
# -----------------------------
def fit_lin(x: np.ndarray, y: np.ndarray) -> Optional[Tuple[float, float, float]]:
    if len(x) < 12:
        return None
    if np.unique(x).size < 3:
        return None
    xm, ym = x.mean(), y.mean()
    varx = ((x - xm) ** 2).sum()
    if varx <= 1e-9:
        return None
    cov = ((x - xm) * (y - ym)).sum()
    b = cov / varx
    a = ym - b * xm
    yhat = a + b * x
    ss_res = ((y - yhat) ** 2).sum()
    ss_tot = ((y - ym) ** 2).sum() + 1e-9
    r2 = 1.0 - ss_res / ss_tot
    return float(a), float(b), float(r2)

def learn_price_models(std_df: pd.DataFrame, max_lag: int = 3) -> Dict[str, float]:
    """
    Returns best lag model for Accepted and Deliveries wrt PriceGapPct.
    """
    best = {
        "lag": 0,
        "a_acc": 0.0, "b_acc": 0.0, "r2_acc": -999.0,
        "a_del": 0.0, "b_del": 0.0, "r2_del": -999.0,
        "n": 0,
    }
    if std_df is None or std_df.empty:
        return best

    for lag in range(0, max_lag + 1):
        x = std_df["PriceGapPct"].shift(lag).values
        y_acc = std_df["Accepted"].values
        y_del = std_df["Deliveries"].values

        mask = np.isfinite(x) & np.isfinite(y_acc) & np.isfinite(y_del)
        x1 = x[mask].astype(float)
        ya = y_acc[mask].astype(float)
        yd = y_del[mask].astype(float)
        if len(x1) < 12:
            continue

        fa = fit_lin(x1, ya)
        fd = fit_lin(x1, yd)
        if fa is None:
            continue

        # choose by Accepted model quality first
        r2a = fa[2]
        if r2a > best["r2_acc"]:
            best["lag"] = lag
            best["a_acc"], best["b_acc"], best["r2_acc"] = fa
            if fd is not None:
                best["a_del"], best["b_del"], best["r2_del"] = fd
            best["n"] = int(len(x1))

    # sanity: if slope wrong direction (b_acc positive strongly), still keep but warn later
    return best

# -----------------------------
# Capacity estimation
# -----------------------------
def estimate_capacity(std_df: pd.DataFrame, window: int = 20) -> float:
    """
    Estimate effective max deliveries/day (capacity proxy).
    Use recent window: pick high-quantile deliveries where not stockout-like.
    """
    if std_df is None or std_df.empty:
        return 0.0
    df = std_df.tail(window).copy()
    d = df["Deliveries"].astype(float).values
    if len(d) == 0:
        return 0.0
    # robust: take median of top 5 deliveries
    top = np.sort(d)[-min(5, len(d)):]
    cap = float(np.median(top)) if len(top) else float(np.max(d))
    return max(0.0, cap)

# -----------------------------
# Regime detection (smart logic)
# -----------------------------
def detect_regime(std_last: pd.Series) -> str:
    """
    Regimes:
    - "CAPACITY_CONSTRAINED": EWL high OR fill rate low OR backlog > 0
    - "DEMAND_CONSTRAINED": fill rate ~1 and backlog ~0 but accepted low vs historical
    - "BALANCED": none strong
    """
    ewl = float(std_last.get("EWL", 0.0))
    fill = float(std_last.get("FillRate", 1.0))
    backlog = float(std_last.get("BacklogProxy", 0.0))
    if (ewl >= 95.0) or (fill < 0.98) or (backlog > 0):
        return "CAPACITY_CONSTRAINED"
    if (fill >= 0.995) and (backlog <= 0.0) and (ewl < 90.0):
        return "DEMAND_CONSTRAINED"
    return "BALANCED"

# -----------------------------
# Price suggestion (capacity-aware optimizer)
# -----------------------------
def suggest_price_today(
    std_df: pd.DataFrame,
    model: Dict[str, float],
    cap: float,
    market_today: float,
    price_today: float,
    risk_tolerance: float = 1.0,
) -> Dict[str, float]:
    """
    Choose price multiplier among candidates to maximize Revenue/day proxy
    with backlog/fill risk penalty.
    """
    if market_today <= 0 and price_today > 0:
        market_today = price_today
    if market_today <= 0:
        return {"suggested_price": price_today, "method": 0.0, "reason_code": 0.0}

    last = std_df.iloc[-1] if (std_df is not None and len(std_df) > 0) else None
    regime = detect_regime(last) if last is not None else "BALANCED"

    # Candidate multipliers
    if regime == "CAPACITY_CONSTRAINED":
        # prioritize reducing demand to match capacity
        cand = np.array([1.00, 1.03, 1.05, 1.08, 1.10, 1.12, 1.15])
    else:
        # explore both sides
        cand = np.array([0.85, 0.90, 0.95, 0.98, 1.00, 1.02, 1.05, 1.08, 1.10, 1.12, 1.15])

    # Demand model: Accepted = a + b*gap
    a = float(model.get("a_acc", 0.0))
    b = float(model.get("b_acc", 0.0))
    r2 = float(model.get("r2_acc", -999.0))

    # fallback demand baseline = recent mean accepted
    base_acc = float(std_df["Accepted"].tail(10).mean()) if (std_df is not None and len(std_df) >= 10) else float(std_df["Accepted"].mean()) if std_df is not None and len(std_df) else 0.0

    def predict_accepted(price: float) -> float:
        gap = (price - market_today) / market_today
        if (r2 > -0.2) and (abs(b) > 1e-9):
            return max(0.0, a + b * gap)
        # heuristic elasticity: +10% price => -5% accepted
        pct = (price - market_today) / market_today
        return max(0.0, base_acc * (1.0 - 0.5 * pct))

    best = {"price": price_today, "score": -1e18, "acc": 0.0, "sold": 0.0, "rev": 0.0, "risk": 0.0, "regime": regime}

    # risk signals from last
    backlog = float(last.get("BacklogProxy", 0.0)) if last is not None else 0.0
    fill = float(last.get("FillRate", 1.0)) if last is not None else 1.0
    ewl = float(last.get("EWL", 0.0)) if last is not None else 0.0

    # penalty weights
    w_risk = 1.0 * risk_tolerance
    w_backlog = 0.8 * risk_tolerance
    w_fill = 1.2 * risk_tolerance

    for m in cand:
        p = float(market_today * float(m))
        acc = predict_accepted(p)

        # capacity constraint: sold cannot exceed cap
        sold = min(acc, cap) if cap > 0 else acc
        rev = p * sold

        # risk penalty: if we are already backlog/fill bad, penalize choices that increase demand above cap
        overload = max(0.0, acc - (cap if cap > 0 else acc))
        risk = (overload) + (backlog * 0.2) + (max(0.0, 0.98 - fill) * base_acc * 2.0) + (max(0.0, ewl - 95.0) * 0.05)

        # score: revenue - penalties
        score = rev - w_risk * (w_backlog * backlog + w_fill * risk)

        if score > best["score"]:
            best.update({"price": p, "score": score, "acc": acc, "sold": sold, "rev": rev, "risk": risk})

    method = 1.0 if r2 > -0.2 else 2.0
    return {
        "suggested_price": float(best["price"]),
        "method": method,
        "regime": best["regime"],
        "pred_accepted": float(best["acc"]),
        "pred_sold": float(best["sold"]),
        "pred_rev_per_day": float(best["rev"]),
        "risk_proxy": float(best["risk"]),
        "model_r2": float(r2),
        "model_b": float(b),
    }

# -----------------------------
# Inventory policy (ROP/ROQ) with cash-aware tweak
# -----------------------------
def recommend_inventory_policy(
    constants: GameConstants,
    std_accepted_per_day: float,
    cus_demand_per_day: float,
    inventory_on_hand_parts: float,
    cash_on_hand: float,
) -> Dict[str, float]:
    # parts/day
    D_parts = (std_accepted_per_day * constants.std_parts_per_unit) + (cus_demand_per_day * constants.cus_parts_per_unit)

    rop = D_parts * constants.lead_time_days
    # EOQ: sqrt(2DS/H)
    if D_parts > 0:
        roq = math.sqrt((2.0 * D_parts * constants.order_fee) / max(1e-9, constants.holding_cost_per_part_day))
    else:
        roq = 0.0

    # cash-aware tweak: if low cash, reduce ROQ (buy smaller batches more often)
    # simple rule: if cash < 2*order_fee => cut ROQ by 35%
    if cash_on_hand > 0 and cash_on_hand < 2.0 * constants.order_fee:
        roq *= 0.65

    coverage_days = safe_div(inventory_on_hand_parts, D_parts, default=0.0)

    return {
        "parts_per_day": float(D_parts),
        "rop": float(rop),
        "roq": float(roq),
        "coverage_days": float(coverage_days),
    }

# -----------------------------
# Loan helper
# -----------------------------
def loan_cost_per_day(constants: GameConstants, loan_amount: float, spread_days: int = 30) -> float:
    apr = constants.normal_debt_apr
    com = constants.loan_commission_rate
    interest_per_day = loan_amount * (apr / 365.0)
    commission_per_day = (loan_amount * com) / max(1, int(spread_days))
    return float(interest_per_day + commission_per_day)

# -----------------------------
# Cost model for forecasting: Costs/day ≈ fixed + var * sold
# Use finance proxy + sold proxy from deliveries (Std + Cus)
# -----------------------------
def fit_cost_model(fin_daily: pd.DataFrame, sold_proxy: pd.Series) -> Dict[str, float]:
    """
    OLS on last N days: cost = c0 + c1*sold
    If not enough data, fallback to averages.
    """
    out = {"c0": 0.0, "c1": 0.0, "r2": 0.0}
    if fin_daily is None or fin_daily.empty:
        return out
    if "Costs_Proxy_per_Day" not in fin_daily.columns:
        return out

    df = fin_daily.copy()
    df["SoldProxy"] = sold_proxy.reindex(df.index).fillna(0.0).astype(float).values
    df = df.tail(60).copy()

    y = df["Costs_Proxy_per_Day"].astype(float).values
    x = df["SoldProxy"].astype(float).values

    if len(x) < 20 or np.unique(x).size < 3:
        # fallback: assume mostly fixed cost
        out["c0"] = float(np.nanmean(y)) if len(y) else 0.0
        out["c1"] = 0.0
        out["r2"] = 0.0
        return out

    X = np.vstack([np.ones_like(x), x]).T
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    c0, c1 = float(beta[0]), float(beta[1])

    yhat = c0 + c1 * x
    ss_res = float(((y - yhat) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum() + 1e-9)
    r2 = 1.0 - ss_res / ss_tot

    out.update({"c0": c0, "c1": c1, "r2": float(r2)})
    return out

# -----------------------------
# Forecast 100 days simulator (policy-driven)
# -----------------------------
def simulate_100_days(
    constants: GameConstants,
    std_df: pd.DataFrame,
    fin_daily: Optional[pd.DataFrame],
    inv_ts: Optional[pd.DataFrame],
    model: Dict[str, float],
    horizon: int = 100,
    risk_tolerance: float = 1.0,
) -> pd.DataFrame:
    """
    Conservative simulator:
      - Demand from price model using PriceGapPct and market
      - Sold limited by capacity proxy
      - Cash updated using Profit proxy: revenue - (fixed + var*sold) - loan_cost(if any)
      - Inventory parts tracked with leadtime pipeline (ROP/ROQ)
    """
    if std_df is None or std_df.empty:
        return pd.DataFrame()

    last = std_df.iloc[-1]
    day0 = int(last["Day"])
    market0 = float(last["Market"]) if float(last["Market"]) > 0 else float(last["Price"])
    price0 = float(last["Price"])
    accepted0 = float(last["Accepted"])
    deliveries0 = float(last["Deliveries"])
    backlog0 = float(last["BacklogProxy"])
    ewl0 = float(last.get("EWL", 0.0))

    # capacity proxy
    cap = estimate_capacity(std_df, window=20)
    if cap <= 0:
        cap = max(1.0, float(std_df["Deliveries"].tail(10).mean()))

    # cash/debt from finance if exists
    cash0, debt0 = 0.0, 0.0
    if fin_daily is not None and not fin_daily.empty and "Cash_On_Hand" in fin_daily.columns:
        cash0 = float(fin_daily["Cash_On_Hand"].iloc[-1])
    if fin_daily is not None and not fin_daily.empty and "Debt" in fin_daily.columns:
        debt0 = float(fin_daily["Debt"].iloc[-1])

    # inventory parts on hand (best effort)
    inv0 = 0.0
    if inv_ts is not None and not inv_ts.empty:
        inv_col = pick_col(inv_ts, COL["INV_LEVEL"])
        if inv_col:
            inv0 = float(as_numeric_series(inv_ts, inv_col).iloc[-1])

    # sold proxy series for cost fit: use deliveries (std) only for standard
    sold_proxy = std_df["Deliveries"].astype(float)
    cost_model = fit_cost_model(fin_daily, sold_proxy) if fin_daily is not None else {"c0": 0.0, "c1": 0.0, "r2": 0.0}

    # Leadtime pipeline for parts ordering
    pipeline = [0.0] * (int(constants.lead_time_days) + 1)  # arrival at index LT
    # For simulation: each day we may place an order of ROQ parts, arrives after LT days.

    rows = []
    price_today = price0
    market_today = market0
    backlog = backlog0
    cash = cash0
    debt = debt0
    inv_parts = inv0

    # baseline custom demand unknown here → use 0 in simulator unless user extends
    cus_demand = 0.0

    # model parameters
    a = float(model.get("a_acc", 0.0))
    b = float(model.get("b_acc", 0.0))
    r2 = float(model.get("r2_acc", -999.0))

    base_acc = float(std_df["Accepted"].tail(10).mean()) if len(std_df) >= 10 else float(std_df["Accepted"].mean())

    def predict_accepted(price: float) -> float:
        if market_today <= 0:
            return max(0.0, base_acc)
        gap = (price - market_today) / market_today
        if r2 > -0.2 and abs(b) > 1e-9:
            return max(0.0, a + b * gap)
        pct = (price - market_today) / market_today
        return max(0.0, base_acc * (1.0 - 0.5 * pct))

    for k in range(1, horizon + 1):
        day = day0 + k

        # Arrivals from pipeline
        arrived = pipeline.pop(0)
        inv_parts += arrived
        pipeline.append(0.0)

        # Regime from current state (use synthetic)
        fill_proxy = 1.0 if accepted0 <= 0 else min(2.0, safe_div(deliveries0, accepted0, 1.0))
        regime = "CAPACITY_CONSTRAINED" if (backlog > 0 or fill_proxy < 0.98 or ewl0 >= 95) else "BALANCED"

        # Choose price (policy)
        sugg = suggest_price_today(
            std_df=std_df.tail(50).copy(),
            model=model,
            cap=cap,
            market_today=market_today,
            price_today=price_today,
            risk_tolerance=risk_tolerance,
        )
        price_today = float(sugg["suggested_price"])

        # Demand
        accepted = predict_accepted(price_today)

        # Parts needed for production (sold units)
        # We'll try to sell up to capacity, but limited by inventory parts too.
        sold_cap = min(accepted, cap)

        # inventory constraint:
        # need std_parts_per_unit parts per unit
        max_by_inv = safe_div(inv_parts, constants.std_parts_per_unit, default=0.0)
        sold = min(sold_cap, max_by_inv)

        # consume parts
        inv_parts -= sold * constants.std_parts_per_unit

        # backlog evolution
        backlog = max(0.0, backlog + accepted - sold)

        # inventory policy for ordering
        inv_policy = recommend_inventory_policy(
            constants=constants,
            std_accepted_per_day=accepted,
            cus_demand_per_day=cus_demand,
            inventory_on_hand_parts=inv_parts,
            cash_on_hand=cash,
        )
        rop, roq = float(inv_policy["rop"]), float(inv_policy["roq"])

        order_qty = 0.0
        order_cost = 0.0
        if inv_parts < rop and roq > 0:
            # place order
            order_qty = roq
            pipeline[int(constants.lead_time_days)] += order_qty  # arrives after LT
            order_cost = order_qty * constants.cost_per_part + constants.order_fee
            cash -= order_cost

        # revenue & cost model
        revenue = price_today * sold
        cost = float(cost_model.get("c0", 0.0)) + float(cost_model.get("c1", 0.0)) * sold

        # holding cost (approx)
        holding = inv_parts * constants.holding_cost_per_part_day

        profit = revenue - cost - holding

        # debt interest on current debt
        debt_interest = debt * (constants.normal_debt_apr / 365.0)
        profit -= debt_interest

        cash += profit

        # update accepted0/deliveries0 for next loop (synthetic)
        accepted0 = accepted
        deliveries0 = sold

        rows.append({
            "Day": day,
            "Regime": regime,
            "Market": market_today,
            "Price": price_today,
            "Accepted_Forecast": accepted,
            "Capacity_Proxy": cap,
            "Sold_Forecast": sold,
            "Backlog_Forecast": backlog,
            "InvParts_End": inv_parts,
            "OrderQty": order_qty,
            "OrderCost": order_cost,
            "Revenue": revenue,
            "CostModel_Cost": cost,
            "HoldingCost": holding,
            "DebtInterest": debt_interest,
            "Profit_Forecast": profit,
            "Cash_Forecast": cash,
            "ROP": rop,
            "ROQ": roq,
        })

        # drift market (optional): keep constant (safer); user can extend later.

    return pd.DataFrame(rows)

# ============================================================
# UI (Professional)
# ============================================================
st.set_page_config(page_title="Factory Autopilot Analyzer", layout="wide")

with st.container():
    a, b = st.columns([2.2, 1])
    with a:
        st.title("🏭 Factory Autopilot Analyzer — Medica Scientific")
        st.caption("Upload Excel → Analyze (Snapshot + Trends) → Autopilot Suggest → Forecast 100 days (conservative, capacity-aware)")
    with b:
        st.markdown("#### Session")
        st.code(SID[:8])
        if st.button("🔄 Reset (เฉพาะฉัน)"):
            st.session_state.pop("sid", None)
            st.rerun()

tabs = st.tabs([
    "0) Upload",
    "1) Snapshot",
    "2) Trends",
    "3) Price Intelligence",
    "4) Autopilot Today",
    "5) Forecast 100 Days",
    "6) Settings",
])

# -----------------------------
# Tab 0: Upload
# -----------------------------
with tabs[0]:
    st.subheader("Upload .xlsx (Export จากเกม)")
    up = st.file_uploader("เลือกไฟล์ .xlsx", type=["xlsx"])
    if up is not None:
        try:
            xbytes = up.getvalue()
            S["last_uploaded_bytes"] = xbytes
            std_ts, cus_ts, inv_ts, fin_ts, wf_ts = make_timeseries(xbytes)

            # find max day
            days = []
            for d in [std_ts, cus_ts, inv_ts, fin_ts, wf_ts]:
                if d is not None and not d.empty:
                    days.append(int(d["Day"].max()))
            max_day = max(days) if days else 0
            S["import_day"] = max_day

            st.success(f"✅ Uploaded! Detected max Day = {max_day}")
            st.write("ไปต่อที่แท็บ Snapshot/Trends ได้เลย")

        except ImportError as e:
            st.error("อ่านไฟล์ .xlsx ไม่ได้ (ขาด openpyxl)")
            st.code("เพิ่มใน requirements.txt:\nopenpyxl")
            st.exception(e)
        except Exception as e:
            st.error("Upload/Parse ล้มเหลว")
            st.exception(e)

# -----------------------------
# Tab 6: Settings (constants + goals)
# -----------------------------
with tabs[6]:
    st.subheader("Settings (Game constants + Goals)")
    c: GameConstants = S["constants"]
    mp: MachinePrices = S["machine_prices"]

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### ⏱ Supply / Inventory")
        c.lead_time_days = st.number_input("Lead time (days) (D)", value=float(c.lead_time_days), step=1.0)
        c.cost_per_part = st.number_input("Cost per part", value=float(c.cost_per_part), step=1.0)
        c.order_fee = st.number_input("Order fee", value=float(c.order_fee), step=100.0)
        c.holding_cost_per_part_day = st.number_input("Holding cost / part / day", value=float(c.holding_cost_per_part_day), step=0.1)

        st.markdown("### 🔩 Parts per unit")
        c.std_parts_per_unit = st.number_input("Standard parts/unit", value=float(c.std_parts_per_unit), step=0.5)
        c.cus_parts_per_unit = st.number_input("Custom parts/unit", value=float(c.cus_parts_per_unit), step=0.5)

    with col2:
        st.markdown("### 🏦 Loan / Interest")
        c.normal_debt_apr = st.number_input("Debt APR", value=float(c.normal_debt_apr), step=0.01, format="%.3f")
        c.loan_commission_rate = st.number_input("Loan commission rate", value=float(c.loan_commission_rate), step=0.005, format="%.3f")
        c.cash_interest_daily = st.number_input("Cash interest (daily)", value=float(c.cash_interest_daily), step=0.0001, format="%.4f")

        st.markdown("### 👷 Workforce (for later extensions)")
        c.days_to_expert = st.number_input("Days to become expert", value=float(c.days_to_expert), step=1.0)
        c.rookie_prod_vs_expert = st.number_input("Rookie productivity vs expert", value=float(c.rookie_prod_vs_expert), step=0.05)
        c.salary_rookie_per_day = st.number_input("Rookie salary/day", value=float(c.salary_rookie_per_day), step=10.0)
        c.salary_expert_per_day = st.number_input("Expert salary/day", value=float(c.salary_expert_per_day), step=10.0)

    with col3:
        st.markdown("### 🏭 Machine prices (your run may differ)")
        mp.s1_buy = st.number_input("Buy S1", value=float(mp.s1_buy), step=1000.0)
        mp.s2_buy = st.number_input("Buy S2", value=float(mp.s2_buy), step=1000.0)
        mp.s3_buy = st.number_input("Buy S3", value=float(mp.s3_buy), step=1000.0)

        st.markdown("### 🎯 Goals (weights)")
        g = S["ui_goal"]
        g["profit"] = st.slider("Weight: Profit/day", 0.0, 3.0, float(g["profit"]), 0.1)
        g["cash_end"] = st.slider("Weight: Cash endgame", 0.0, 3.0, float(g["cash_end"]), 0.1)
        g["risk"] = st.slider("Weight: Backlog/Service risk", 0.0, 3.0, float(g["risk"]), 0.1)
        g["debt"] = st.slider("Weight: Debt risk", 0.0, 3.0, float(g["debt"]), 0.1)

    st.info("✅ Settings saved (session-isolated).")

# Guard
if S["last_uploaded_bytes"] is None:
    with tabs[1]:
        st.info("อัปโหลดไฟล์ก่อนในแท็บ Upload")
    with tabs[2]:
        st.info("อัปโหลดไฟล์ก่อนในแท็บ Upload")
    with tabs[3]:
        st.info("อัปโหลดไฟล์ก่อนในแท็บ Upload")
    with tabs[4]:
        st.info("อัปโหลดไฟล์ก่อนในแท็บ Upload")
    with tabs[5]:
        st.info("อัปโหลดไฟล์ก่อนในแท็บ Upload")
    st.stop()

# Parse once for other tabs
std_ts, cus_ts, inv_ts, fin_ts, wf_ts = make_timeseries(S["last_uploaded_bytes"])
std_df = build_standard_df(std_ts) if (std_ts is not None and not std_ts.empty) else pd.DataFrame()
fin_daily = finance_daily_delta(fin_ts) if (fin_ts is not None and not fin_ts.empty) else None

# -----------------------------
# Tab 1: Snapshot
# -----------------------------
with tabs[1]:
    st.subheader("Snapshot (Latest Day) — Diagnose + Reasons")
    if std_df.empty:
        st.warning("ไม่พบ/อ่านชีท Standard ได้ไม่ครบ")
    else:
        last = std_df.iloc[-1]
        cap = estimate_capacity(std_df, window=20)
        model = learn_price_models(std_df, max_lag=3)
        regime = detect_regime(last)

        # inventory + cash
        cash = float(fin_daily["Cash_On_Hand"].iloc[-1]) if (fin_daily is not None and "Cash_On_Hand" in fin_daily.columns) else 0.0
        debt = float(fin_daily["Debt"].iloc[-1]) if (fin_daily is not None and "Debt" in fin_daily.columns) else 0.0
        inv_parts = 0.0
        if inv_ts is not None and not inv_ts.empty:
            inv_col = pick_col(inv_ts, COL["INV_LEVEL"])
            if inv_col:
                inv_parts = float(as_numeric_series(inv_ts, inv_col).iloc[-1])

        # custom demand best-effort
        cus_demand = 0.0
        if cus_ts is not None and not cus_ts.empty:
            dcol = pick_col(cus_ts, COL["CUS_DEMAND"])
            if dcol:
                cus_demand = float(as_numeric_series(cus_ts, dcol).iloc[-1])

        inv_policy = recommend_inventory_policy(
            constants=S["constants"],
            std_accepted_per_day=float(last["Accepted"]),
            cus_demand_per_day=float(cus_demand),
            inventory_on_hand_parts=float(inv_parts),
            cash_on_hand=float(cash),
        )

        # key metrics
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric("Day", str(int(last["Day"])))
        k2.metric("Regime", regime)
        k3.metric("Accepted", num(last["Accepted"]))
        k4.metric("Deliveries", num(last["Deliveries"]))
        k5.metric("Fill rate", num(last["FillRate"]))
        k6.metric("Backlog", num(last["BacklogProxy"]))

        k7, k8, k9, k10 = st.columns(4)
        k7.metric("Price / Market", f"{num(last['Price'])} / {num(last['Market'])}")
        k8.metric("PriceGap%", f"{num(last['PriceGapPct']*100)}%")
        k9.metric("Capacity proxy", num(cap))
        k10.metric("Cash / Debt", f"{money(cash)} / {money(debt)}")

        reasons = []
        if inv_policy["coverage_days"] < S["constants"].lead_time_days:
            reasons.append(f"Raw coverage ~{num(inv_policy['coverage_days'])} days < lead time {num(S['constants'].lead_time_days)} → เสี่ยง stockout → ส่งของสะดุด → เงินสดพัง")
        if regime == "CAPACITY_CONSTRAINED":
            reasons.append("สัญญาณติด capacity/backlog → ลดราคาจะทำให้ Accepted พุ่งแต่ Deliveries ไม่ขึ้น → backlog โต")
        if float(model.get("b_acc", 0.0)) > 0.0:
            reasons.append("⚠️ โมเดลพบ slope บวก (ราคาแพงขึ้นแต่ Accepted เพิ่ม) อาจเกิดจากข้อมูลไม่หลากหลาย/ติด capacity → อย่าเชื่อ regression มาก")
        if not reasons:
            reasons.append("ไม่พบสัญญาณผิดปกติเด่นจาก snapshot")

        with st.expander("Why (เหตุผลหลัก)", expanded=True):
            for r in reasons:
                st.write(f"- {r}")

        st.markdown("### Inventory policy (Today)")
        c1, c2, c3 = st.columns(3)
        c1.metric("Parts/day", num(inv_policy["parts_per_day"]))
        c2.metric("ROP", num(inv_policy["rop"]))
        c3.metric("ROQ", num(inv_policy["roq"]))
        st.caption(f"Coverage days: {num(inv_policy['coverage_days'])}")

# -----------------------------
# Tab 2: Trends
# -----------------------------
with tabs[2]:
    st.subheader("Trends (Full-file)")
    if fin_daily is not None and not fin_daily.empty:
        c1 = [c for c in ["Cash_On_Hand", "Debt"] if c in fin_daily.columns]
        if c1:
            st.markdown("#### 💵 Cash & Debt")
            st.line_chart(fin_daily.set_index("Day")[c1], height=220)

        c2 = [c for c in ["Sales_per_Day", "Costs_Proxy_per_Day", "Profit_Proxy_per_Day"] if c in fin_daily.columns]
        if c2:
            st.markdown("#### 📊 Sales / Cost / Profit (Proxy) per Day")
            st.line_chart(fin_daily.set_index("Day")[c2], height=220)
    else:
        st.warning("ไม่เจอ Finance (to-date) เพียงพอสำหรับ Profit proxy")

    if inv_ts is not None and not inv_ts.empty:
        inv_col = pick_col(inv_ts, COL["INV_LEVEL"])
        if inv_col:
            st.markdown("#### 📦 Inventory parts")
            st.line_chart(inv_ts.set_index("Day")[[inv_col]], height=200)

    if not std_df.empty:
        st.markdown("#### 🧱 Standard — Accepted vs Deliveries")
        st.line_chart(std_df.set_index("Day")[["Accepted", "Deliveries"]], height=220)

        st.markdown("#### 🧱 Standard — Price vs Market")
        st.line_chart(std_df.set_index("Day")[["Price", "Market"]], height=200)

        if "EWL" in std_df.columns and "MP_Out" in std_df.columns:
            st.markdown("#### 🧱 Standard — Manual Processing (EWL & Output)")
            st.line_chart(std_df.set_index("Day")[["EWL", "MP_Out"]], height=220)

# -----------------------------
# Tab 3: Price Intelligence
# -----------------------------
with tabs[3]:
    st.subheader("Price Intelligence — Does price affect demand/deliveries?")
    if std_df.empty:
        st.warning("ไม่มีข้อมูล Standard usable")
    else:
        model = learn_price_models(std_df, max_lag=3)
        st.info(
            f"Learned model (best lag={int(model['lag'])}, n={int(model['n'])}) | "
            f"Accepted ≈ a + b*PriceGap%  (b={num(model['b_acc'])}, R²={num(model['r2_acc'])})"
        )

        # Show relationship table
        view = std_df.copy()
        view["PriceGap%"] = view["PriceGapPct"] * 100.0
        view_small = view[["Day", "Price", "Market", "PriceGap%", "Accepted", "Deliveries", "FillRate", "BacklogProxy", "EWL"]].tail(40)
        st.dataframe(view_small, use_container_width=True)

        st.markdown("### Quick Interpretation")
        b_acc = float(model.get("b_acc", 0.0))
        if b_acc < 0:
            st.success("✅ สอดคล้องกับคำเพื่อน: Price > Market (gap+) → Accepted ลด | Price < Market (gap-) → Accepted เพิ่ม")
        else:
            st.warning("⚠️ slope ไม่เป็นลบชัดเจน: อาจเพราะข้อมูลราคาไม่หลากหลาย หรือระบบติด capacity/backlog ทำให้ demand ที่เห็น 'เพี้ยน'")

        st.markdown("### Visual (sorted by PriceGap%)")
        tmp = std_df.sort_values("PriceGapPct")[["PriceGapPct", "Accepted", "Deliveries"]].reset_index(drop=True)
        tmp = tmp.rename(columns={"PriceGapPct": "PriceGapPct_sorted"})
        st.caption("ดูแนวโน้มว่า gap เปลี่ยน → Accepted/Deliveries เปลี่ยนยังไง (แบบเรียงลำดับ)")
        st.line_chart(tmp.set_index("PriceGapPct_sorted")[["Accepted", "Deliveries"]], height=240)

# -----------------------------
# Tab 4: Autopilot Today (Suggest settings)
# -----------------------------
with tabs[4]:
    st.subheader("Autopilot Today — Suggested actions (copy into game)")

    if std_df.empty:
        st.warning("ไม่มี Standard data")
    else:
        last = std_df.iloc[-1]
        cap = estimate_capacity(std_df, window=20)
        model = learn_price_models(std_df, max_lag=3)

        cash = float(fin_daily["Cash_On_Hand"].iloc[-1]) if (fin_daily is not None and "Cash_On_Hand" in fin_daily.columns) else 0.0
        debt = float(fin_daily["Debt"].iloc[-1]) if (fin_daily is not None and "Debt" in fin_daily.columns) else 0.0

        inv_parts = 0.0
        if inv_ts is not None and not inv_ts.empty:
            inv_col = pick_col(inv_ts, COL["INV_LEVEL"])
            if inv_col:
                inv_parts = float(as_numeric_series(inv_ts, inv_col).iloc[-1])

        cus_demand = 0.0
        if cus_ts is not None and not cus_ts.empty:
            dcol = pick_col(cus_ts, COL["CUS_DEMAND"])
            if dcol:
                cus_demand = float(as_numeric_series(cus_ts, dcol).iloc[-1])

        # risk tolerance from goal weights
        g = S["ui_goal"]
        risk_tolerance = float(g["risk"]) if g else 1.0

        price_sugg = suggest_price_today(
            std_df=std_df,
            model=model,
            cap=cap,
            market_today=float(last["Market"]),
            price_today=float(last["Price"]),
            risk_tolerance=risk_tolerance,
        )

        inv_policy = recommend_inventory_policy(
            constants=S["constants"],
            std_accepted_per_day=float(last["Accepted"]),
            cus_demand_per_day=float(cus_demand),
            inventory_on_hand_parts=float(inv_parts),
            cash_on_hand=float(cash),
        )

        method = "Regression/learned" if price_sugg["method"] == 1.0 else "Heuristic fallback"
        st.markdown("### ✅ Suggested Settings (Today)")
        st.json({
            "Std Product Price": float(price_sugg["suggested_price"]),
            "Std Price Method": method,
            "Regime": price_sugg["regime"],
            "Pred Accepted": float(price_sugg["pred_accepted"]),
            "Pred Sold (cap-aware)": float(price_sugg["pred_sold"]),
            "Pred Revenue/day (proxy)": float(price_sugg["pred_rev_per_day"]),
            "Inventory ROP (parts)": float(inv_policy["rop"]),
            "Inventory ROQ (parts)": float(inv_policy["roq"]),
            "Coverage Days": float(inv_policy["coverage_days"]),
            "Capacity Proxy (deliveries/day)": float(cap),
            "Cash": float(cash),
            "Debt": float(debt),
        })

        st.markdown("### Why these suggestions?")
        bullets = []
        if price_sugg["regime"] == "CAPACITY_CONSTRAINED":
            bullets.append("ระบบเห็นสัญญาณติด capacity/backlog → ตั้งราคาสูงขึ้นเพื่อคุม demand ไม่ให้ backlog โต")
        else:
            bullets.append("ระบบมองว่าสามารถ optimize ราคาเพื่อ revenue/day ได้ (แต่ยังคุม risk)")
        if inv_policy["coverage_days"] < S["constants"].lead_time_days:
            bullets.append("Raw coverage < lead time → ต้องยก ROP/ROQ เพื่อกัน stockout (ตัวทำเงินพังที่เร็วสุด)")
        else:
            bullets.append("Raw coverage พอ lead time → โอกาส stockout ต่ำลง → ส่งมอบนิ่งขึ้น")
        bullets.append(f"โมเดล demand: b={num(price_sugg['model_b'])}, R²={num(price_sugg['model_r2'])} (R² ต่ำ → ใช้ heuristic มากขึ้น)")

        for b in bullets:
            st.write(f"- {b}")

# -----------------------------
# Tab 5: Forecast 100 Days
# -----------------------------
with tabs[5]:
    st.subheader("Forecast 100 Days (Conservative Autopilot Simulation)")

    horizon = st.slider("Horizon (days)", 30, 150, 100, 5)
    risk_tol = st.slider("Risk tolerance (higher = more conservative)", 0.5, 3.0, float(S["ui_goal"]["risk"]), 0.1)

    if std_df.empty:
        st.warning("ไม่มี Standard data สำหรับ forecast")
    else:
        model = learn_price_models(std_df, max_lag=3)
        sim = simulate_100_days(
            constants=S["constants"],
            std_df=std_df,
            fin_daily=fin_daily,
            inv_ts=inv_ts,
            model=model,
            horizon=int(horizon),
            risk_tolerance=float(risk_tol),
        )

        if sim.empty:
            st.warning("จำลองไม่สำเร็จ (ข้อมูลไม่พอ)")
        else:
            # headline metrics
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Cash end (forecast)", money(sim["Cash_Forecast"].iloc[-1]))
            c2.metric("Avg Profit/day", money(sim["Profit_Forecast"].mean()))
            c3.metric("Max Backlog", num(sim["Backlog_Forecast"].max()))
            c4.metric("Avg Sold/day", num(sim["Sold_Forecast"].mean()))

            st.markdown("### Cash & Profit forecast")
            st.line_chart(sim.set_index("Day")[["Cash_Forecast", "Profit_Forecast"]], height=240)

            st.markdown("### Backlog & Inventory parts forecast")
            st.line_chart(sim.set_index("Day")[["Backlog_Forecast", "InvParts_End"]], height=240)

            st.markdown("### Price & Accepted/Sold forecast")
            st.line_chart(sim.set_index("Day")[["Price", "Accepted_Forecast", "Sold_Forecast"]], height=260)

            st.markdown("### Table (last 30 days)")
            st.dataframe(sim.tail(30), use_container_width=True)

            with st.expander("Download-ready CSV preview"):
                st.code(sim.to_csv(index=False)[:4000])

# Footer
st.caption("Autopilot Analyzer v1.0 — conservative, capacity-aware. Next upgrade: multi-line (Custom) + machine/hiring ROI learning from friend Day400 patterns.")
