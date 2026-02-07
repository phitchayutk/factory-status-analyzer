# ============================================================
# Factory Status Analyzer (Game Excel Export) — PRO UI + WHY + WHAT-IF + LOAN ✅
# Copy-paste ทั้งไฟล์: Ctrl+A → Ctrl+V ได้เลย
#
# ✅ Robust import (alias columns, BytesIO)
# ✅ Per-user session isolation (เพื่อนเข้าลิงก์เดียวกันไม่เห็นค่ากัน)
# ✅ Full-file analysis (timeseries) + Trend + “ช่วงพัง”
# ✅ Suggest Standard Product Price (fit Price↔Demand) + fallback
# ✅ Capacity-aware pricing warning (ติดคอขวด → regression ไม่น่าเชื่อ)
# ✅ Loan engine (กู้/ไม่กู้/กู้เท่าไร/ควรคืนเมื่อไร) แบบ heuristic
# ✅ Root-cause explainer (ทำไมดิ่ง) + Evidence
# ✅ What-if forecast (คาดการณ์ผลจากการปรับ Suggestion 7/14/30 วัน)
# ✅ Professional UI: sidebar control center + KPI cards + callouts
#
# Requirements:
#   streamlit
#   pandas
#   openpyxl
#
# Run:
#   streamlit run app.py
# ============================================================

import io
import math
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


# ============================================================
# UI THEME (CSS) — Professional look
# ============================================================
def apply_global_styles() -> None:
    st.markdown(
        """
        <style>
        /* ---- page ---- */
        .block-container {padding-top: 1.2rem; padding-bottom: 2.5rem;}
        /* ---- headings ---- */
        h1, h2, h3 {letter-spacing: -0.02em;}
        /* ---- cards ---- */
        .card {
            border: 1px solid rgba(49, 51, 63, 0.12);
            border-radius: 16px;
            padding: 14px 14px;
            background: rgba(255,255,255,0.96);
            box-shadow: 0 1px 10px rgba(0,0,0,0.05);
        }
        .card-title {font-size: 0.80rem; color: rgba(49,51,63,0.65); margin-bottom: 6px;}
        .card-value {font-size: 1.35rem; font-weight: 700; margin-bottom: 2px;}
        .card-sub {font-size: 0.78rem; color: rgba(49,51,63,0.65);}
        /* ---- callouts ---- */
        .callout {
            border-radius: 16px;
            padding: 12px 14px;
            border: 1px solid rgba(49,51,63,0.12);
            background: rgba(248, 250, 252, 0.9);
        }
        .callout.danger {border-color: rgba(220, 38, 38, 0.35); background: rgba(254, 242, 242, 0.85);}
        .callout.warn {border-color: rgba(245, 158, 11, 0.35); background: rgba(255, 251, 235, 0.85);}
        .callout.ok {border-color: rgba(16, 185, 129, 0.35); background: rgba(236, 253, 245, 0.85);}
        .callout.info {border-color: rgba(59, 130, 246, 0.35); background: rgba(239, 246, 255, 0.85);}
        .callout-title {font-weight: 800; margin-bottom: 4px;}
        .mono {font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;}
        /* ---- small helpers ---- */
        .muted {color: rgba(49,51,63,0.65);}
        </style>
        """,
        unsafe_allow_html=True,
    )


def kpi_card(title: str, value: str, sub: str = "") -> None:
    st.markdown(
        f"""
        <div class="card">
          <div class="card-title">{title}</div>
          <div class="card-value">{value}</div>
          <div class="card-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def callout(kind: str, title: str, body: str) -> None:
    kind = kind if kind in ("danger", "warn", "ok", "info") else "info"
    st.markdown(
        f"""
        <div class="callout {kind}">
          <div class="callout-title">{title}</div>
          <div class="muted">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# Helpers
# ============================================================
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b not in (0, 0.0) else default


def to_float(x, default: float = 0.0) -> float:
    try:
        if pd.isna(x):
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def money(x: float) -> str:
    return f"${x:,.2f}"


def num(x: float) -> str:
    return f"{x:,.2f}"


def ceil_int(x: float) -> int:
    return int(math.ceil(max(0.0, x)))


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


# ============================================================
# Inputs (editable assumptions + snapshot)
# ============================================================
@dataclass
class CheatDefaults:
    lead_time_days: float = 4.0
    cost_per_part: float = 45.0
    raw_order_fee: float = 1500.0
    holding_cost_per_part_day: float = 1.0

    # workforce
    salary_rookie_per_day: float = 80.0
    salary_expert_per_day: float = 150.0
    rookie_productivity_vs_expert: float = 0.40
    days_to_become_expert: float = 15.0

    # finance
    normal_debt_apr: float = 0.365
    loan_commission_rate: float = 0.02

    # parts usage
    std_parts_per_unit: float = 2.0
    cus_parts_per_unit: float = 1.0


@dataclass
class MachinePrices:
    station1_buy: float = 18000.0
    station1_sell: float = 8000.0
    station2_buy: float = 12000.0
    station2_sell: float = 6000.0
    station3_buy: float = 10000.0
    station3_sell: float = 5000.0


# ============================================================
# Column aliases (robust import)
# ============================================================
COL = {
    "DAY": ["Day", "day", "DAY"],

    # Inventory
    "INV_LEVEL": ["Inventory-Level", "Inventory Level", "Inventory_Level"],

    # Finance
    "CASH": ["Finance-Cash On Hand", "Cash On Hand", "Finance Cash On Hand", "Cash"],
    "DEBT": ["Finance-Debt", "Debt", "Finance Debt"],
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
    "STD_EWL": ["Standard Manual Processing-Effective Work Load (%)", "Effective Work Load (%)", "Effective Work Load"],
    "STD_MP_OUT": ["Standard Manual Processing-Output", "Manual Processing-Output", "Manual Output"],

    # Custom
    "CUS_DEMAND": ["Custom Orders-Demand", "Daily Demand", "Demand"],
    "CUS_ACCEPT": ["Custom Orders-Accepted Orders", "Custom Accepted Orders", "Accepted Orders"],
    "CUS_ACCUM": ["Custom Orders-Accumulated Orders", "Custom Accumulated Orders", "Accumulated Orders"],
    "CUS_DELIV": ["Custom Deliveries-Deliveries", "Deliveries", "Deliveries Out"],
    "CUS_LT": ["Custom Deliveries-Average Lead Time", "Average Lead Time", "Lead Time"],
    "CUS_PRICE": ["Custom Deliveries-Actual Price", "Actual Price"],

    "CUS_Q2_1": ["Custom Queue 2-Level First Pass", "Level First Pass", "Q2 First Pass"],
    "CUS_Q2_2": ["Custom Queue 2-Level Second Pass", "Level Second Pass", "Q2 Second Pass"],
}


# ============================================================
# Session isolation (per user)
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
        "cheats": CheatDefaults(),
        "machine_prices": MachinePrices(),
        "import_day": None,
        "fullfile_day_range": None,
    }

S = st.session_state.sessions[SID]


# ============================================================
# Import + normalization
# ============================================================
def norm_day(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None:
        return None
    dcol = pick_col(df, COL["DAY"])
    if not dcol:
        return None
    out = df.copy()
    out["Day"] = pd.to_numeric(out[dcol], errors="coerce").fillna(-1).astype(int)
    out = out[out["Day"] >= 0].sort_values("Day")
    return out


def make_timeseries_from_excel(xbytes: bytes) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    xl = excel_file_from_bytes(xbytes)
    std_df = read_sheet(xl, "Standard")
    cus_df = read_sheet(xl, "Custom")
    inv_df = read_sheet(xl, "Inventory")
    fin_df = read_sheet(xl, "Finance", "Financial")
    wf_df = read_sheet(xl, "WorkForce", "Workforce")
    return tuple(map(norm_day, [std_df, cus_df, inv_df, fin_df, wf_df]))


def pick_best_day(std_df, cus_df, fin_df) -> int:
    all_days = pd.concat(
        [safe_day_series(std_df, COL["DAY"]), safe_day_series(cus_df, COL["DAY"]), safe_day_series(fin_df, COL["DAY"])],
        ignore_index=True,
    )
    if all_days.empty:
        return 0
    max_day = int(all_days.max())

    # Score latest days by "activity"
    def score_day(d: int) -> float:
        s = 0.0
        for df, cols in [
            (cus_df, [COL["CUS_DEMAND"], COL["CUS_DELIV"]]),
            (std_df, [COL["STD_ACCEPT"], COL["STD_DELIV"]]),
        ]:
            if df is None:
                continue
            dcol = pick_col(df, COL["DAY"])
            if not dcol:
                continue
            row = df.loc[pd.to_numeric(df[dcol], errors="coerce").fillna(-1).astype(int) == d]
            if row.empty:
                continue
            r = row.iloc[0]
            for a in cols:
                c = pick_col(df, a)
                if c:
                    s += abs(to_float(r.get(c, 0.0), 0.0))
        if fin_df is not None:
            dcol = pick_col(fin_df, COL["DAY"])
            if dcol:
                row = fin_df.loc[pd.to_numeric(fin_df[dcol], errors="coerce").fillna(-1).astype(int) == d]
                if not row.empty:
                    r = row.iloc[0]
                    cash_c = pick_col(fin_df, COL["CASH"])
                    if cash_c:
                        s += abs(to_float(r.get(cash_c, 0.0), 0.0))
        return s

    for d in range(max_day, -1, -1):
        if score_day(d) > 0:
            return d
    return max_day


# ============================================================
# Finance daily delta (proxy) from *To Date columns
# ============================================================
def _td_series(fin_df: pd.DataFrame, aliases: List[str]) -> pd.Series:
    c = pick_col(fin_df, aliases)
    return as_numeric_series(fin_df, c)


def finance_daily_delta(fin_ts: pd.DataFrame) -> pd.DataFrame:
    df = fin_ts.sort_values("Day").copy()

    sales_std_td = _td_series(df, COL["FIN_SALES_STD_TD"])
    sales_cus_td = _td_series(df, COL["FIN_SALES_CUS_TD"])
    salaries_td = _td_series(df, COL["FIN_SALARIES_TD"])
    h_raw_td = _td_series(df, COL["FIN_HOLD_RAW_TD"])
    h_cus_td = _td_series(df, COL["FIN_HOLD_CUS_TD"])
    h_std_td = _td_series(df, COL["FIN_HOLD_STD_TD"])
    int_td = _td_series(df, COL["FIN_DEBT_INT_TD"])
    com_td = _td_series(df, COL["FIN_LOAN_COM_TD"])

    out = pd.DataFrame({"Day": df["Day"]})
    out["Sales_per_Day"] = (sales_std_td + sales_cus_td).diff().fillna(0.0)
    out["Costs_Proxy_per_Day"] = (salaries_td + h_raw_td + h_cus_td + h_std_td + int_td + com_td).diff().fillna(0.0)
    out["Profit_Proxy_per_Day"] = out["Sales_per_Day"] - out["Costs_Proxy_per_Day"]

    cash_c = pick_col(df, COL["CASH"])
    debt_c = pick_col(df, COL["DEBT"])
    if cash_c:
        out["Cash_On_Hand"] = as_numeric_series(df, cash_c)
    if debt_c:
        out["Debt"] = as_numeric_series(df, debt_c)

    # Also break out cost components (optional evidence)
    out["Salaries_per_Day"] = salaries_td.diff().fillna(0.0)
    out["Holding_per_Day"] = (h_raw_td + h_cus_td + h_std_td).diff().fillna(0.0)
    out["Interest_per_Day"] = int_td.diff().fillna(0.0)
    out["Commission_per_Day"] = com_td.diff().fillna(0.0)

    return out


# ============================================================
# Standard Pricing Model
# ============================================================
def build_standard_price_dataset(std_ts: pd.DataFrame) -> pd.DataFrame:
    price_c = pick_col(std_ts, COL["STD_PRICE"])
    mkt_c = pick_col(std_ts, COL["STD_MKT"])
    acc_c = pick_col(std_ts, COL["STD_ACCEPT"])
    del_c = pick_col(std_ts, COL["STD_DELIV"])
    accum_c = pick_col(std_ts, COL["STD_ACCUM"])

    df = pd.DataFrame({"Day": std_ts["Day"]})
    df["Price"] = as_numeric_series(std_ts, price_c)
    df["Market"] = as_numeric_series(std_ts, mkt_c)
    df["Accepted"] = as_numeric_series(std_ts, acc_c)
    df["Deliveries"] = as_numeric_series(std_ts, del_c)
    df["Accumulated"] = as_numeric_series(std_ts, accum_c)

    # Demand proxy: Accepted (daily)
    df["DemandProxy"] = df["Accepted"].clip(lower=0.0)

    # Backlog proxy: Accumulated - Deliveries
    df["BacklogProxy"] = (df["Accumulated"] - df["Deliveries"]).clip(lower=0.0)

    # Fill rate proxy
    df["FillRateProxy"] = df["Deliveries"] / (df["DemandProxy"].replace(0, pd.NA))
    df["FillRateProxy"] = df["FillRateProxy"].fillna(1.0).clip(lower=0.0, upper=2.0)

    df = df[(df["Price"] > 0) & (df["DemandProxy"] >= 0)]
    return df


def fit_linear_demand(price: pd.Series, demand: pd.Series) -> Optional[Tuple[float, float, float]]:
    x = price.astype(float).values
    y = demand.astype(float).values
    if len(x) < 8:
        return None
    if float(pd.Series(x).nunique()) < 3:
        return None
    x_mean = x.mean()
    y_mean = y.mean()
    var_x = ((x - x_mean) ** 2).sum()
    if var_x <= 1e-9:
        return None
    cov_xy = ((x - x_mean) * (y - y_mean)).sum()

    b = cov_xy / var_x
    a = y_mean - b * x_mean

    y_hat = a + b * x
    ss_res = ((y - y_hat) ** 2).sum()
    ss_tot = ((y - y_mean) ** 2).sum() + 1e-9
    r2 = 1.0 - ss_res / ss_tot
    return float(a), float(b), float(r2)


def suggest_standard_price(std_price_df: pd.DataFrame) -> Dict[str, float]:
    if std_price_df is None or std_price_df.empty:
        return {"suggested_price": 0.0, "method": 0.0, "r2": 0.0}

    last_market = float(std_price_df["Market"].replace(0, pd.NA).dropna().iloc[-1]) if (std_price_df["Market"] > 0).any() else 0.0
    last_price = float(std_price_df["Price"].iloc[-1])
    last_fill = float(std_price_df["FillRateProxy"].iloc[-1])
    last_backlog = float(std_price_df["BacklogProxy"].iloc[-1])

    fit = fit_linear_demand(std_price_df["Price"], std_price_df["DemandProxy"])
    if fit is not None:
        a, b, r2 = fit
        if b < 0 and r2 >= -0.5:
            p_star = -a / (2.0 * b)  # revenue-max price for linear demand
            if last_market > 0:
                lo, hi = 0.7 * last_market, 1.3 * last_market
            else:
                lo, hi = float(std_price_df["Price"].quantile(0.1)), float(std_price_df["Price"].quantile(0.9))
                if lo <= 0 or hi <= 0 or lo >= hi:
                    lo, hi = float(std_price_df["Price"].min()), float(std_price_df["Price"].max())
            p_suggest = float(clamp(p_star, lo, hi))
            return {
                "suggested_price": p_suggest,
                "method": 1.0,  # regression
                "r2": float(r2),
                "last_price": last_price,
                "last_market": last_market,
                "slope_b": float(b),
                "intercept_a": float(a),
            }

    # Fallback: market + service pressure
    base = last_market if last_market > 0 else last_price
    if base <= 0:
        base = 1.0

    adj = 0.0
    if last_backlog > 0 or last_fill < 0.95:
        adj = +0.08  # raise price to calm demand
    elif last_fill > 1.05 and last_backlog <= 0:
        adj = -0.05  # lower to stimulate
    lo, hi = 0.7 * base, 1.3 * base
    p_suggest = float(clamp(base * (1.0 + adj), lo, hi))

    return {
        "suggested_price": p_suggest,
        "method": 2.0,  # fallback
        "r2": 0.0,
        "last_price": last_price,
        "last_market": last_market,
        "last_fill_rate": last_fill,
        "last_backlog": last_backlog,
    }


# ============================================================
# Inventory / Capacity / Workforce core heuristics
# ============================================================
def compute_parts_per_day(std_demand_units: float, cus_demand_units: float, cheats: CheatDefaults) -> Dict[str, float]:
    std_parts = std_demand_units * cheats.std_parts_per_unit
    cus_parts = cus_demand_units * cheats.cus_parts_per_unit
    return {"std_parts_per_day": std_parts, "cus_parts_per_day": cus_parts, "parts_per_day": std_parts + cus_parts}


def recommend_reorder_policy(parts_per_day: float, cheats: CheatDefaults) -> Dict[str, float]:
    D = max(0.0, parts_per_day)
    rop = D * cheats.lead_time_days
    roq = math.sqrt((2.0 * D * cheats.raw_order_fee) / max(1e-9, cheats.holding_cost_per_part_day)) if D > 0 else 0.0
    return {"recommended_rop": rop, "recommended_roq": roq}


def inventory_coverage_days(inv_parts: float, parts_per_day: float) -> float:
    return safe_div(inv_parts, parts_per_day, default=0.0)


def detect_capacity_constrained(std_window: pd.DataFrame) -> Dict[str, float]:
    ewl_c = pick_col(std_window, COL["STD_EWL"])
    mp_c = pick_col(std_window, COL["STD_MP_OUT"])
    del_c = pick_col(std_window, COL["STD_DELIV"])
    acc_c = pick_col(std_window, COL["STD_ACCEPT"])

    ewl = as_numeric_series(std_window, ewl_c) if ewl_c else pd.Series([0.0] * len(std_window))
    mp_out = as_numeric_series(std_window, mp_c) if mp_c else pd.Series([0.0] * len(std_window))
    deliveries = as_numeric_series(std_window, del_c) if del_c else pd.Series([0.0] * len(std_window))
    demand = as_numeric_series(std_window, acc_c) if acc_c else pd.Series([0.0] * len(std_window))

    ewl_avg = float(ewl.mean()) if len(ewl) else 0.0
    delivery_gap_avg = float((demand - deliveries).mean()) if len(deliveries) else 0.0

    constrained = (ewl_avg >= 95.0) or (delivery_gap_avg > 0.01)
    return {
        "capacity_constrained": 1.0 if constrained else 0.0,
        "ewl_avg": ewl_avg,
        "manual_out_avg": float(mp_out.mean()) if len(mp_out) else 0.0,
        "std_gap_avg": delivery_gap_avg,
    }


# ============================================================
# Loan Engine (heuristic)
# ============================================================
def daily_interest_rate(apr: float) -> float:
    return max(0.0, apr) / 365.0


def loan_total_cost(loan_amount: float, apr: float, commission_rate: float, horizon_days: int) -> float:
    r = daily_interest_rate(apr)
    interest = loan_amount * r * max(0, horizon_days)
    commission = loan_amount * max(0.0, commission_rate)
    return interest + commission


def loan_should_borrow(
    need_cash: float,
    expected_delta_profit_per_day: float,
    apr: float,
    commission_rate: float,
    horizon_days: int,
    runway_days: float,
    safety_runway_days: float,
) -> Dict[str, float]:
    if need_cash <= 0:
        return {"borrow": 0.0, "borrow_amount": 0.0, "reason_code": 0.0}

    # Only consider borrowing if runway is tight OR ROI very strong
    roi_value = expected_delta_profit_per_day * max(0, horizon_days)
    cost = loan_total_cost(need_cash, apr, commission_rate, horizon_days)

    ok_roi = roi_value > cost * 1.15  # margin
    tight_runway = runway_days < safety_runway_days

    borrow = 1.0 if (ok_roi and (tight_runway or expected_delta_profit_per_day > 0)) else 0.0
    return {
        "borrow": borrow,
        "borrow_amount": float(need_cash if borrow else 0.0),
        "roi_value": float(roi_value),
        "loan_cost": float(cost),
        "tight_runway": 1.0 if tight_runway else 0.0,
        "reason_code": 1.0 if ok_roi else 2.0,  # 1 ROI ok, 2 ROI not ok
    }


def repayment_policy(cash_on_hand: float, cash_buffer: float, debt: float, profit_per_day: float) -> Dict[str, float]:
    # Heuristic: repay when cash is comfortably above buffer AND profit is positive
    if debt <= 0:
        return {"repay": 0.0, "repay_amount": 0.0}
    if cash_on_hand > cash_buffer * 1.5 and profit_per_day > 0:
        repay_amount = min(debt, cash_on_hand - cash_buffer)
        return {"repay": 1.0, "repay_amount": float(max(0.0, repay_amount))}
    return {"repay": 0.0, "repay_amount": 0.0}


# ============================================================
# Root Cause Explainer (Why it happened)
# ============================================================
def window_metrics(std_w: Optional[pd.DataFrame], cus_w: Optional[pd.DataFrame], inv_w: Optional[pd.DataFrame], fin_daily_w: Optional[pd.DataFrame]) -> Dict[str, float]:
    out: Dict[str, float] = {}

    # Finance
    if fin_daily_w is not None and not fin_daily_w.empty:
        out["profit_day_avg"] = float(fin_daily_w["Profit_Proxy_per_Day"].mean())
        out["sales_day_avg"] = float(fin_daily_w["Sales_per_Day"].mean())
        out["cost_day_avg"] = float(fin_daily_w["Costs_Proxy_per_Day"].mean())
        out["holding_day_avg"] = float(fin_daily_w.get("Holding_per_Day", pd.Series([0.0])).mean())
        out["interest_day_avg"] = float(fin_daily_w.get("Interest_per_Day", pd.Series([0.0])).mean())
        out["commission_day_avg"] = float(fin_daily_w.get("Commission_per_Day", pd.Series([0.0])).mean())
        if "Cash_On_Hand" in fin_daily_w.columns:
            out["cash_last"] = float(fin_daily_w["Cash_On_Hand"].iloc[-1])
        if "Debt" in fin_daily_w.columns:
            out["debt_last"] = float(fin_daily_w["Debt"].iloc[-1])

    # Standard
    if std_w is not None and not std_w.empty:
        acc_c = pick_col(std_w, COL["STD_ACCEPT"])
        del_c = pick_col(std_w, COL["STD_DELIV"])
        price_c = pick_col(std_w, COL["STD_PRICE"])
        mkt_c = pick_col(std_w, COL["STD_MKT"])
        mp_c = pick_col(std_w, COL["STD_MP_OUT"])
        ewl_c = pick_col(std_w, COL["STD_EWL"])

        demand = as_numeric_series(std_w, acc_c)
        deliveries = as_numeric_series(std_w, del_c)
        out["std_demand_avg"] = float(demand.mean())
        out["std_deliv_avg"] = float(deliveries.mean())
        out["std_gap_avg"] = float((demand - deliveries).mean())
        out["std_price_avg"] = float(as_numeric_series(std_w, price_c).replace(0, pd.NA).dropna().mean()) if price_c else 0.0
        out["std_market_avg"] = float(as_numeric_series(std_w, mkt_c).replace(0, pd.NA).dropna().mean()) if mkt_c else 0.0
        out["std_mp_out_avg"] = float(as_numeric_series(std_w, mp_c).mean()) if mp_c else 0.0
        out["std_ewl_avg"] = float(as_numeric_series(std_w, ewl_c).mean()) if ewl_c else 0.0

    # Custom
    if cus_w is not None and not cus_w.empty:
        dem_c = pick_col(cus_w, COL["CUS_DEMAND"])
        del_c = pick_col(cus_w, COL["CUS_DELIV"])
        lt_c = pick_col(cus_w, COL["CUS_LT"])
        q2_1 = pick_col(cus_w, COL["CUS_Q2_1"])
        q2_2 = pick_col(cus_w, COL["CUS_Q2_2"])

        dem = as_numeric_series(cus_w, dem_c)
        deliv = as_numeric_series(cus_w, del_c)
        out["cus_demand_avg"] = float(dem.mean())
        out["cus_deliv_avg"] = float(deliv.mean())
        out["cus_gap_avg"] = float((dem - deliv).mean())
        out["cus_lt_avg"] = float(as_numeric_series(cus_w, lt_c).mean()) if lt_c else 0.0
        out["cus_q2_first_avg"] = float(as_numeric_series(cus_w, q2_1).mean()) if q2_1 else 0.0
        out["cus_q2_second_avg"] = float(as_numeric_series(cus_w, q2_2).mean()) if q2_2 else 0.0

    # Inventory
    if inv_w is not None and not inv_w.empty:
        inv_c = pick_col(inv_w, COL["INV_LEVEL"])
        inv_parts = as_numeric_series(inv_w, inv_c) if inv_c else pd.Series([0.0] * len(inv_w))
        out["inv_parts_avg"] = float(inv_parts.mean())
        out["inv_parts_last"] = float(inv_parts.iloc[-1]) if len(inv_parts) else 0.0

    return out


def diff_metrics(mA: Dict[str, float], mB: Dict[str, float]) -> Dict[str, float]:
    keys = sorted(set(mA.keys()) | set(mB.keys()))
    d: Dict[str, float] = {}
    for k in keys:
        d[k] = float(mB.get(k, 0.0) - mA.get(k, 0.0))
    return d


def explain_drop(mA: Dict[str, float], mB: Dict[str, float], d: Dict[str, float]) -> Tuple[str, List[Dict[str, str]]]:
    """
    Return: summary string + evidence list (Top causes)
    """
    evidence: List[Dict[str, str]] = []

    # Candidate causes
    # 1) Throughput/capacity
    if d.get("cus_gap_avg", 0.0) > 0.5 or d.get("std_gap_avg", 0.0) > 0.5:
        evidence.append({
            "cause": "ผลิต/ส่งมอบไม่ทัน (Throughput constraint)",
            "why": f"Gap เพิ่มขึ้น: Std Δ{num(d.get('std_gap_avg',0.0))}/day | Cus Δ{num(d.get('cus_gap_avg',0.0))}/day",
            "impact": "ยอดขาย/day ไม่โต (เพราะส่งมอบไม่เพิ่ม) + backlog/WIP โต → lead time สูง",
        })

    # 2) Holding costs
    if d.get("holding_day_avg", 0.0) > 0.5:
        evidence.append({
            "cause": "Holding cost กินกำไร (WIP/Stock ค้าง)",
            "why": f"Holding/day เพิ่ม Δ{money(d.get('holding_day_avg',0.0))}",
            "impact": "Profit/day ดิ่ง แม้ Sales/day ไม่ได้ลดมาก",
        })

    # 3) Financial drag
    if d.get("interest_day_avg", 0.0) > 0.1 or d.get("commission_day_avg", 0.0) > 0.1:
        evidence.append({
            "cause": "ต้นทุนดอกเบี้ย/ค่าคอมกู้เพิ่ม",
            "why": f"Interest/day Δ{money(d.get('interest_day_avg',0.0))} | Commission/day Δ{money(d.get('commission_day_avg',0.0))}",
            "impact": "กำไรและ cash ถูกดูดออกทุกวัน → ซื้อเครื่อง/แก้คอขวดไม่ได้",
        })

    # 4) Price changes without capacity
    if abs(d.get("std_price_avg", 0.0)) > 0.01 and (mB.get("std_gap_avg", 0.0) > 0.5):
        evidence.append({
            "cause": "ปรับราคา แต่ติดคอขวด → ยอดขายไม่ขึ้น",
            "why": f"Std price Δ{money(d.get('std_price_avg',0.0))} แต่ Std gap ยัง {num(mB.get('std_gap_avg',0.0))}/day",
            "impact": "Demand signal เพี้ยน (ผลิตไม่ทัน) ทำให้ optimization ราคาไม่น่าเชื่อ",
        })

    # 5) Lead time pressure
    if d.get("cus_lt_avg", 0.0) > 0.2:
        evidence.append({
            "cause": "Custom lead time สูงขึ้น (WIP/คิวพอง)",
            "why": f"Avg LT เพิ่ม Δ{num(d.get('cus_lt_avg',0.0))} days | Q2(second) Δ{num(d.get('cus_q2_second_avg',0.0))}",
            "impact": "Penalty/holding เพิ่ม + backlog ระบายช้า → cashflow แย่",
        })

    # Summary
    profA = mA.get("profit_day_avg", 0.0)
    profB = mB.get("profit_day_avg", 0.0)
    salesA = mA.get("sales_day_avg", 0.0)
    salesB = mB.get("sales_day_avg", 0.0)

    summary = (
        f"Profit/day เฉลี่ย {money(profA)} → {money(profB)} (Δ{money(profB-profA)}), "
        f"Sales/day เฉลี่ย {money(salesA)} → {money(salesB)} (Δ{money(salesB-salesA)}). "
        f"สาเหตุเด่นมักมาจาก ‘ส่งมอบไม่ทัน + holding/finance cost เพิ่ม’"
    )

    # Keep top 3 causes (by heuristic priority)
    return summary, evidence[:3]


# ============================================================
# What-if Forecast (simple but useful)
# ============================================================
def whatif_forecast(
    base_metrics: Dict[str, float],
    actions: Dict[str, float],
    cheats: CheatDefaults,
    horizon_days: int,
) -> Dict[str, float]:
    """
    Heuristic forecast:
    - Hiring: adds capacity gradually (rookie productivity now, full after days_to_become_expert)
    - Inventory ROP/ROQ: reduces stockout risk => improves delivery stability (modeled as fill boost)
    - Price: affects demand proxy if regression is reliable (we keep conservative)
    - CapEx (machines): not simulated deeply; we treat as cash hit + small capacity gain if user sets it
    """
    out: Dict[str, float] = {"horizon_days": float(horizon_days)}

    # Base
    profit_day = float(base_metrics.get("profit_day_avg", 0.0))
    cash_last = float(base_metrics.get("cash_last", 0.0))
    debt_last = float(base_metrics.get("debt_last", 0.0))

    std_gap = float(base_metrics.get("std_gap_avg", 0.0))
    cus_gap = float(base_metrics.get("cus_gap_avg", 0.0))

    # Action inputs
    hire_rookies = int(actions.get("hire_rookies", 0))
    capex = float(actions.get("capex", 0.0))
    borrow = float(actions.get("borrow_amount", 0.0))
    repay = float(actions.get("repay_amount", 0.0))

    # Hiring effect on throughput (conservative):
    # Early horizon: avg productivity between rookie and expert depending on horizon vs days_to_become_expert
    t = max(0, horizon_days)
    if hire_rookies > 0:
        if t <= cheats.days_to_become_expert:
            eff = cheats.rookie_productivity_vs_expert
        else:
            # part of horizon as rookie, part as expert
            frac_rook = cheats.days_to_become_expert / t
            eff = frac_rook * cheats.rookie_productivity_vs_expert + (1 - frac_rook) * 1.0
        # Convert to "gap reduction" proxy: each expert-equivalent reduces gap by 1 unit of gap scale
        # (We do not know exact mapping -> keep modest)
        gap_reduction = min(cus_gap, hire_rookies * eff * 0.8)
    else:
        gap_reduction = 0.0

    cus_gap_new = max(0.0, cus_gap - gap_reduction)
    std_gap_new = max(0.0, std_gap - (hire_rookies * 0.15))  # small spillover (conservative)

    # Profit changes:
    # - Salary increases immediately
    salary_cost = hire_rookies * cheats.salary_rookie_per_day
    # - Throughput improvement increases profit (proxy) with diminishing returns
    profit_boost = (gap_reduction * 6.0)  # each unit of gap reduction worth ~$6/day proxy (tunable)

    # Finance cost for new debt
    apr = cheats.normal_debt_apr
    comm = cheats.loan_commission_rate
    debt_cost_day = borrow * daily_interest_rate(apr)
    commission_cost = borrow * comm

    profit_day_new = profit_day + profit_boost - salary_cost - debt_cost_day

    # Cash trajectory:
    cash_change = profit_day_new * t
    cash_new = cash_last + cash_change - capex - commission_cost - repay + borrow
    debt_new = max(0.0, debt_last + borrow - repay)

    out.update({
        "profit_day_base": profit_day,
        "profit_day_pred": profit_day_new,
        "cash_base": cash_last,
        "cash_pred": cash_new,
        "debt_base": debt_last,
        "debt_pred": debt_new,
        "cus_gap_base": cus_gap,
        "cus_gap_pred": cus_gap_new,
        "std_gap_base": std_gap,
        "std_gap_pred": std_gap_new,
        "assumed_profit_boost": profit_boost,
        "assumed_salary_cost_day": salary_cost,
        "assumed_debt_cost_day": debt_cost_day,
        "assumed_commission_cost": commission_cost,
    })
    return out


# ============================================================
# Streamlit App
# ============================================================
st.set_page_config(page_title="Factory Status Analyzer", layout="wide")
apply_global_styles()

st.title("🏭 Factory Status Analyzer — Pro Dashboard")
st.caption(f"Session: {SID[:8]} (แยกค่าต่อคน)")

# ------------------------------------------------------------
# Sidebar: Control Center
# ------------------------------------------------------------
with st.sidebar:
    st.subheader("Control Center")

    if st.button("🔄 Reset (เฉพาะฉัน)"):
        st.session_state.pop("sid", None)
        st.rerun()

    uploaded = st.file_uploader("Upload เกม Export (.xlsx)", type=["xlsx"])

    st.divider()
    st.markdown("**Strategy Mode**")
    mode = st.selectbox("Mode", ["Auto", "Crisis (เงินตึง)", "Stabilize (แก้คอขวด)", "Growth (ทำกำไร)"], index=0)

    st.markdown("**Objective Weights**")
    w_profit = st.slider("Weight: Profit/day", 0.0, 1.0, 0.55, 0.05)
    w_endcash = 1.0 - w_profit
    st.caption(f"Weight: Cash ปลายเกม = {w_endcash:.2f}")

    st.divider()
    st.markdown("**Cheat/Assumptions**")
    cheats: CheatDefaults = S["cheats"]
    cheats.lead_time_days = st.number_input("Lead time (days)", value=float(cheats.lead_time_days), step=1.0)
    cheats.raw_order_fee = st.number_input("Raw order fee", value=float(cheats.raw_order_fee), step=100.0)
    cheats.holding_cost_per_part_day = st.number_input("Holding cost / part / day", value=float(cheats.holding_cost_per_part_day), step=0.1)
    cheats.cost_per_part = st.number_input("Raw cost/part", value=float(cheats.cost_per_part), step=1.0)

    st.caption("Workforce")
    cheats.salary_rookie_per_day = st.number_input("Rookie salary/day", value=float(cheats.salary_rookie_per_day), step=10.0)
    cheats.salary_expert_per_day = st.number_input("Expert salary/day", value=float(cheats.salary_expert_per_day), step=10.0)
    cheats.rookie_productivity_vs_expert = st.number_input("Rookie productivity (vs expert)", value=float(cheats.rookie_productivity_vs_expert), step=0.05)
    cheats.days_to_become_expert = st.number_input("Days → Expert", value=float(cheats.days_to_become_expert), step=1.0)

    st.caption("Finance")
    cheats.normal_debt_apr = st.number_input("Normal Debt APR", value=float(cheats.normal_debt_apr), step=0.01)
    cheats.loan_commission_rate = st.number_input("Loan commission rate", value=float(cheats.loan_commission_rate), step=0.01)

    st.divider()
    st.markdown("**Machine Prices (scenario-dependent)**")
    mp: MachinePrices = S["machine_prices"]
    mp.station1_buy = st.number_input("S1 buy", value=float(mp.station1_buy), step=500.0)
    mp.station2_buy = st.number_input("S2 buy", value=float(mp.station2_buy), step=500.0)
    mp.station3_buy = st.number_input("S3 buy", value=float(mp.station3_buy), step=500.0)

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if uploaded is not None:
    try:
        xbytes = uploaded.getvalue()
        S["last_uploaded_bytes"] = xbytes

        # reset slider key when new file uploaded to prevent state conflict
        st.session_state.pop("fullfile_day_range", None)

        std_ts, cus_ts, inv_ts, fin_ts, wf_ts = make_timeseries_from_excel(xbytes)

        if fin_ts is None and std_ts is None and cus_ts is None:
            callout("danger", "Import failed", "ไม่เจอชีทหลัก (Standard/Custom/Finance) ในไฟล์นี้")
            st.stop()

        suggested_day = pick_best_day(std_ts, cus_ts, fin_ts)
        S["import_day"] = suggested_day

    except ImportError:
        callout("danger", "Missing dependency", "อ่าน .xlsx ไม่ได้: ต้องมี openpyxl (เพิ่มใน requirements.txt: openpyxl)")
        st.stop()
    except Exception as e:
        callout("danger", "Import error", f"{e}")
        st.stop()
else:
    callout("info", "Upload ไฟล์ก่อน", "อัปโหลด Excel export จากเกมเพื่อเริ่มวิเคราะห์")
    st.stop()

# ------------------------------------------------------------
# Tabs
# ------------------------------------------------------------
tabs = st.tabs(["Overview", "Trends", "Pricing", "Why (Root cause)", "What-if", "Checklist"])

# ------------------------------------------------------------
# Common: day range selection + window slicing
# ------------------------------------------------------------
def window(df: Optional[pd.DataFrame], d0: int, d1: int) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    return df[(df["Day"] >= d0) & (df["Day"] <= d1)].copy()


min_day = 0
max_day = 0
for df in [std_ts, cus_ts, inv_ts, fin_ts]:
    if df is not None and not df.empty:
        min_day = min(min_day, int(df["Day"].min()))
        max_day = max(max_day, int(df["Day"].max()))

if max_day < 0:
    max_day = 0

with st.sidebar:
    st.divider()
    st.markdown("**Full-file Range**")
    if max_day == min_day:
        d0, d1 = max_day, max_day
        st.caption(f"Range: Day {d0}")
    else:
        d0, d1 = st.slider(
            "Select day range",
            min_value=int(min_day),
            max_value=int(max_day),
            value=(int(min_day), int(max_day)),
            step=1,
            key="fullfile_day_range",
        )

std_w = window(std_ts, d0, d1)
cus_w = window(cus_ts, d0, d1)
inv_w = window(inv_ts, d0, d1)
fin_w = window(fin_ts, d0, d1)

fin_daily = finance_daily_delta(fin_ts) if fin_ts is not None and not fin_ts.empty else None
fin_daily_w = window(fin_daily, d0, d1) if fin_daily is not None else None


# ------------------------------------------------------------
# OVERVIEW
# ------------------------------------------------------------
with tabs[0]:
    st.subheader("Overview")

    # Compute window metrics
    base = window_metrics(std_w, cus_w, inv_w, fin_daily_w)

    # Inventory parts/day approximation from std/cus demand avg
    std_demand = float(base.get("std_demand_avg", 0.0))
    cus_demand = float(base.get("cus_demand_avg", 0.0))
    parts = compute_parts_per_day(std_demand, cus_demand, cheats)

    inv_parts_last = float(base.get("inv_parts_last", base.get("inv_parts_avg", 0.0)))
    cov = inventory_coverage_days(inv_parts_last, parts["parts_per_day"])
    inv_rec = recommend_reorder_policy(parts["parts_per_day"], cheats)

    # Pricing suggestion (Standard)
    price_sugg = {"suggested_price": 0.0, "method": 0.0, "r2": 0.0}
    capacity_warn = {"capacity_constrained": 0.0, "ewl_avg": 0.0, "std_gap_avg": 0.0, "manual_out_avg": 0.0}

    if std_w is not None and not std_w.empty:
        std_price_df = build_standard_price_dataset(std_w)
        if not std_price_df.empty:
            price_sugg = suggest_standard_price(std_price_df)
        capacity_warn = detect_capacity_constrained(std_w)

    # Status heuristic
    profit_day = float(base.get("profit_day_avg", 0.0))
    runway = safe_div(float(base.get("cash_last", 0.0)), max(1e-9, float(base.get("cost_day_avg", 0.0))), default=0.0)

    severity = 0
    reasons = []
    if cov < cheats.lead_time_days and parts["parts_per_day"] > 0:
        severity += 2
        reasons.append("Inventory coverage < lead time (เสี่ยง stockout)")
    if float(base.get("cus_gap_avg", 0.0)) > 1.0:
        severity += 2
        reasons.append("Custom ส่งมอบไม่ทัน (gap สูง)")
    if float(base.get("std_gap_avg", 0.0)) > 1.0:
        severity += 1
        reasons.append("Standard ส่งมอบไม่ทัน")
    if runway > 0 and runway < 10:
        severity += 2
        reasons.append("Cash runway ต่ำ (<10 วัน)")
    if profit_day < 0:
        severity += 2
        reasons.append("Profit/day ติดลบ")

    if severity >= 6:
        status = "CRITICAL"
        emoji = "🔴"
        kind = "danger"
    elif severity >= 3:
        status = "WARNING"
        emoji = "🟠"
        kind = "warn"
    else:
        status = "OK"
        emoji = "🟢"
        kind = "ok"

    callout(kind, f"{emoji} STATUS: {status}", " | ".join(reasons) if reasons else "โรงงานค่อนข้างนิ่งในช่วงที่เลือก")

    # KPI row
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        kpi_card("Profit/day (proxy)", money(profit_day), f"Range: Day {d0}–{d1}")
    with c2:
        kpi_card("Cash (last)", money(float(base.get("cash_last", 0.0))), f"Runway ~{num(runway)} days")
    with c3:
        kpi_card("Debt (last)", money(float(base.get("debt_last", 0.0))), " ")
    with c4:
        kpi_card("Inventory coverage", f"{num(cov)} d", f"Lead time {num(cheats.lead_time_days)} d")
    with c5:
        kpi_card("Std gap avg", num(float(base.get("std_gap_avg", 0.0))), "Demand - Deliveries")
    with c6:
        kpi_card("Cus gap avg", num(float(base.get("cus_gap_avg", 0.0))), "Demand - Deliveries")

    st.divider()

    # Key recommendations preview
    st.markdown("### Recommendations (Preview)")
    recs = []

    if parts["parts_per_day"] > 0 and cov < cheats.lead_time_days:
        recs.append({
            "Area": "Inventory",
            "Action": "ตั้ง ROP/ROQ ให้ครอบคลุม lead time",
            "Why": f"Coverage {num(cov)}d < LT {num(cheats.lead_time_days)}d",
            "Recommended": f"ROP≈{num(inv_rec['recommended_rop'])}, ROQ≈{num(inv_rec['recommended_roq'])}",
        })

    if capacity_warn.get("capacity_constrained", 0.0) >= 1.0:
        recs.append({
            "Area": "Standard",
            "Action": "อย่า optimize ราคาแบบจริงจังจนกว่าจะคลายคอขวด",
            "Why": f"EWL(avg)={num(capacity_warn.get('ewl_avg',0.0))}% หรือ Std gap(avg)={num(capacity_warn.get('std_gap_avg',0.0))}",
            "Recommended": "แก้ throughput ก่อน แล้วค่อย re-fit ราคา",
        })

    method_name = "Regression" if price_sugg.get("method", 0.0) == 1.0 else "Fallback"
    if price_sugg.get("suggested_price", 0.0) > 0:
        recs.append({
            "Area": "Pricing (Standard)",
            "Action": "ปรับ Std Product Price",
            "Why": f"Method={method_name}, R²={num(price_sugg.get('r2',0.0))}",
            "Recommended": f"Std Price ≈ {money(price_sugg.get('suggested_price',0.0))}",
        })

    if float(base.get("cus_gap_avg", 0.0)) > 0:
        # simple hiring suggestion: close part of the gap
        gap = float(base.get("cus_gap_avg", 0.0))
        hire = ceil_int(safe_div(gap, max(1e-9, cheats.rookie_productivity_vs_expert * 0.8), default=0.0))
        hire = int(clamp(hire, 0, 30))
        recs.append({
            "Area": "Workforce",
            "Action": "Hire rookies เพื่อปิด gap (แบบ conservative)",
            "Why": f"Custom gap(avg)={num(gap)}/day",
            "Recommended": f"Hire Rookies ≈ {hire}",
        })

    if recs:
        st.dataframe(pd.DataFrame(recs), use_container_width=True)
    else:
        callout("info", "No strong actions", "ช่วงนี้ข้อมูลไม่เห็นสัญญาณเด่น หรือ demand/gap ใกล้ศูนย์")

# ------------------------------------------------------------
# TRENDS
# ------------------------------------------------------------
with tabs[1]:
    st.subheader("Trends")

    if fin_daily is not None and not fin_daily.empty:
        st.markdown("#### 💵 Cash & Debt")
        cols = [c for c in ["Cash_On_Hand", "Debt"] if c in fin_daily.columns]
        if cols:
            st.line_chart(fin_daily.set_index("Day")[cols], height=240)

        st.markdown("#### 📊 Sales / Cost / Profit (proxy) per day")
        cols = [c for c in ["Sales_per_Day", "Costs_Proxy_per_Day", "Profit_Proxy_per_Day"] if c in fin_daily.columns]
        if cols:
            st.line_chart(fin_daily.set_index("Day")[cols], height=240)

        with st.expander("Show finance daily table"):
            st.dataframe(fin_daily, use_container_width=True)

    if std_ts is not None and not std_ts.empty:
        st.markdown("#### 🧱 Standard — Accepted vs Deliveries")
        acc_c = pick_col(std_ts, COL["STD_ACCEPT"])
        del_c = pick_col(std_ts, COL["STD_DELIV"])
        if acc_c and del_c:
            st.line_chart(std_ts.set_index("Day")[[acc_c, del_c]], height=240)

        st.markdown("#### 🧱 Standard — Price vs Market")
        pp_c = pick_col(std_ts, COL["STD_PRICE"])
        mp_c = pick_col(std_ts, COL["STD_MKT"])
        if pp_c and mp_c:
            st.line_chart(std_ts.set_index("Day")[[pp_c, mp_c]], height=220)

        st.markdown("#### 🧱 Standard — Manual Processing (Output & EWL)")
        ewl_c = pick_col(std_ts, COL["STD_EWL"])
        mpout_c = pick_col(std_ts, COL["STD_MP_OUT"])
        cols = [c for c in [mpout_c, ewl_c] if c]
        if cols:
            st.line_chart(std_ts.set_index("Day")[cols], height=220)

    if cus_ts is not None and not cus_ts.empty:
        st.markdown("#### 🧩 Custom — Demand vs Deliveries")
        dem_c = pick_col(cus_ts, COL["CUS_DEMAND"])
        del_c = pick_col(cus_ts, COL["CUS_DELIV"])
        cols = [c for c in [dem_c, del_c] if c]
        if cols:
            st.line_chart(cus_ts.set_index("Day")[cols], height=240)

        st.markdown("#### 🧩 Custom — Lead Time")
        lt_c = pick_col(cus_ts, COL["CUS_LT"])
        if lt_c:
            st.line_chart(cus_ts.set_index("Day")[[lt_c]], height=220)

        st.markdown("#### 🧩 Custom — Q2 First vs Second Pass")
        q2_1 = pick_col(cus_ts, COL["CUS_Q2_1"])
        q2_2 = pick_col(cus_ts, COL["CUS_Q2_2"])
        cols = [c for c in [q2_1, q2_2] if c]
        if cols:
            st.line_chart(cus_ts.set_index("Day")[cols], height=220)

# ------------------------------------------------------------
# PRICING
# ------------------------------------------------------------
with tabs[2]:
    st.subheader("Pricing (Standard)")

    if std_w is None or std_w.empty:
        callout("warn", "No Standard data in selected range", "เลือกช่วงวันที่มีข้อมูล Standard")
    else:
        dfP = build_standard_price_dataset(std_w)
        if dfP.empty:
            callout("warn", "Pricing dataset empty", "Price หรือ DemandProxy เป็น 0 ทั้งช่วง → fit ไม่ได้")
        else:
            sugg = suggest_standard_price(dfP)
            method_name = "Regression (Price↔Demand)" if sugg.get("method", 0) == 1.0 else "Fallback (Market + Backlog/Fill)"
            callout(
                "info",
                "Suggested Standard Product Price",
                f"{money(sugg.get('suggested_price',0.0))} | Method: {method_name} | R²: {num(sugg.get('r2',0.0))}",
            )

            cap = detect_capacity_constrained(std_w)
            if cap.get("capacity_constrained", 0.0) >= 1.0:
                callout(
                    "warn",
                    "Capacity-aware warning",
                    f"ช่วงนี้ Standard ติดคอขวด: EWL(avg)={num(cap.get('ewl_avg',0.0))}% หรือ Gap(avg)={num(cap.get('std_gap_avg',0.0))}/day → Demand ที่เห็นอาจถูกบิดเบือน",
                )

            st.markdown("### Price vs Demand (range)")
            st.caption("หมายเหตุ: ถ้าติดคอขวดหนัก การเปลี่ยนราคาอาจไม่ดันยอดขาย เพราะผลิตไม่ทัน")
            st.scatter_chart(dfP.set_index("Day")[["Price", "DemandProxy"]])

            st.markdown("### Backlog & Fill Rate (proxy)")
            st.line_chart(dfP.set_index("Day")[["BacklogProxy", "FillRateProxy"]], height=240)

            st.markdown("### Worst days (BadScore)")
            bad = dfP.copy()
            bad["BadScore"] = bad["BacklogProxy"] + (1.0 - bad["FillRateProxy"]).clip(lower=0.0) * bad["DemandProxy"]
            st.dataframe(
                bad.sort_values("BadScore", ascending=False)[
                    ["Day", "Price", "Market", "DemandProxy", "Deliveries", "BacklogProxy", "FillRateProxy", "BadScore"]
                ].head(12),
                use_container_width=True,
            )

# ------------------------------------------------------------
# WHY (Root cause)
# ------------------------------------------------------------
with tabs[3]:
    st.subheader("Why did it happen? (Root-cause)")

    if fin_daily is None or fin_daily.empty:
        callout("warn", "No Finance daily proxy", "ต้องมีชีท Finance (Sales/Costs *To Date) เพื่ออธิบายกำไรดิ่งได้ดี")
    else:
        # choose two windows to compare
        st.caption("เลือกช่วง A และช่วง B เพื่อดูว่า “ทำไมมันดิ่ง” (เปรียบเทียบค่าเฉลี่ยต่อวัน)")
        cA, cB = st.columns(2)
        with cA:
            a0, a1 = st.slider("Window A (baseline)", min_value=int(min_day), max_value=int(max_day), value=(max(int(min_day), d0), max(int(min_day), min(d1, int(max_day // 2)))), step=1, key="winA")
        with cB:
            b0, b1 = st.slider("Window B (compare)", min_value=int(min_day), max_value=int(max_day), value=(max(int(min_day), int(max_day // 2)), int(max_day)), step=1, key="winB")

        std_A = window(std_ts, a0, a1)
        cus_A = window(cus_ts, a0, a1)
        inv_A = window(inv_ts, a0, a1)
        finA = window(fin_daily, a0, a1)

        std_B = window(std_ts, b0, b1)
        cus_B = window(cus_ts, b0, b1)
        inv_B = window(inv_ts, b0, b1)
        finB = window(fin_daily, b0, b1)

        mA = window_metrics(std_A, cus_A, inv_A, finA)
        mB = window_metrics(std_B, cus_B, inv_B, finB)
        dM = diff_metrics(mA, mB)

        summary, causes = explain_drop(mA, mB, dM)

        callout("info", "Executive summary", summary)

        st.markdown("### Top causes (with evidence)")
        if causes:
            st.dataframe(pd.DataFrame(causes), use_container_width=True)
        else:
            callout("ok", "No strong causes detected", "ความเปลี่ยนแปลงไม่ชัด หรือข้อมูลบางคอลัมน์ไม่มีในไฟล์")

        with st.expander("Show metrics A vs B (debug/evidence)"):
            st.write("Metrics A", mA)
            st.write("Metrics B", mB)
            st.write("Delta (B-A)", dM)

# ------------------------------------------------------------
# WHAT-IF
# ------------------------------------------------------------
with tabs[4]:
    st.subheader("What-if (คาดการณ์ผลจากการปรับ Suggestion)")

    base = window_metrics(std_w, cus_w, inv_w, fin_daily_w)
    profit_day = float(base.get("profit_day_avg", 0.0))
    cash_last = float(base.get("cash_last", 0.0))
    debt_last = float(base.get("debt_last", 0.0))
    cost_day = float(base.get("cost_day_avg", 0.0))
    runway = safe_div(cash_last, max(1e-9, cost_day), default=0.0)

    st.markdown("### Choose actions")
    c1, c2, c3 = st.columns(3)
    with c1:
        hire_rookies = st.number_input("Hire rookies", min_value=0, max_value=50, value=0, step=1)
        capex = st.number_input("CapEx (machines/etc)", min_value=0.0, value=0.0, step=500.0)
    with c2:
        borrow_amount = st.number_input("Borrow amount", min_value=0.0, value=0.0, step=1000.0)
        repay_amount = st.number_input("Repay amount", min_value=0.0, value=0.0, step=1000.0)
    with c3:
        horizon = st.selectbox("Forecast horizon", [7, 14, 30], index=1)

    # Loan suggestion helper (optional)
    st.markdown("### Loan helper (optional suggestion)")
    expected_delta_profit_per_day = st.number_input(
        "Expected ΔProfit/day from actions (rough guess)",
        value=0.0,
        step=10.0,
        help="ถ้าคุณไม่แน่ใจ ใส่ 0 แล้วใช้ loan helper แค่เป็น warning",
    )
    loan_advice = loan_should_borrow(
        need_cash=max(0.0, capex - cash_last),
        expected_delta_profit_per_day=expected_delta_profit_per_day,
        apr=cheats.normal_debt_apr,
        commission_rate=cheats.loan_commission_rate,
        horizon_days=int(horizon),
        runway_days=float(runway),
        safety_runway_days=10.0,
    )
    if loan_advice.get("borrow", 0.0) >= 1.0:
        callout(
            "info",
            "Borrowing is justifiable (heuristic)",
            f"Need cash≈{money(max(0.0, capex - cash_last))} | ROI≈{money(loan_advice.get('roi_value',0.0))} vs Cost≈{money(loan_advice.get('loan_cost',0.0))}",
        )
    else:
        callout(
            "warn",
            "Borrowing not recommended (heuristic)",
            f"ROI≈{money(loan_advice.get('roi_value',0.0))} vs Cost≈{money(loan_advice.get('loan_cost',0.0))} | Runway tight={int(loan_advice.get('tight_runway',0.0))}",
        )

    # Repayment suggestion helper
    repay_advice = repayment_policy(cash_last, cash_buffer=max(0.0, cost_day * 7), debt=debt_last, profit_per_day=profit_day)
    if repay_advice.get("repay", 0.0) >= 1.0:
        callout("info", "Repayment suggestion", f"ควรคืนหนี้ได้ประมาณ {money(repay_advice.get('repay_amount',0.0))} โดยกัน buffer ไว้ ~7 วัน")
    else:
        callout("info", "Repayment suggestion", "ยังไม่ควรเร่งคืนหนี้ (กัน cash buffer/หรือกำไรยังไม่เป็นบวก)")

    # Forecast
    actions = {
        "hire_rookies": float(hire_rookies),
        "capex": float(capex),
        "borrow_amount": float(borrow_amount),
        "repay_amount": float(repay_amount),
    }
    pred = whatif_forecast(base, actions, cheats, horizon_days=int(horizon))

    st.markdown("### Forecast result")
    cA, cB, cC, cD = st.columns(4)
    with cA:
        kpi_card("Profit/day (pred)", money(pred["profit_day_pred"]), f"base {money(pred['profit_day_base'])}")
    with cB:
        kpi_card("Cash (pred)", money(pred["cash_pred"]), f"base {money(pred['cash_base'])}")
    with cC:
        kpi_card("Debt (pred)", money(pred["debt_pred"]), f"base {money(pred['debt_base'])}")
    with cD:
        kpi_card("Cus gap (pred)", num(pred["cus_gap_pred"]), f"base {num(pred['cus_gap_base'])}")

    with st.expander("Show forecast details (assumptions)"):
        st.json(pred)

# ------------------------------------------------------------
# CHECKLIST (copy to game)
# ------------------------------------------------------------
with tabs[5]:
    st.subheader("Checklist (copy into game)")

    base = window_metrics(std_w, cus_w, inv_w, fin_daily_w)
    std_demand = float(base.get("std_demand_avg", 0.0))
    cus_demand = float(base.get("cus_demand_avg", 0.0))
    parts = compute_parts_per_day(std_demand, cus_demand, cheats)

    inv_parts_last = float(base.get("inv_parts_last", base.get("inv_parts_avg", 0.0)))
    cov = inventory_coverage_days(inv_parts_last, parts["parts_per_day"])
    inv_rec = recommend_reorder_policy(parts["parts_per_day"], cheats)

    # Pricing suggestion
    price_sugg = {"suggested_price": 0.0, "method": 0.0, "r2": 0.0}
    if std_w is not None and not std_w.empty:
        dfP = build_standard_price_dataset(std_w)
        if not dfP.empty:
            price_sugg = suggest_standard_price(dfP)

    # Hiring suggestion
    cus_gap = float(base.get("cus_gap_avg", 0.0))
    hire = ceil_int(safe_div(cus_gap, max(1e-9, cheats.rookie_productivity_vs_expert * 0.8), default=0.0)) if cus_gap > 0 else 0
    hire = int(clamp(hire, 0, 30))

    # Capex estimate placeholder (you can wire this to bottleneck module later)
    capex_est = 0.0

    rec_settings = {
        "Inventory": {
            "Parts/day (est.)": float(parts["parts_per_day"]),
            "Coverage days (est.)": float(cov),
            "ROP (no safety)": float(inv_rec["recommended_rop"]),
            "ROQ (EOQ, no safety)": float(inv_rec["recommended_roq"]),
        },
        "Workforce": {
            "Hire rookies (heuristic)": int(hire),
            "Rookie productivity": float(cheats.rookie_productivity_vs_expert),
            "Days to expert": float(cheats.days_to_become_expert),
        },
        "Pricing (Standard)": {
            "Suggested Std Product Price": float(price_sugg.get("suggested_price", 0.0)),
            "Method": "Regression" if price_sugg.get("method", 0.0) == 1.0 else "Fallback",
            "R2": float(price_sugg.get("r2", 0.0)),
        },
        "Machines (scenario)": {
            "S1 buy": float(mp.station1_buy),
            "S2 buy": float(mp.station2_buy),
            "S3 buy": float(mp.station3_buy),
            "CapEx estimate (placeholder)": float(capex_est),
        },
        "Notes": {
            "Capacity-aware pricing": "ถ้า Standard gap>0 หรือ EWL>95% → ราคาอาจไม่ทำให้ยอดขายเพิ่ม เพราะผลิตไม่ทัน",
            "Loan rule": "กู้เมื่อ ROI (Δprofit/day*horizon) > (interest+commission) และ cash runway ตึง",
        }
    }

    st.json(rec_settings)

    st.markdown("### Quick flags")
    flags = []
    if parts["parts_per_day"] > 0 and cov < cheats.lead_time_days:
        flags.append("🔴 Stockout risk: coverage < lead time")
    if float(base.get("cus_gap_avg", 0.0)) > 0:
        flags.append("🟠 Custom delivery gap > 0")
    if float(base.get("std_gap_avg", 0.0)) > 0:
        flags.append("🟠 Standard delivery gap > 0")
    if float(base.get("profit_day_avg", 0.0)) < 0:
        flags.append("🔴 Profit/day < 0")
    if flags:
        callout("warn", "Flags", " | ".join(flags))
    else:
        callout("ok", "Flags", "ไม่มีสัญญาณวิกฤตเด่นในช่วงที่เลือก")

st.caption("Tip: ถ้ากติกาเหมือนกันแต่ราคาเครื่องต่างกัน ให้ปรับ Machine Prices ใน sidebar แล้วผล capex/loan จะสมจริงขึ้น")
