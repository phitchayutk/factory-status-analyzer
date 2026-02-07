# ============================================================
# Factory Status Analyzer (Game Excel Export) — FULL-FILE + PRICE SUGGEST ✅
# ✅ Robust import (alias columns, pick best day)
# ✅ Per-user session isolation (เพื่อนเข้าลิงก์เดียวกันไม่เห็นค่ากัน)
# ✅ Full-file analysis (ทั้งไฟล์) + Trend + “ช่วงพัง”
# ✅ Suggest Standard Product Price from historical Price↔Demand (if varies)
# ✅ Fallback price rule using Market + Backlog/Fill-Rate when data insufficient
# ============================================================

import io
import math
import uuid
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st


# ============================================================
# Helpers
# ============================================================
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b not in (0, 0.0) else default

def money(x: float) -> str:
    return f"${x:,.2f}"

def num(x: float) -> str:
    return f"{x:,.2f}"

def to_float(x, default=0.0) -> float:
    try:
        if pd.isna(x):
            return float(default)
        return float(x)
    except Exception:
        return float(default)

def ceil_int(x: float) -> int:
    return int(math.ceil(max(0.0, x)))

def excel_file_from_bytes(xbytes: bytes) -> pd.ExcelFile:
    # reading .xlsx requires openpyxl installed on server
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

def as_numeric_series(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)


# ============================================================
# Inputs
# ============================================================
@dataclass
class InventoryInputs:
    inventory_level_parts: float = 0.0
    cost_per_part: float = 45.0
    order_fee: float = 1500.0
    lead_time_days: float = 4.0
    reorder_point: float = 0.0
    reorder_quantity: float = 0.0

@dataclass
class FinancialInputs:
    cash_on_hand: float = 0.0
    debt: float = 0.0
    normal_debt_apr: float = 0.365
    cash_interest_daily: float = 0.0005
    loan_commission_rate: float = 0.02

@dataclass
class WorkforceInputs:
    rookies: float = 0.0
    experts: float = 0.0
    days_to_become_expert: float = 15.0
    rookie_productivity_vs_expert: float = 0.40
    salary_rookie_per_day: float = 80.0
    salary_expert_per_day: float = 150.0
    overtime_cost_multiplier: float = 1.50

@dataclass
class StandardLineInputs:
    accepted_orders: float = 0.0
    accumulated_orders: float = 0.0
    deliveries: float = 0.0
    product_price: float = 0.0
    market_price: float = 0.0
    order_size_units: float = 60.0
    order_frequency_days: float = 5.0
    queue1_level: float = 0.0
    queue2_level: float = 0.0
    queue3_level: float = 0.0
    queue4_level: float = 0.0
    queue5_level: float = 0.0
    station1_machines: float = 0.0
    station1_output: float = 0.0
    initial_batch_output: float = 0.0
    manual_processing_output: float = 0.0
    final_batch_output: float = 0.0
    effective_work_load_pct: float = 0.0
    daily_demand_override: float = 0.0
    parts_per_unit: float = 2.0

def std_daily_demand(std: StandardLineInputs) -> float:
    if std.daily_demand_override and std.daily_demand_override > 0:
        return float(std.daily_demand_override)
    if std.order_frequency_days and std.order_frequency_days > 0:
        return float(std.order_size_units) / float(std.order_frequency_days)
    return float(std.accepted_orders)

@dataclass
class CustomLineInputs:
    accepted_orders: float = 0.0
    accumulated_orders: float = 0.0
    daily_demand: float = 0.0
    deliveries: float = 0.0
    average_lead_time: float = 0.0
    actual_price: float = 0.0
    queue1_level: float = 0.0
    queue2_level_first_pass: float = 0.0
    queue2_level_second_pass: float = 0.0
    queue3_level: float = 0.0
    station1_output: float = 0.0
    station2_capacity_alloc_first_pass_pct: float = 50.0
    station2_machines: float = 0.0
    station2_output_first_pass: float = 0.0
    station3_machines: float = 0.0
    station3_output: float = 0.0
    parts_per_unit: float = 1.0
    station2_passes_per_unit: float = 2.0
    max_orders_in_line: float = 450.0

@dataclass
class MachinePrices:
    station1_buy: float = 18000.0
    station1_sell: float = 8000.0
    station2_buy: float = 12000.0
    station2_sell: float = 6000.0
    station3_buy: float = 10000.0
    station3_sell: float = 5000.0


# ============================================================
# Column aliases
# ============================================================
COL = {
    "DAY": ["Day", "day", "DAY"],

    "INV_LEVEL": ["Inventory-Level", "Inventory Level", "Inventory_Level"],

    "CASH": ["Finance-Cash On Hand", "Cash On Hand", "Finance Cash On Hand", "Cash"],
    "DEBT": ["Finance-Debt", "Debt", "Finance Debt"],

    "ROOKIES": ["WorkForce-Rookies", "Workforce-Rookies", "Rookies", "Work Force-Rookies"],
    "EXPERTS": ["WorkForce-Experts", "Workforce-Experts", "Experts", "Work Force-Experts"],

    "STD_ACCEPT": ["Standard Orders-Accepted Orders", "Standard Accepted Orders", "Standard Accepted", "Accepted Orders"],
    "STD_ACCUM": ["Standard Orders-Accumulated Orders", "Standard Accumulated Orders", "Standard Accumulated", "Accumulated Orders"],
    "STD_DELIV": ["Standard Deliveries-Deliveries", "Standard Deliveries", "Deliveries", "Deliveries Out", "Deliveries_Out"],

    "STD_PRICE": ["Standard Deliveries-Product Price", "Product Price", "Std Product Price"],
    "STD_MKT": ["Standard Deliveries-Market Price", "Market Price", "Standard Market Price"],

    "FIN_SALES_STD_TD": ["Finance-Sales Standard *To Date", "Finance-Sales Standard To Date", "Sales Standard *To Date"],
    "FIN_SALES_CUS_TD": ["Finance-Sales Custom *To Date", "Finance-Sales Custom To Date", "Sales Custom *To Date"],
    "FIN_SALARIES_TD": ["Finance-Salaries *To Date", "Finance-Salaries To Date", "Salaries *To Date"],
    "FIN_HOLD_RAW_TD": ["Finance-Raw Inventory Holding Costs *To Date", "Raw Inventory Holding Costs *To Date"],
    "FIN_HOLD_CUS_TD": ["Finance-Custom Queues Holding Costs *To Date", "Custom Queues Holding Costs *To Date"],
    "FIN_HOLD_STD_TD": ["Finance-Standard Queues Holding Costs *To Date", "Standard Queues Holding Costs *To Date"],
    "FIN_DEBT_INT_TD": ["Finance-Debt Interest Paid *To Date", "Debt Interest Paid *To Date"],
    "FIN_LOAN_COM_TD": ["Finance-Loan Commission Paid *To Date", "Loan Commission Paid *To Date"],
}

CHEAT_DEFAULTS = {
    "lead_time_days": 4.0,
    "cost_per_part": 45.0,
    "raw_order_fee": 1500.0,
    "std_parts_per_unit": 2.0,
    "cus_parts_per_unit": 1.0,
    "loan_commission_rate": 0.02,
    "normal_debt_apr": 0.365,
    "cash_interest_daily": 0.0005,
    "days_to_become_expert": 15.0,
    "rookie_productivity_vs_expert": 0.40,
    "salary_rookie_per_day": 80.0,
    "salary_expert_per_day": 150.0,
}


# ============================================================
# ✅ Per-user session isolation
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
        "inventory": InventoryInputs(),
        "financial": FinancialInputs(),
        "workforce": WorkforceInputs(),
        "standard": StandardLineInputs(),
        "custom": CustomLineInputs(),
        "machine_prices": MachinePrices(),
        "import_day": None,
        "last_uploaded_bytes": None,
    }

S = st.session_state.sessions[SID]


# ============================================================
# Import utilities
# ============================================================
def pick_best_day(std_df, cus_df, fin_df) -> int:
    all_days = pd.concat(
        [
            safe_day_series(std_df, COL["DAY"]),
            safe_day_series(cus_df, COL["DAY"]),
            safe_day_series(fin_df, COL["DAY"]),
        ],
        ignore_index=True,
    )
    if all_days.empty:
        return 0
    return int(all_days.max())

def load_inputs_from_excel(xbytes: bytes, day: Optional[int] = None) -> Dict[str, object]:
    xl = excel_file_from_bytes(xbytes)

    std_df = read_sheet(xl, "Standard")
    cus_df = read_sheet(xl, "Custom")
    inv_df = read_sheet(xl, "Inventory")
    fin_df = read_sheet(xl, "Finance", "Financial")
    wf_df = read_sheet(xl, "WorkForce", "Workforce")

    if day is None:
        day = pick_best_day(std_df, cus_df, fin_df)

    def get_row(df: Optional[pd.DataFrame]) -> Optional[pd.Series]:
        if df is None:
            return None
        dcol = pick_col(df, COL["DAY"])
        if not dcol:
            return None
        dser = pd.to_numeric(df[dcol], errors="coerce").fillna(-1).astype(int)
        row = df.loc[dser == int(day)]
        return None if row.empty else row.iloc[0]

    std_r = get_row(std_df)
    cus_r = get_row(cus_df)
    inv_r = get_row(inv_df)
    fin_r = get_row(fin_df)
    wf_r = get_row(wf_df)

    inv = InventoryInputs()
    fin = FinancialInputs()
    wf = WorkforceInputs()
    std = StandardLineInputs()
    cus = CustomLineInputs()
    mp = MachinePrices()

    if inv_r is not None and inv_df is not None:
        inv.inventory_level_parts = getv(inv_r, inv_df, COL["INV_LEVEL"], 0.0)
    inv.cost_per_part = CHEAT_DEFAULTS["cost_per_part"]
    inv.order_fee = CHEAT_DEFAULTS["raw_order_fee"]
    inv.lead_time_days = CHEAT_DEFAULTS["lead_time_days"]

    if fin_r is not None and fin_df is not None:
        fin.cash_on_hand = getv(fin_r, fin_df, COL["CASH"], 0.0)
        fin.debt = getv(fin_r, fin_df, COL["DEBT"], 0.0)
    fin.normal_debt_apr = CHEAT_DEFAULTS["normal_debt_apr"]
    fin.cash_interest_daily = CHEAT_DEFAULTS["cash_interest_daily"]
    fin.loan_commission_rate = CHEAT_DEFAULTS["loan_commission_rate"]

    if wf_r is not None and wf_df is not None:
        wf.rookies = getv(wf_r, wf_df, COL["ROOKIES"], 0.0)
        wf.experts = getv(wf_r, wf_df, COL["EXPERTS"], 0.0)
    wf.days_to_become_expert = CHEAT_DEFAULTS["days_to_become_expert"]
    wf.rookie_productivity_vs_expert = CHEAT_DEFAULTS["rookie_productivity_vs_expert"]
    wf.salary_rookie_per_day = CHEAT_DEFAULTS["salary_rookie_per_day"]
    wf.salary_expert_per_day = CHEAT_DEFAULTS["salary_expert_per_day"]

    if std_r is not None and std_df is not None:
        std.accepted_orders = getv(std_r, std_df, COL["STD_ACCEPT"], 0.0)
        std.accumulated_orders = getv(std_r, std_df, COL["STD_ACCUM"], 0.0)
        std.deliveries = getv(std_r, std_df, COL["STD_DELIV"], 0.0)
        std.market_price = getv(std_r, std_df, COL["STD_MKT"], 0.0)
        std.product_price = getv(std_r, std_df, COL["STD_PRICE"], std.market_price)

    std.parts_per_unit = CHEAT_DEFAULTS["std_parts_per_unit"]
    cus.parts_per_unit = CHEAT_DEFAULTS["cus_parts_per_unit"]

    return {
        "day": int(day),
        "inventory": inv,
        "financial": fin,
        "workforce": wf,
        "standard": std,
        "custom": cus,
        "machine_prices": mp,
    }


# ============================================================
# Full-file dataframe builder
# ============================================================
def make_timeseries_from_excel(xbytes: bytes):
    xl = excel_file_from_bytes(xbytes)
    std_df = read_sheet(xl, "Standard")
    cus_df = read_sheet(xl, "Custom")
    inv_df = read_sheet(xl, "Inventory")
    fin_df = read_sheet(xl, "Finance", "Financial")

    def norm_day(df):
        if df is None:
            return None
        dcol = pick_col(df, COL["DAY"])
        if not dcol:
            return None
        out = df.copy()
        out["Day"] = pd.to_numeric(out[dcol], errors="coerce").fillna(-1).astype(int)
        out = out[out["Day"] >= 0].sort_values("Day")
        return out

    return tuple(map(norm_day, [std_df, cus_df, inv_df, fin_df]))


# ============================================================
# Finance daily delta (proxy)
# ============================================================
def _series_from_to_date(fin_df: pd.DataFrame, aliases: List[str]) -> pd.Series:
    c = pick_col(fin_df, aliases)
    return as_numeric_series(fin_df, c)

def finance_daily_delta(fin_df: pd.DataFrame) -> pd.DataFrame:
    df = fin_df.sort_values("Day").copy()

    sales_std_td = _series_from_to_date(df, COL["FIN_SALES_STD_TD"])
    sales_cus_td = _series_from_to_date(df, COL["FIN_SALES_CUS_TD"])
    salaries_td = _series_from_to_date(df, COL["FIN_SALARIES_TD"])
    h_raw_td = _series_from_to_date(df, COL["FIN_HOLD_RAW_TD"])
    h_cus_td = _series_from_to_date(df, COL["FIN_HOLD_CUS_TD"])
    h_std_td = _series_from_to_date(df, COL["FIN_HOLD_STD_TD"])
    int_td = _series_from_to_date(df, COL["FIN_DEBT_INT_TD"])
    com_td = _series_from_to_date(df, COL["FIN_LOAN_COM_TD"])

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

    return out


# ============================================================
# ✅ Standard price suggestion from whole-file (Price ↔ Demand)
# ============================================================
def build_standard_price_dataset(std_ts: pd.DataFrame) -> pd.DataFrame:
    """Return df with Day, Price, Market, DemandProxy, Deliveries, BacklogProxy, FillRateProxy."""
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

    # Demand proxy: use Accepted (if that column is meaningful daily)
    df["DemandProxy"] = df["Accepted"].clip(lower=0.0)

    # Backlog proxy: Accumulated - Deliveries (>=0)
    df["BacklogProxy"] = (df["Accumulated"] - df["Deliveries"]).clip(lower=0.0)

    # Fill rate proxy
    df["FillRateProxy"] = df["Deliveries"] / (df["DemandProxy"].replace(0, pd.NA))
    df["FillRateProxy"] = df["FillRateProxy"].fillna(1.0).clip(lower=0.0, upper=2.0)

    # Keep valid rows
    df = df[(df["Price"] > 0) & (df["DemandProxy"] >= 0)]
    return df


def fit_linear_demand(price: pd.Series, demand: pd.Series) -> Optional[Tuple[float, float, float]]:
    """
    Fit demand ≈ a + b*price  (expect b <= 0)
    Return (a, b, r2) or None if not enough variation.
    """
    x = price.astype(float).values
    y = demand.astype(float).values

    if len(x) < 8:
        return None

    # Need price variation
    if float(pd.Series(x).nunique()) < 3:
        return None

    # Basic OLS by formulas (no sklearn)
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
    """
    Suggest price that maximizes revenue = price * predicted_demand,
    with safety clamps around Market.
    Fallback rule when demand curve not learnable.
    """
    if std_price_df is None or std_price_df.empty:
        return {"suggested_price": 0.0, "method": 0.0, "r2": 0.0}

    # Use last available market as reference
    last_market = float(std_price_df["Market"].replace(0, pd.NA).dropna().iloc[-1]) if (std_price_df["Market"] > 0).any() else 0.0
    last_price = float(std_price_df["Price"].iloc[-1])
    last_fill = float(std_price_df["FillRateProxy"].iloc[-1])
    last_backlog = float(std_price_df["BacklogProxy"].iloc[-1])

    # Fit demand curve
    fit = fit_linear_demand(std_price_df["Price"], std_price_df["DemandProxy"])
    if fit is not None:
        a, b, r2 = fit

        # If b >= 0, demand not decreasing with price -> unsafe for optimization, fallback.
        if b < 0 and r2 >= -0.5:
            # Revenue max for linear demand: R(p) = p*(a+bp) -> derivative a+2bp = 0 -> p* = -a/(2b)
            p_star = -a / (2.0 * b)

            # Clamp around market if market exists; otherwise clamp around historical range
            if last_market > 0:
                lo, hi = 0.7 * last_market, 1.3 * last_market
            else:
                lo, hi = float(std_price_df["Price"].quantile(0.1)), float(std_price_df["Price"].quantile(0.9))
                if lo <= 0 or hi <= 0 or lo >= hi:
                    lo, hi = float(std_price_df["Price"].min()), float(std_price_df["Price"].max())

            p_suggest = float(clamp(p_star, lo, hi))

            return {
                "suggested_price": p_suggest,
                "method": 1.0,   # 1 = fitted demand curve
                "r2": float(r2),
                "last_price": last_price,
                "last_market": last_market,
            }

    # ------------------------
    # Fallback rule (market + service/backlog)
    # ------------------------
    # If backlog high or fill rate < 1 => raise price a bit (reduce demand)
    # If fill rate > 1 and backlog ~0 => can lower a bit (stimulate demand)
    base = last_market if last_market > 0 else last_price
    if base <= 0:
        base = 1.0

    adj = 0.0
    if last_backlog > 0 or last_fill < 0.95:
        adj = +0.08
    elif last_fill > 1.05 and last_backlog <= 0:
        adj = -0.05
    else:
        adj = 0.0

    lo, hi = (0.7 * base, 1.3 * base) if base > 0 else (0.0, 0.0)
    p_suggest = float(clamp(base * (1.0 + adj), lo, hi))

    return {
        "suggested_price": p_suggest,
        "method": 2.0,  # 2 = fallback rule
        "r2": 0.0,
        "last_price": last_price,
        "last_market": last_market,
        "last_fill_rate": last_fill,
        "last_backlog": last_backlog,
    }


# ============================================================
# (เดิม) Core recommendations (ROP/ROQ) แบบ snapshot วันเดียว
# ============================================================
def recommend_reorder_policy(inv: InventoryInputs, std: StandardLineInputs, cus: CustomLineInputs) -> Dict[str, float]:
    std_d = std_daily_demand(std)
    std_parts = std_d * std.parts_per_unit
    cus_parts = cus.daily_demand * cus.parts_per_unit
    D = std_parts + cus_parts

    h = 1.0
    Sfee = inv.order_fee
    rop = D * inv.lead_time_days
    roq = math.sqrt((2.0 * D * Sfee) / h) if D > 0 else 0.0

    return {
        "parts_per_day": D,
        "recommended_rop": rop,
        "recommended_roq": roq,
        "std_daily_demand": std_d,
        "std_parts_per_day": std_parts,
        "cus_parts_per_day": cus_parts,
    }


# ============================================================
# Streamlit UI
# ============================================================
st.set_page_config(page_title="Factory Status Analyzer", layout="wide")
st.title("🏭 Factory Status Analyzer")

tabs = st.tabs([
    "0) Import Excel",
    "1) Input (Snapshot)",
    "2) Dashboard (Snapshot+Trends)",
    "3) Full-file Analysis + Price Suggest"
])

# --------------------
# Tab 0: Import
# --------------------
with tabs[0]:
    st.subheader("Import Excel (Export จากเกม)")

    cA, cB = st.columns([1, 3])
    with cA:
        if st.button("🔄 Reset (เฉพาะฉัน)"):
            st.session_state.pop("sid", None)
            st.rerun()
    with cB:
        st.caption(f"Session: {SID[:8]} (แยกค่าต่อคน)")

    up = st.file_uploader("อัปโหลดไฟล์ .xlsx ที่ Export จากเกม", type=["xlsx"])

    if up is not None:
        try:
            xbytes = up.getvalue()
            S["last_uploaded_bytes"] = xbytes

            std_ts, cus_ts, inv_ts, fin_ts = make_timeseries_from_excel(xbytes)
            max_day = int(std_ts["Day"].max()) if std_ts is not None and not std_ts.empty else 0

            suggested = pick_best_day(std_ts, cus_ts, fin_ts)
            st.caption(f"ระบบแนะนำเลือก Day = {suggested}")

            day = st.number_input(
                "เลือก Day ที่ต้องการโหลดเข้าแบบฟอร์ม (snapshot)",
                min_value=0,
                max_value=max_day,
                value=int(suggested),
                step=1,
            )

            if st.button("✅ Load day นี้เข้าแบบฟอร์ม"):
                loaded = load_inputs_from_excel(xbytes, day=int(day))
                S["inventory"] = loaded["inventory"]
                S["financial"] = loaded["financial"]
                S["workforce"] = loaded["workforce"]
                S["standard"] = loaded["standard"]
                S["custom"] = loaded["custom"]
                S["machine_prices"] = loaded["machine_prices"]
                S["import_day"] = loaded["day"]
                st.success(f"โหลดข้อมูลสำเร็จ (Day {loaded['day']}) — ไปแท็บ Full-file ได้เลย")

        except ImportError as e:
            st.error("อ่านไฟล์ .xlsx ไม่ได้ เพราะขาด openpyxl")
            st.code("เพิ่มใน requirements.txt:\nopenpyxl")
            st.exception(e)
        except Exception as e:
            st.error("Import ล้มเหลว")
            st.exception(e)


# --------------------
# Tab 1: Snapshot input
# --------------------
with tabs[1]:
    st.subheader("Input (Snapshot)")

    inv = S["inventory"]
    fin = S["financial"]
    work = S["workforce"]
    std = S["standard"]
    cus = S["custom"]

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("### 📦 Inventory")
        inv.inventory_level_parts = st.number_input("Inventory Level (parts)", value=float(inv.inventory_level_parts), step=1.0)
        inv.order_fee = st.number_input("Order Fee", value=float(inv.order_fee), step=100.0)
        inv.lead_time_days = st.number_input("Lead Time (days)", value=float(inv.lead_time_days), step=1.0)

        st.markdown("### 💰 Finance")
        fin.cash_on_hand = st.number_input("Cash On Hand", value=float(fin.cash_on_hand), step=1000.0)
        fin.debt = st.number_input("Debt", value=float(fin.debt), step=1000.0)

    with c2:
        st.markdown("### 🧱 Standard (Snapshot)")
        std.accepted_orders = st.number_input("Std Accepted Orders", value=float(std.accepted_orders), step=1.0)
        std.deliveries = st.number_input("Std Deliveries", value=float(std.deliveries), step=1.0)
        std.product_price = st.number_input("Std Product Price", value=float(std.product_price), step=0.01)
        std.market_price = st.number_input("Std Market Price", value=float(std.market_price), step=0.01)
        std.order_size_units = st.number_input("Std Order Size (units)", value=float(std.order_size_units), step=1.0)
        std.order_frequency_days = st.number_input("Std Order Frequency (days)", value=float(std.order_frequency_days), step=1.0)
        std.daily_demand_override = st.number_input("Std Daily Demand Override", value=float(std.daily_demand_override), step=1.0)

    with c3:
        st.markdown("### 🧩 Custom (Snapshot)")
        cus.daily_demand = st.number_input("Custom Daily Demand", value=float(cus.daily_demand), step=0.01)
        cus.deliveries = st.number_input("Custom Deliveries", value=float(cus.deliveries), step=0.01)
        cus.average_lead_time = st.number_input("Custom Avg Lead Time", value=float(cus.average_lead_time), step=0.01)

    rec_inv = recommend_reorder_policy(inv, std, cus)
    st.markdown("### Snapshot Recommended (ROP/ROQ)")
    st.json({
        "parts_per_day": rec_inv["parts_per_day"],
        "ROP_no_safety": rec_inv["recommended_rop"],
        "ROQ_EOQ_no_safety": rec_inv["recommended_roq"],
        "std_daily_demand_used": rec_inv["std_daily_demand"],
    })


# --------------------
# Tab 2: Snapshot+Trends
# --------------------
with tabs[2]:
    st.subheader("Dashboard (Snapshot + Trends)")

    if S["last_uploaded_bytes"] is None:
        st.info("อัปโหลดไฟล์ในแท็บ Import ก่อน")
    else:
        std_ts, cus_ts, inv_ts, fin_ts = make_timeseries_from_excel(S["last_uploaded_bytes"])

        if fin_ts is not None:
            fin_daily = finance_daily_delta(fin_ts)

            cols1 = [c for c in ["Cash_On_Hand", "Debt"] if c in fin_daily.columns]
            if cols1:
                st.markdown("#### 💵 Finance — Cash & Debt")
                st.line_chart(fin_daily.set_index("Day")[cols1], height=220)

            cols2 = [c for c in ["Sales_per_Day", "Costs_Proxy_per_Day", "Profit_Proxy_per_Day"] if c in fin_daily.columns]
            if cols2:
                st.markdown("#### 📊 Finance — Sales / Cost / Profit (Proxy) per Day")
                st.line_chart(fin_daily.set_index("Day")[cols2], height=220)

        if std_ts is not None:
            price_c = pick_col(std_ts, COL["STD_PRICE"])
            mkt_c = pick_col(std_ts, COL["STD_MKT"])
            acc_c = pick_col(std_ts, COL["STD_ACCEPT"])
            del_c = pick_col(std_ts, COL["STD_DELIV"])

            if acc_c and del_c:
                st.markdown("#### 🧱 Standard — Accepted vs Deliveries")
                st.line_chart(std_ts.set_index("Day")[[acc_c, del_c]], height=220)

            if price_c and mkt_c:
                st.markdown("#### 🧱 Standard — Product Price vs Market Price")
                st.line_chart(std_ts.set_index("Day")[[price_c, mkt_c]], height=200)


# --------------------
# Tab 3: Full-file analysis + Price Suggest
# --------------------
with tabs[3]:
    st.subheader("Full-file Analysis + Suggest Standard Product Price")

    if S["last_uploaded_bytes"] is None:
        st.info("อัปโหลดไฟล์ในแท็บ Import ก่อน")
    else:
        std_ts, cus_ts, inv_ts, fin_ts = make_timeseries_from_excel(S["last_uploaded_bytes"])

        # --- Build dataset and suggest price ---
        if std_ts is None or std_ts.empty:
            st.warning("ไม่เจอชีท Standard หรือข้อมูลว่าง")
        else:
            std_price_df = build_standard_price_dataset(std_ts)

            # Range filter
            min_d = int(std_price_df["Day"].min()) if not std_price_df.empty else 0
            max_d = int(std_price_df["Day"].max()) if not std_price_df.empty else 0
            r = st.slider("เลือกช่วงวันสำหรับ Full-file analysis", min_d, max_d, (min_d, max_d))

            dfR = std_price_df[(std_price_df["Day"] >= r[0]) & (std_price_df["Day"] <= r[1])].copy()

            sugg = suggest_standard_price(dfR)

            st.markdown("### ✅ Suggested Standard Product Price")
            method_name = "Fitted demand curve (Price↔Demand)" if sugg.get("method", 0) == 1.0 else "Fallback rule (Market + Backlog/Fill)"
            st.info(
                f"Suggested Price: **{money(sugg.get('suggested_price', 0.0))}**  | "
                f"Method: {method_name}  | "
                f"R²: {num(sugg.get('r2', 0.0))}"
            )

            st.json(sugg)

            # --- Show key diagnostics across file ---
            st.markdown("### 📌 Standard — Full-file KPI (ช่วงที่เลือก)")
            avg_fill = float(dfR["FillRateProxy"].mean()) if not dfR.empty else 0.0
            avg_backlog = float(dfR["BacklogProxy"].mean()) if not dfR.empty else 0.0
            price_var = float(dfR["Price"].nunique()) if not dfR.empty else 0.0

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Avg Fill Rate (proxy)", num(avg_fill))
            c2.metric("Avg Backlog (proxy)", num(avg_backlog))
            c3.metric("Price unique values", str(int(price_var)))
            c4.metric("Days in range", str(int(len(dfR))))

            st.markdown("### 📈 Standard — Price vs DemandProxy (Scatter)")
            # show as table + simple line charts (streamlit native line_chart has no scatter)
            # We'll approximate scatter by showing a sorted-by-price line
            sc = dfR.sort_values("Price")[["Price", "DemandProxy"]].reset_index(drop=True)
            st.caption("หมายเหตุ: เป็นการเรียงตามราคาเพื่อดูแนวโน้ม Demand ลด/เพิ่มตามราคา")
            st.line_chart(sc.set_index("Price")[["DemandProxy"]], height=220)

            st.markdown("### 🧱 Standard — Backlog & Fill Rate Over Time")
            st.line_chart(dfR.set_index("Day")[["BacklogProxy", "FillRateProxy"]], height=220)

            st.markdown("### 🔍 ช่วงที่ “พัง” (Top 10 days)")
            # Define "badness" = backlog + (1-fill)*demand
            bad = dfR.copy()
            bad["BadScore"] = bad["BacklogProxy"] + (1.0 - bad["FillRateProxy"]).clip(lower=0.0) * bad["DemandProxy"]
            st.dataframe(
                bad.sort_values("BadScore", ascending=False)[
                    ["Day", "Price", "Market", "DemandProxy", "Deliveries", "BacklogProxy", "FillRateProxy", "BadScore"]
                ].head(10),
                use_container_width=True
            )
