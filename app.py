# ============================================================
# Factory Status Analyzer (Game Excel Export) — PRO + FULL ✅
# ✅ Keeps your ORIGINAL snapshot analysis (ครบ/แน่น/เหมือนตอนแรก)
# ✅ Robust import (alias columns, pick best day that has activity)
# ✅ Per-user session isolation (เพื่อนเข้าลิงก์เดียวกันไม่เห็นค่ากัน)
# ✅ Snapshot checklist + recommended settings (ROP/ROQ, S2 alloc, hire, machines)
# ✅ Full-file trends (optional, ไม่ทำให้ logic เดิมเพี้ยน)
# ✅ Pricing suggest (optional) + capacity-aware warning
# ✅ Why/What-if (optional): อธิบายเหตุผล + คาดการณ์ผลแบบ conservative
#
# Requirements (Streamlit Cloud):
#   pandas
#   openpyxl
#   streamlit
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
# Inputs (Snapshot)
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
    daily_demand_override: float = 0.0

    # Queues
    queue1_level: float = 0.0
    queue2_level: float = 0.0
    queue3_level: float = 0.0
    queue4_level: float = 0.0
    queue5_level: float = 0.0

    # Capacity signals
    station1_machines: float = 0.0
    station1_output: float = 0.0
    initial_batch_output: float = 0.0
    manual_processing_output: float = 0.0
    final_batch_output: float = 0.0
    effective_work_load_pct: float = 0.0

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

    # Queues
    queue1_level: float = 0.0
    queue2_level_first_pass: float = 0.0
    queue2_level_second_pass: float = 0.0
    queue3_level: float = 0.0

    # Capacity signals
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
# Column aliases (robust import)
# ============================================================
COL = {
    "DAY": ["Day", "day", "DAY"],

    "INV_LEVEL": ["Inventory-Level", "Inventory Level", "Inventory_Level"],

    "CASH": ["Finance-Cash On Hand", "Cash On Hand", "Finance Cash On Hand", "Cash"],
    "DEBT": ["Finance-Debt", "Debt", "Finance Debt"],

    "ROOKIES": ["WorkForce-Rookies", "Workforce-Rookies", "Rookies", "Work Force-Rookies"],
    "EXPERTS": ["WorkForce-Experts", "Workforce-Experts", "Experts", "Work Force-Experts"],

    # Standard Orders / Deliveries
    "STD_ACCEPT": ["Standard Orders-Accepted Orders", "Standard Accepted Orders", "Standard Accepted", "Accepted Orders"],
    "STD_ACCUM": ["Standard Orders-Accumulated Orders", "Standard Accumulated Orders", "Standard Accumulated", "Accumulated Orders"],
    "STD_DELIV": ["Standard Deliveries-Deliveries", "Standard Deliveries", "Deliveries", "Deliveries Out", "Deliveries_Out"],
    "STD_PRICE": ["Standard Deliveries-Product Price", "Product Price", "Std Product Price"],
    "STD_MKT": ["Standard Deliveries-Market Price", "Market Price", "Standard Market Price"],

    # Standard Queues
    "STD_Q1": ["Standard Queue 1-Level", "Standard Q1-Level", "Queue 1-Level", "Queue1 Level"],
    "STD_Q2": ["Standard Queue 2-Level", "Standard Q2-Level", "Queue 2-Level", "Queue2 Level"],
    "STD_Q3": ["Standard Queue 3-Level", "Standard Q3-Level", "Queue 3-Level", "Queue3 Level"],
    "STD_Q4": ["Standard Queue 4-Level", "Standard Q4-Level", "Queue 4-Level", "Queue4 Level"],
    "STD_Q5": ["Standard Queue 5-Level", "Standard Q5-Level", "Queue 5-Level", "Queue5 Level"],

    # Standard Capacity
    "STD_MACHINES": ["Standard Station 1-Number of Machines", "Station 1-Number of Machines", "Number of Machines"],
    "STD_S1_OUT": ["Standard Station 1-Output", "Station 1-Output", "Output"],
    "STD_IB_OUT": ["Standard Initial Batching-Output", "Initial Batching-Output"],
    "STD_MP_OUT": ["Standard Manual Processing-Output", "Manual Processing-Output"],
    "STD_FB_OUT": ["Standard Final Batching-Output", "Final Batching-Output"],
    "STD_EWL": ["Standard Manual Processing-Effective Work Load (%)", "Effective Work Load (%)", "Effective Work Load"],

    # Custom Orders / Deliveries
    "CUS_DEMAND": ["Custom Orders-Demand", "Daily Demand", "Demand"],
    "CUS_ACCEPT": ["Custom Orders-Accepted Orders", "Custom Accepted Orders", "Accepted Orders"],
    "CUS_ACCUM": ["Custom Orders-Accumulated Orders", "Custom Accumulated Orders", "Accumulated Orders"],
    "CUS_DELIV": ["Custom Deliveries-Deliveries", "Deliveries", "Deliveries Out"],
    "CUS_LT": ["Custom Deliveries-Average Lead Time", "Average Lead Time", "Lead Time"],
    "CUS_PRICE": ["Custom Deliveries-Actual Price", "Actual Price"],

    # Custom Queues
    "CUS_Q1": ["Custom Queue 1-Level", "Queue 1-Level", "Level", "Queue1 Level"],
    "CUS_Q2_1": ["Custom Queue 2-Level First Pass", "Level First Pass", "Q2 First Pass"],
    "CUS_Q2_2": ["Custom Queue 2-Level Second Pass", "Level Second Pass", "Q2 Second Pass"],
    "CUS_Q3": ["Custom Queue 3-Level", "Queue 3-Level", "Level", "Queue3 Level"],

    # Custom Capacity
    "CUS_S1_OUT": ["Custom Station 1-Output", "Output"],
    "CUS_S2_MACH": ["Custom Station 2-Number of Machines", "Number of Machines"],
    "CUS_S2_OUT_1": ["Custom Station 2-Output First Pass", "Output First Pass"],
    "CUS_S3_MACH": ["Custom Station 3-Number of Machines", "Number of Machines"],
    "CUS_S3_OUT": ["Custom Station 3-Output", "Output"],

    # Finance to-date (profit proxy)
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
        "last_loaded_snapshot": {},
    }

S = st.session_state.sessions[SID]


# ============================================================
# Import utilities
# ============================================================
def pick_best_day(std_df, cus_df, fin_df) -> int:
    """Pick latest day that has real activity (เหมือนแนวเดิมของคุณ)."""
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

    max_day = int(all_days.max())

    def score_day(d: int) -> float:
        s = 0.0

        if cus_df is not None:
            dcol = pick_col(cus_df, COL["DAY"])
            if dcol:
                r = cus_df.loc[pd.to_numeric(cus_df[dcol], errors="coerce").fillna(-1).astype(int) == d]
                if not r.empty:
                    row = r.iloc[0]
                    s += abs(getv(row, cus_df, COL["CUS_DEMAND"]))
                    s += abs(getv(row, cus_df, COL["CUS_DELIV"]))
                    s += abs(getv(row, cus_df, COL["CUS_LT"]))

        if std_df is not None:
            dcol = pick_col(std_df, COL["DAY"])
            if dcol:
                r = std_df.loc[pd.to_numeric(std_df[dcol], errors="coerce").fillna(-1).astype(int) == d]
                if not r.empty:
                    row = r.iloc[0]
                    s += abs(getv(row, std_df, COL["STD_ACCEPT"]))
                    s += abs(getv(row, std_df, COL["STD_DELIV"]))
                    s += abs(getv(row, std_df, COL["STD_MP_OUT"]))
                    s += abs(getv(row, std_df, COL["STD_EWL"]))

        if fin_df is not None:
            dcol = pick_col(fin_df, COL["DAY"])
            if dcol:
                r = fin_df.loc[pd.to_numeric(fin_df[dcol], errors="coerce").fillna(-1).astype(int) == d]
                if not r.empty:
                    row = r.iloc[0]
                    s += abs(getv(row, fin_df, COL["CASH"]))

        return s

    for d in range(max_day, -1, -1):
        if score_day(d) > 0:
            return d

    return max_day

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

    # Inventory
    if inv_r is not None and inv_df is not None:
        inv.inventory_level_parts = getv(inv_r, inv_df, COL["INV_LEVEL"], 0.0)
    inv.cost_per_part = CHEAT_DEFAULTS["cost_per_part"]
    inv.order_fee = CHEAT_DEFAULTS["raw_order_fee"]
    inv.lead_time_days = CHEAT_DEFAULTS["lead_time_days"]

    # Finance
    if fin_r is not None and fin_df is not None:
        fin.cash_on_hand = getv(fin_r, fin_df, COL["CASH"], 0.0)
        fin.debt = getv(fin_r, fin_df, COL["DEBT"], 0.0)
    fin.normal_debt_apr = CHEAT_DEFAULTS["normal_debt_apr"]
    fin.cash_interest_daily = CHEAT_DEFAULTS["cash_interest_daily"]
    fin.loan_commission_rate = CHEAT_DEFAULTS["loan_commission_rate"]

    # Workforce
    if wf_r is not None and wf_df is not None:
        wf.rookies = getv(wf_r, wf_df, COL["ROOKIES"], 0.0)
        wf.experts = getv(wf_r, wf_df, COL["EXPERTS"], 0.0)
    wf.days_to_become_expert = CHEAT_DEFAULTS["days_to_become_expert"]
    wf.rookie_productivity_vs_expert = CHEAT_DEFAULTS["rookie_productivity_vs_expert"]
    wf.salary_rookie_per_day = CHEAT_DEFAULTS["salary_rookie_per_day"]
    wf.salary_expert_per_day = CHEAT_DEFAULTS["salary_expert_per_day"]

    # Standard (FULL like your original)
    if std_r is not None and std_df is not None:
        std.accepted_orders = getv(std_r, std_df, COL["STD_ACCEPT"], 0.0)
        std.accumulated_orders = getv(std_r, std_df, COL["STD_ACCUM"], 0.0)
        std.deliveries = getv(std_r, std_df, COL["STD_DELIV"], 0.0)

        std.market_price = getv(std_r, std_df, COL["STD_MKT"], 0.0)
        std.product_price = getv(std_r, std_df, COL["STD_PRICE"], std.market_price)

        std.queue1_level = getv(std_r, std_df, COL["STD_Q1"], 0.0)
        std.queue2_level = getv(std_r, std_df, COL["STD_Q2"], 0.0)
        std.queue3_level = getv(std_r, std_df, COL["STD_Q3"], 0.0)
        std.queue4_level = getv(std_r, std_df, COL["STD_Q4"], 0.0)
        std.queue5_level = getv(std_r, std_df, COL["STD_Q5"], 0.0)

        std.station1_machines = getv(std_r, std_df, COL["STD_MACHINES"], 0.0)
        std.station1_output = getv(std_r, std_df, COL["STD_S1_OUT"], 0.0)
        std.initial_batch_output = getv(std_r, std_df, COL["STD_IB_OUT"], 0.0)
        std.manual_processing_output = getv(std_r, std_df, COL["STD_MP_OUT"], 0.0)
        std.final_batch_output = getv(std_r, std_df, COL["STD_FB_OUT"], 0.0)
        std.effective_work_load_pct = getv(std_r, std_df, COL["STD_EWL"], 0.0)

    std.parts_per_unit = CHEAT_DEFAULTS["std_parts_per_unit"]

    # Custom (FULL like your original)
    if cus_r is not None and cus_df is not None:
        cus.accepted_orders = getv(cus_r, cus_df, COL["CUS_ACCEPT"], 0.0)
        cus.accumulated_orders = getv(cus_r, cus_df, COL["CUS_ACCUM"], 0.0)
        cus.daily_demand = getv(cus_r, cus_df, COL["CUS_DEMAND"], 0.0)

        cus.deliveries = getv(cus_r, cus_df, COL["CUS_DELIV"], 0.0)
        cus.average_lead_time = getv(cus_r, cus_df, COL["CUS_LT"], 0.0)
        cus.actual_price = getv(cus_r, cus_df, COL["CUS_PRICE"], 0.0)

        cus.queue1_level = getv(cus_r, cus_df, COL["CUS_Q1"], 0.0)
        cus.queue2_level_first_pass = getv(cus_r, cus_df, COL["CUS_Q2_1"], 0.0)
        cus.queue2_level_second_pass = getv(cus_r, cus_df, COL["CUS_Q2_2"], 0.0)
        cus.queue3_level = getv(cus_r, cus_df, COL["CUS_Q3"], 0.0)

        cus.station1_output = getv(cus_r, cus_df, COL["CUS_S1_OUT"], 0.0)
        cus.station2_output_first_pass = getv(cus_r, cus_df, COL["CUS_S2_OUT_1"], 0.0)
        cus.station2_machines = getv(cus_r, cus_df, COL["CUS_S2_MACH"], 0.0)
        cus.station3_output = getv(cus_r, cus_df, COL["CUS_S3_OUT"], 0.0)
        cus.station3_machines = getv(cus_r, cus_df, COL["CUS_S3_MACH"], 0.0)

    cus.parts_per_unit = CHEAT_DEFAULTS["cus_parts_per_unit"]
    cus.station2_passes_per_unit = 2.0
    cus.max_orders_in_line = 450.0

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
# Core snapshot recommendations (เหมือนเดิม)
# ============================================================
def recommend_reorder_policy(inv: InventoryInputs, std: StandardLineInputs, cus: CustomLineInputs) -> Dict[str, float]:
    std_d = std_daily_demand(std)
    std_parts = std_d * std.parts_per_unit
    cus_parts = cus.daily_demand * cus.parts_per_unit
    D = std_parts + cus_parts  # parts/day

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

def diagnose_inventory(inv: InventoryInputs, parts_per_day: float) -> Dict[str, float]:
    coverage_days = safe_div(inv.inventory_level_parts, parts_per_day, default=0.0)
    return {"coverage_days": coverage_days}

def recommend_station2_allocation(cus: CustomLineInputs) -> Dict[str, float]:
    q1 = cus.queue2_level_first_pass
    q2 = cus.queue2_level_second_pass
    total = q1 + q2 + 1e-9
    imbalance = (q1 - q2) / total  # + means first pass bigger
    suggested = 50.0 + (imbalance * 25.0)
    suggested = clamp(suggested, 10.0, 90.0)
    return {"suggested_alloc_first_pass_pct": suggested, "queue_imbalance": imbalance}

def diagnose_custom_flow(cus: CustomLineInputs) -> Dict[str, float]:
    demand_gap = cus.daily_demand - cus.deliveries
    wip_proxy = cus.queue1_level + cus.queue2_level_first_pass + cus.queue2_level_second_pass + cus.queue3_level
    backlog_proxy = max(0.0, cus.accumulated_orders - cus.deliveries)
    return {"custom_demand_gap": demand_gap, "custom_wip_proxy": wip_proxy, "custom_backlog_proxy": backlog_proxy}

def diagnose_standard_flow(std: StandardLineInputs) -> Dict[str, float]:
    d = std_daily_demand(std)
    demand_gap = d - std.deliveries
    wip_proxy = std.queue1_level + std.queue2_level + std.queue3_level + std.queue4_level + std.queue5_level
    backlog_proxy = max(0.0, std.accumulated_orders - std.deliveries)
    return {"std_demand_gap": demand_gap, "std_wip_proxy": wip_proxy, "std_backlog_proxy": backlog_proxy}

def pick_custom_bottleneck(cus: CustomLineInputs) -> str:
    if cus.queue2_level_second_pass > cus.queue2_level_first_pass * 1.2 and cus.queue2_level_second_pass > 5:
        return "S2"
    s1, s2, s3 = cus.station1_output, cus.station2_output_first_pass, cus.station3_output
    candidates = [(s1, "S1"), (s2, "S2"), (s3, "S3")]
    positive = [(v, name) for v, name in candidates if v > 0]
    return "S2" if not positive else min(positive, key=lambda x: x[0])[1]

def recommend_capacity_and_hiring(
    cus: CustomLineInputs,
    wf: WorkforceInputs,
    mp: MachinePrices,
    target_fill_ratio: float = 1.0,
) -> Dict[str, float]:
    demand = cus.daily_demand
    deliveries = cus.deliveries
    gap = max(0.0, demand - deliveries) * target_fill_ratio

    bottleneck = pick_custom_bottleneck(cus)

    s1_total = cus.station1_output
    s2_total = cus.station2_output_first_pass
    s3_total = cus.station3_output

    s2_per_machine = safe_div(s2_total, max(1.0, cus.station2_machines), default=0.0)
    s3_per_machine = safe_div(s3_total, max(1.0, cus.station3_machines), default=0.0)

    add_s1 = add_s2 = add_s3 = 0

    if gap <= 0:
        return {
            "custom_gap": 0.0,
            "bottleneck_stage": bottleneck,
            "add_station1": 0,
            "add_station2": 0,
            "add_station3": 0,
            "hire_rookies": 0,
            "capex_estimate": 0.0,
        }

    if bottleneck == "S2":
        add_s2 = ceil_int(safe_div(gap, max(1e-9, s2_per_machine), default=0.0)) if s2_per_machine > 0 else 1
    elif bottleneck == "S3":
        add_s3 = ceil_int(safe_div(gap, max(1e-9, s3_per_machine), default=0.0)) if s3_per_machine > 0 else 1
    else:
        add_s1 = 0

    rookie_prod = wf.rookie_productivity_vs_expert if wf.rookie_productivity_vs_expert > 0 else 0.40

    base = max(1.0, {"S1": s1_total, "S2": s2_total, "S3": s3_total}.get(bottleneck, s2_total))
    expert_equiv_needed = gap / base
    hire_rookies = ceil_int(expert_equiv_needed / rookie_prod)
    hire_rookies = max(1, hire_rookies)

    capex = (
        add_s1 * mp.station1_buy +
        add_s2 * mp.station2_buy +
        add_s3 * mp.station3_buy
    )

    return {
        "custom_gap": gap,
        "bottleneck_stage": bottleneck,
        "add_station1": add_s1,
        "add_station2": add_s2,
        "add_station3": add_s3,
        "hire_rookies": hire_rookies,
        "capex_estimate": capex,
    }

def build_status_and_checklist(
    inv: InventoryInputs,
    fin: FinancialInputs,
    work: WorkforceInputs,
    std: StandardLineInputs,
    cus: CustomLineInputs,
    mp: MachinePrices,
) -> Tuple[str, List[Dict[str, str]], Dict[str, float], Dict[str, float], List[str]]:

    rec_inv = recommend_reorder_policy(inv, std, cus)
    inv_diag = diagnose_inventory(inv, rec_inv["parts_per_day"])

    cus_diag = diagnose_custom_flow(cus)
    std_diag = diagnose_standard_flow(std)

    alloc = recommend_station2_allocation(cus)
    caprec = recommend_capacity_and_hiring(cus, work, mp)

    metrics = {
        **rec_inv,
        **inv_diag,
        **cus_diag,
        **std_diag,
        "suggested_station2_alloc_first_pass_pct": alloc["suggested_alloc_first_pass_pct"],
        "queue_imbalance": alloc["queue_imbalance"],
        "std_product_price": std.product_price,
        "std_market_price": std.market_price,
        "std_ewl": std.effective_work_load_pct,
        "std_mp_out": std.manual_processing_output,
        "std_wip_proxy": std_diag["std_wip_proxy"],
        "cus_wip_proxy": cus_diag["custom_wip_proxy"],
    }

    reasons: List[str] = []
    checklist: List[Dict[str, str]] = []
    severity = 0

    # Inventory coverage vs lead time
    if rec_inv["parts_per_day"] > 0 and inv_diag["coverage_days"] < inv.lead_time_days:
        severity += 2
        reasons.append("Raw parts coverage < lead time → เสี่ยง stockout → ส่งของไม่ทัน → backlog โต")
        checklist.append({
            "area": "Inventory",
            "finding": f"สต็อกคุ้มครองได้ ~{num(inv_diag['coverage_days'])} วัน (< lead time {num(inv.lead_time_days)}d)",
            "action": "ตั้ง ROP ให้พอช่วง lead time (ยังไม่กัน safety)",
            "recommended_value": f"ROP≈{num(rec_inv['recommended_rop'])} | ROQ≈{num(rec_inv['recommended_roq'])}",
        })

    # Standard demand gap
    if metrics["std_daily_demand"] > 0 and std_diag["std_demand_gap"] > 0:
        severity += 1
        if std.effective_work_load_pct >= 95 or std.manual_processing_output > 0:
            reasons.append("Standard ส่งมอบไม่ทัน + Workload สูง → demand ถูกจำกัดด้วย capacity (price-fit อาจหลอกได้)")
        checklist.append({
            "area": "Standard Line",
            "finding": f"Standard ส่งมอบไม่ทัน demand (gap {num(std_diag['std_demand_gap'])}/day)",
            "action": "ดูคิว+คอขวด (Initial/Manual/Final) แล้วเพิ่ม capacity จุดตัน",
            "recommended_value": f"WIP≈{num(std_diag['std_wip_proxy'])} | EWL≈{num(std.effective_work_load_pct)}%",
        })

    # Custom demand gap
    if cus.daily_demand > 0 and cus_diag["custom_demand_gap"] > 0:
        severity += 2
        reasons.append("Custom gap > 0 → backlog + lead time พุ่ง (มักมาจาก Q2 pass imbalance หรือ bottleneck stage)")
        checklist.append({
            "area": "Custom Line",
            "finding": f"Custom ส่งมอบไม่ทัน demand (gap {num(cus_diag['custom_demand_gap'])}/day)",
            "action": "แก้คอขวด + ปรับ Station2 allocation",
            "recommended_value": f"Station2 First Pass≈{num(alloc['suggested_alloc_first_pass_pct'])}%",
        })

    if cus.average_lead_time >= 10:
        severity += 1
        reasons.append("Lead time สูง = WIP/คิวค้างสะสม (โดยเฉพาะ Q2 second pass)")
        checklist.append({
            "area": "Custom Lead Time",
            "finding": f"Average Lead Time สูง ({num(cus.average_lead_time)} days)",
            "action": "ลดคิวที่พองที่สุดก่อน (Q2 second pass มักทำให้ lead time พุ่ง)",
            "recommended_value": f"Q2(first)={num(cus.queue2_level_first_pass)} | Q2(second)={num(cus.queue2_level_second_pass)}",
        })

    if caprec["custom_gap"] > 0:
        severity += 1
        checklist.append({
            "area": "Capacity + Workforce",
            "finding": f"คอขวดโดยประมาณ: {caprec['bottleneck_stage']} | gap≈{num(caprec['custom_gap'])}/day",
            "action": "เพิ่มเครื่อง/คนให้ตรงคอขวด (Hire ได้แค่ Rookie → 15 วันถึงแรงขึ้น)",
            "recommended_value": (
                f"+S1:{int(caprec['add_station1'])}, +S2:{int(caprec['add_station2'])}, +S3:{int(caprec['add_station3'])} | "
                f"Hire Rookie:{int(caprec['hire_rookies'])} | "
                f"CapEx≈{money(caprec['capex_estimate'])}"
            ),
        })

    status = "CRITICAL" if severity >= 5 else ("WARNING" if severity >= 2 else "OK")
    if not reasons:
        reasons = ["ไม่มีสัญญาณผิดปกติเด่นจาก snapshot (หรือ demand เป็น 0)"]

    return status, checklist, metrics, caprec, reasons


# ============================================================
# Full-file timeseries (optional) + Profit proxy
# ============================================================
def make_timeseries_from_excel(xbytes: bytes):
    xl = excel_file_from_bytes(xbytes)
    std_df = read_sheet(xl, "Standard")
    cus_df = read_sheet(xl, "Custom")
    inv_df = read_sheet(xl, "Inventory")
    fin_df = read_sheet(xl, "Finance", "Financial")
    wf_df = read_sheet(xl, "WorkForce", "Workforce")

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

    return tuple(map(norm_day, [std_df, cus_df, inv_df, fin_df, wf_df]))

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
# Pricing suggestion (optional) — conservative + capacity-aware warning
# ============================================================
def build_standard_price_dataset(std_ts: pd.DataFrame) -> pd.DataFrame:
    price_c = pick_col(std_ts, COL["STD_PRICE"])
    mkt_c = pick_col(std_ts, COL["STD_MKT"])
    acc_c = pick_col(std_ts, COL["STD_ACCEPT"])
    del_c = pick_col(std_ts, COL["STD_DELIV"])
    accum_c = pick_col(std_ts, COL["STD_ACCUM"])
    ewl_c = pick_col(std_ts, COL["STD_EWL"])
    mp_out_c = pick_col(std_ts, COL["STD_MP_OUT"])

    df = pd.DataFrame({"Day": std_ts["Day"]})
    df["Price"] = as_numeric_series(std_ts, price_c)
    df["Market"] = as_numeric_series(std_ts, mkt_c)
    df["Accepted"] = as_numeric_series(std_ts, acc_c)
    df["Deliveries"] = as_numeric_series(std_ts, del_c)
    df["Accumulated"] = as_numeric_series(std_ts, accum_c)
    df["EWL"] = as_numeric_series(std_ts, ewl_c)
    df["MP_Out"] = as_numeric_series(std_ts, mp_out_c)

    # DemandProxy (ดีที่สุดคือ accepted ต่อวัน ถ้ามี)
    df["DemandProxy"] = df["Accepted"].clip(lower=0.0)

    # Backlog proxy
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
    last_ewl = float(std_price_df["EWL"].iloc[-1]) if "EWL" in std_price_df.columns else 0.0

    fit = fit_linear_demand(std_price_df["Price"], std_price_df["DemandProxy"])
    if fit is not None:
        a, b, r2 = fit
        if b < 0 and r2 >= -0.5:
            p_star = -a / (2.0 * b)
            if last_market > 0:
                lo, hi = 0.8 * last_market, 1.2 * last_market
            else:
                lo, hi = float(std_price_df["Price"].quantile(0.1)), float(std_price_df["Price"].quantile(0.9))
                if lo <= 0 or hi <= 0 or lo >= hi:
                    lo, hi = float(std_price_df["Price"].min()), float(std_price_df["Price"].max())

            p_suggest = float(clamp(p_star, lo, hi))
            return {
                "suggested_price": p_suggest,
                "method": 1.0,
                "r2": float(r2),
                "last_price": last_price,
                "last_market": last_market,
                "last_fill_rate": last_fill,
                "last_backlog": last_backlog,
                "last_ewl": last_ewl,
                "a": float(a),
                "b": float(b),
            }

    # Fallback: market ± by service/backlog
    base = last_market if last_market > 0 else last_price
    if base <= 0:
        base = 1.0

    adj = 0.0
    if last_backlog > 0 or last_fill < 0.95:
        adj = +0.08
    elif last_fill > 1.05 and last_backlog <= 0:
        adj = -0.05

    p_suggest = float(clamp(base * (1.0 + adj), 0.8 * base, 1.2 * base))
    return {
        "suggested_price": p_suggest,
        "method": 2.0,
        "r2": 0.0,
        "last_price": last_price,
        "last_market": last_market,
        "last_fill_rate": last_fill,
        "last_backlog": last_backlog,
        "last_ewl": last_ewl,
    }

def conservative_what_if_std(
    current_price: float,
    suggested_price: float,
    fit_info: Dict[str, float],
    current_demand_proxy: float,
    current_deliveries: float,
) -> Dict[str, float]:
    """
    What-if แบบ conservative:
    - ถ้ามี slope (b) ใช้ demand_hat = a + b*P
    - จำกัดยอดขายด้วย capacity (ใช้ deliveries ปัจจุบันเป็น proxy)
    - แสดง revenue delta แบบง่าย (ไม่เดารายจ่ายทั้งหมด)
    """
    a = float(fit_info.get("a", 0.0))
    b = float(fit_info.get("b", 0.0))
    method = float(fit_info.get("method", 0.0))

    # baseline
    d0 = max(0.0, float(current_demand_proxy))
    cap = max(0.0, float(current_deliveries))

    # demand prediction
    if method == 1.0 and b != 0.0:
        d1 = max(0.0, a + b * float(suggested_price))
    else:
        # heuristic elasticity: +10% price => -5% demand (ปรับได้ทีหลัง)
        if current_price <= 0:
            d1 = d0
        else:
            pct = (float(suggested_price) - float(current_price)) / float(current_price)
            d1 = max(0.0, d0 * (1.0 - 0.5 * pct))

    sold0 = min(d0, cap) if cap > 0 else d0
    sold1 = min(d1, cap) if cap > 0 else d1

    rev0 = float(current_price) * sold0
    rev1 = float(suggested_price) * sold1

    return {
        "demand_est_before": d0,
        "demand_est_after": d1,
        "capacity_proxy": cap,
        "sold_before": sold0,
        "sold_after": sold1,
        "revenue_before": rev0,
        "revenue_after": rev1,
        "revenue_delta": (rev1 - rev0),
    }


# ============================================================
# UI
# ============================================================
st.set_page_config(page_title="Factory Status Analyzer", layout="wide")

# --- Header (clean/professional)
top = st.container()
with top:
    c1, c2 = st.columns([2, 1])
    with c1:
        st.title("🏭 Factory Status Analyzer")
        st.caption("Snapshot-first (เหมือนสคริปต์แรก) + เพิ่ม Trends/Pricing/What-if แบบไม่ทำให้ logic เดิมเพี้ยน")
    with c2:
        st.markdown("#### Session")
        st.code(SID[:8])
        if st.button("🔄 Reset (เฉพาะฉัน)"):
            st.session_state.pop("sid", None)
            st.rerun()

tabs = st.tabs([
    "0) Import Excel",
    "1) Input (Snapshot override)",
    "2) Dashboard (Snapshot)",
    "3) Trends (Full-file)",
    "4) Pricing (Full-file)",
    "5) Why + What-if + Loan",
])

# --------------------
# Tab 0: Import
# --------------------
with tabs[0]:
    st.subheader("Import Excel (Export จากเกม)")
    st.write("อัปโหลดไฟล์ .xlsx จากเกม แล้วเลือก Day เพื่อโหลดเข้า Snapshot Analyzer (ครบเหมือนเดิม)")

    up = st.file_uploader("Upload .xlsx", type=["xlsx"])

    if up is not None:
        try:
            xbytes = up.getvalue()
            S["last_uploaded_bytes"] = xbytes

            # reset slider state on new upload
            st.session_state.pop("fullfile_day_range", None)

            xl = excel_file_from_bytes(xbytes)
            std_df = read_sheet(xl, "Standard")
            cus_df = read_sheet(xl, "Custom")
            fin_df = read_sheet(xl, "Finance", "Financial")

            all_days = pd.concat(
                [
                    safe_day_series(std_df, COL["DAY"]),
                    safe_day_series(cus_df, COL["DAY"]),
                    safe_day_series(fin_df, COL["DAY"]),
                ],
                ignore_index=True,
            )
            max_day = int(all_days.max()) if not all_days.empty else 0
            suggested = pick_best_day(std_df, cus_df, fin_df)

            st.info(f"ระบบแนะนำ Day = {suggested} (วันล่าสุดที่มี activity)")

            day = st.number_input("เลือก Day ที่ต้องการโหลดเข้า Snapshot", min_value=0, max_value=max_day, value=int(suggested), step=1)

            if st.button("✅ Load Day นี้เข้า Snapshot"):
                loaded = load_inputs_from_excel(xbytes, day=int(day))
                S["inventory"] = loaded["inventory"]
                S["financial"] = loaded["financial"]
                S["workforce"] = loaded["workforce"]
                S["standard"] = loaded["standard"]
                S["custom"] = loaded["custom"]
                S["machine_prices"] = loaded["machine_prices"]
                S["import_day"] = loaded["day"]
                S["last_loaded_snapshot"] = {"day": loaded["day"]}
                st.success(f"โหลดสำเร็จ: Day {loaded['day']} → ไปแท็บ Dashboard ได้เลย")

        except ImportError as e:
            st.error("อ่านไฟล์ .xlsx ไม่ได้ (ขาด openpyxl)")
            st.code("เพิ่มใน requirements.txt:\nopenpyxl")
            st.exception(e)
        except Exception as e:
            st.error("Import ล้มเหลว")
            st.exception(e)

# --------------------
# Tab 1: Snapshot input override (ครบเหมือนเดิม)
# --------------------
with tabs[1]:
    st.subheader("Input (Snapshot override)")
    st.caption("แก้ค่าได้หลัง import — เหมาะเวลาจะลอง what-if แบบเร็ว ๆ")

    inv: InventoryInputs = S["inventory"]
    fin: FinancialInputs = S["financial"]
    wf: WorkforceInputs = S["workforce"]
    std: StandardLineInputs = S["standard"]
    cus: CustomLineInputs = S["custom"]
    mp: MachinePrices = S["machine_prices"]

    colA, colB, colC = st.columns([1.1, 1.1, 1.1])

    with colA:
        st.markdown("### 📦 Inventory")
        inv.inventory_level_parts = st.number_input("Inventory Level (parts)", value=float(inv.inventory_level_parts), step=1.0)
        inv.cost_per_part = st.number_input("Cost per Part", value=float(inv.cost_per_part), step=1.0)
        inv.order_fee = st.number_input("Order Fee", value=float(inv.order_fee), step=100.0)
        inv.lead_time_days = st.number_input("Lead Time (days)", value=float(inv.lead_time_days), step=1.0)
        inv.reorder_point = st.number_input("Current ROP (optional)", value=float(inv.reorder_point), step=1.0)
        inv.reorder_quantity = st.number_input("Current ROQ (optional)", value=float(inv.reorder_quantity), step=1.0)

        st.markdown("### 💰 Finance")
        fin.cash_on_hand = st.number_input("Cash On Hand", value=float(fin.cash_on_hand), step=1000.0)
        fin.debt = st.number_input("Debt", value=float(fin.debt), step=1000.0)
        with st.expander("Loan/Interest assumptions (cheat defaults)"):
            fin.normal_debt_apr = st.number_input("Normal Debt APR", value=float(fin.normal_debt_apr), step=0.001, format="%.3f")
            fin.cash_interest_daily = st.number_input("Cash Interest (daily)", value=float(fin.cash_interest_daily), step=0.0001, format="%.4f")
            fin.loan_commission_rate = st.number_input("Loan Commission Rate", value=float(fin.loan_commission_rate), step=0.001, format="%.3f")

    with colB:
        st.markdown("### 👷 Workforce")
        wf.rookies = st.number_input("Rookies", value=float(wf.rookies), step=1.0)
        wf.experts = st.number_input("Experts", value=float(wf.experts), step=1.0)
        with st.expander("Workforce constants (cheat defaults)"):
            wf.days_to_become_expert = st.number_input("Days to become Expert", value=float(wf.days_to_become_expert), step=1.0)
            wf.rookie_productivity_vs_expert = st.number_input("Rookie productivity vs Expert", value=float(wf.rookie_productivity_vs_expert), step=0.05)
            wf.salary_rookie_per_day = st.number_input("Rookie salary/day", value=float(wf.salary_rookie_per_day), step=10.0)
            wf.salary_expert_per_day = st.number_input("Expert salary/day", value=float(wf.salary_expert_per_day), step=10.0)
            wf.overtime_cost_multiplier = st.number_input("Overtime cost multiplier", value=float(wf.overtime_cost_multiplier), step=0.05)

        st.markdown("### 🧱 Standard (Key)")
        std.accepted_orders = st.number_input("Std Accepted Orders", value=float(std.accepted_orders), step=1.0)
        std.accumulated_orders = st.number_input("Std Accumulated Orders", value=float(std.accumulated_orders), step=1.0)
        std.deliveries = st.number_input("Std Deliveries", value=float(std.deliveries), step=1.0)
        std.product_price = st.number_input("Std Product Price", value=float(std.product_price), step=0.01)
        std.market_price = st.number_input("Std Market Price", value=float(std.market_price), step=0.01)
        std.order_size_units = st.number_input("Std Order Size (units)", value=float(std.order_size_units), step=1.0)
        std.order_frequency_days = st.number_input("Std Order Frequency (days)", value=float(std.order_frequency_days), step=1.0)
        std.daily_demand_override = st.number_input("Std Daily Demand Override", value=float(std.daily_demand_override), step=1.0)

    with colC:
        st.markdown("### 🧱 Standard (Queues + Capacity)")
        std.queue1_level = st.number_input("Std Q1 Level", value=float(std.queue1_level), step=1.0)
        std.queue2_level = st.number_input("Std Q2 Level", value=float(std.queue2_level), step=1.0)
        std.queue3_level = st.number_input("Std Q3 Level", value=float(std.queue3_level), step=1.0)
        std.queue4_level = st.number_input("Std Q4 Level", value=float(std.queue4_level), step=1.0)
        std.queue5_level = st.number_input("Std Q5 Level", value=float(std.queue5_level), step=1.0)

        std.station1_machines = st.number_input("Std S1 Machines", value=float(std.station1_machines), step=1.0)
        std.station1_output = st.number_input("Std S1 Output", value=float(std.station1_output), step=0.01)
        std.initial_batch_output = st.number_input("Std Initial Batching Output", value=float(std.initial_batch_output), step=0.01)
        std.manual_processing_output = st.number_input("Std Manual Processing Output", value=float(std.manual_processing_output), step=0.01)
        std.final_batch_output = st.number_input("Std Final Batching Output", value=float(std.final_batch_output), step=0.01)
        std.effective_work_load_pct = st.number_input("Std Effective Work Load (%)", value=float(std.effective_work_load_pct), step=1.0)

        st.markdown("### 🧩 Custom (Key)")
        cus.daily_demand = st.number_input("Custom Daily Demand", value=float(cus.daily_demand), step=0.01)
        cus.accepted_orders = st.number_input("Custom Accepted Orders", value=float(cus.accepted_orders), step=1.0)
        cus.accumulated_orders = st.number_input("Custom Accumulated Orders", value=float(cus.accumulated_orders), step=1.0)
        cus.deliveries = st.number_input("Custom Deliveries", value=float(cus.deliveries), step=0.01)
        cus.average_lead_time = st.number_input("Custom Avg Lead Time", value=float(cus.average_lead_time), step=0.01)
        cus.actual_price = st.number_input("Custom Actual Price", value=float(cus.actual_price), step=0.01)

        st.markdown("### 🧩 Custom (Queues + Capacity)")
        cus.queue1_level = st.number_input("Custom Q1 Level", value=float(cus.queue1_level), step=1.0)
        cus.queue2_level_first_pass = st.number_input("Custom Q2 First Pass", value=float(cus.queue2_level_first_pass), step=1.0)
        cus.queue2_level_second_pass = st.number_input("Custom Q2 Second Pass", value=float(cus.queue2_level_second_pass), step=1.0)
        cus.queue3_level = st.number_input("Custom Q3 Level", value=float(cus.queue3_level), step=1.0)

        cus.station2_capacity_alloc_first_pass_pct = st.number_input(
            "Custom Station2 Allocation to First Pass (%)",
            value=float(cus.station2_capacity_alloc_first_pass_pct),
            step=1.0
        )

        cus.station1_output = st.number_input("Custom S1 Output", value=float(cus.station1_output), step=0.01)
        cus.station2_machines = st.number_input("Custom S2 Machines", value=float(cus.station2_machines), step=1.0)
        cus.station2_output_first_pass = st.number_input("Custom S2 Output First Pass", value=float(cus.station2_output_first_pass), step=0.01)
        cus.station3_machines = st.number_input("Custom S3 Machines", value=float(cus.station3_machines), step=1.0)
        cus.station3_output = st.number_input("Custom S3 Output", value=float(cus.station3_output), step=0.01)

        st.markdown("### 🏭 Machine Prices (Cheat)")
        mp.station1_buy = st.number_input("S1 buy", value=float(mp.station1_buy), step=1000.0)
        mp.station2_buy = st.number_input("S2 buy", value=float(mp.station2_buy), step=1000.0)
        mp.station3_buy = st.number_input("S3 buy", value=float(mp.station3_buy), step=1000.0)

# --------------------
# Tab 2: Dashboard snapshot (เหมือนตอนแรก + สวยขึ้น)
# --------------------
with tabs[2]:
    st.subheader("Dashboard (Snapshot)")

    inv: InventoryInputs = S["inventory"]
    fin: FinancialInputs = S["financial"]
    wf: WorkforceInputs = S["workforce"]
    std: StandardLineInputs = S["standard"]
    cus: CustomLineInputs = S["custom"]
    mp: MachinePrices = S["machine_prices"]

    status, checklist, metrics, caprec, reasons = build_status_and_checklist(inv, fin, wf, std, cus, mp)

    tag = f"(Imported Day {S['import_day']})" if S["import_day"] is not None else "(No import yet)"
    color = {"OK": "🟢", "WARNING": "🟠", "CRITICAL": "🔴"}[status]
    st.markdown(f"## {color} STATUS: **{status}** {tag}")

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Std daily demand", num(metrics.get("std_daily_demand", 0.0)))
    k2.metric("Parts/day (total)", num(metrics.get("parts_per_day", 0.0)))
    k3.metric("Inventory coverage (days)", num(metrics.get("coverage_days", 0.0)))
    k4.metric("Custom gap", num(metrics.get("custom_demand_gap", 0.0)))
    k5.metric("Std WIP proxy", num(metrics.get("std_wip_proxy", 0.0)))
    k6.metric("Cash / Debt", f"{money(fin.cash_on_hand)} / {money(fin.debt)}")

    with st.expander("📌 Why (เหตุผลหลักที่ทำให้สถานะเป็นแบบนี้)", expanded=True):
        for r in reasons:
            st.write(f"- {r}")

    st.markdown("### ✅ Checklist (Actionable)")
    if not checklist:
        st.success("ไม่พบปัญหาเด่นจาก snapshot (หรือ demand เป็น 0)")
    else:
        st.dataframe(pd.DataFrame(checklist), use_container_width=True)

    st.markdown("### 📌 Recommended Settings (copy into game)")
    rec_settings = {
        "Inventory: ROP (no safety)": float(metrics.get("recommended_rop", 0.0)),
        "Inventory: ROQ (EOQ, no safety)": float(metrics.get("recommended_roq", 0.0)),
        "Custom Station2: Allocation to First Pass (%)": float(metrics.get("suggested_station2_alloc_first_pass_pct", 50.0)),

        "Buy Machines: +Station1": int(caprec.get("add_station1", 0)),
        "Buy Machines: +Station2": int(caprec.get("add_station2", 0)),
        "Buy Machines: +Station3": int(caprec.get("add_station3", 0)),

        "Hire: Rookies": int(caprec.get("hire_rookies", 0)),
        "CapEx Estimate": float(caprec.get("capex_estimate", 0.0)),
        "Bottleneck (heuristic)": str(caprec.get("bottleneck_stage", "")),

        "Std Product Price": float(metrics.get("std_product_price", 0.0)),
        "Std Market Price": float(metrics.get("std_market_price", 0.0)),
        "Std EWL (%)": float(metrics.get("std_ewl", 0.0)),
        "Std Manual Output": float(metrics.get("std_mp_out", 0.0)),
    }
    st.json(rec_settings)

# --------------------
# Tab 3: Trends (Full-file)
# --------------------
with tabs[3]:
    st.subheader("Trends (Full-file)")
    if S["last_uploaded_bytes"] is None:
        st.info("อัปโหลดไฟล์ในแท็บ Import ก่อน")
    else:
        std_ts, cus_ts, inv_ts, fin_ts, wf_ts = make_timeseries_from_excel(S["last_uploaded_bytes"])

        if fin_ts is not None and not fin_ts.empty:
            fin_daily = finance_daily_delta(fin_ts)

            cols1 = [c for c in ["Cash_On_Hand", "Debt"] if c in fin_daily.columns]
            if cols1:
                st.markdown("#### 💵 Finance — Cash & Debt")
                st.line_chart(fin_daily.set_index("Day")[cols1], height=220)

            cols2 = [c for c in ["Sales_per_Day", "Costs_Proxy_per_Day", "Profit_Proxy_per_Day"] if c in fin_daily.columns]
            if cols2:
                st.markdown("#### 📊 Finance — Sales / Cost / Profit (Proxy) per Day")
                st.line_chart(fin_daily.set_index("Day")[cols2], height=220)

        if inv_ts is not None and not inv_ts.empty:
            inv_col = pick_col(inv_ts, COL["INV_LEVEL"])
            if inv_col:
                st.markdown("#### 📦 Inventory — Parts Level")
                st.line_chart(inv_ts.set_index("Day")[[inv_col]], height=200)

        if cus_ts is not None and not cus_ts.empty:
            dcol = pick_col(cus_ts, COL["CUS_DEMAND"])
            delcol = pick_col(cus_ts, COL["CUS_DELIV"])
            ltcol = pick_col(cus_ts, COL["CUS_LT"])
            q2_1 = pick_col(cus_ts, COL["CUS_Q2_1"])
            q2_2 = pick_col(cus_ts, COL["CUS_Q2_2"])

            cols = [c for c in [dcol, delcol] if c]
            if cols:
                st.markdown("#### 🧩 Custom — Demand vs Deliveries")
                st.line_chart(cus_ts.set_index("Day")[cols], height=220)

            cols = [c for c in [q2_1, q2_2] if c]
            if cols:
                st.markdown("#### 🧩 Custom — Q2 First Pass vs Second Pass")
                st.line_chart(cus_ts.set_index("Day")[cols], height=220)

            if ltcol:
                st.markdown("#### 🧩 Custom — Average Lead Time")
                st.line_chart(cus_ts.set_index("Day")[[ltcol]], height=200)

        if std_ts is not None and not std_ts.empty:
            s_acc = pick_col(std_ts, COL["STD_ACCEPT"])
            s_del = pick_col(std_ts, COL["STD_DELIV"])
            s_pp = pick_col(std_ts, COL["STD_PRICE"])
            s_mp = pick_col(std_ts, COL["STD_MKT"])
            s_ewl = pick_col(std_ts, COL["STD_EWL"])
            s_mp_out = pick_col(std_ts, COL["STD_MP_OUT"])

            cols = [c for c in [s_acc, s_del] if c]
            if cols:
                st.markdown("#### 🧱 Standard — Accepted vs Deliveries")
                st.line_chart(std_ts.set_index("Day")[cols], height=220)

            cols = [c for c in [s_pp, s_mp] if c]
            if cols:
                st.markdown("#### 🧱 Standard — Product Price vs Market Price")
                st.line_chart(std_ts.set_index("Day")[cols], height=200)

            cols = [c for c in [s_ewl, s_mp_out] if c]
            if cols:
                st.markdown("#### 🧱 Standard — Manual Processing (EWL & Output)")
                st.line_chart(std_ts.set_index("Day")[cols], height=220)

# --------------------
# Tab 4: Pricing (Full-file)
# --------------------
with tabs[4]:
    st.subheader("Pricing (Full-file) — Suggest Standard Product Price")
    if S["last_uploaded_bytes"] is None:
        st.info("อัปโหลดไฟล์ในแท็บ Import ก่อน")
    else:
        std_ts, cus_ts, inv_ts, fin_ts, wf_ts = make_timeseries_from_excel(S["last_uploaded_bytes"])
        if std_ts is None or std_ts.empty:
            st.warning("ไม่เจอชีท Standard หรือข้อมูลว่าง")
        else:
            std_price_df = build_standard_price_dataset(std_ts)
            if std_price_df.empty:
                st.warning("ไม่มีข้อมูล Standard Price/Demand ที่ใช้ทำ pricing (Price หรือ Accepted อาจเป็น 0 ทั้งหมด)")
            else:
                min_d = int(std_price_df["Day"].min())
                max_d = int(std_price_df["Day"].max())

                if min_d == max_d:
                    st.info(f"มีข้อมูล usable แค่วันเดียว: Day {min_d}")
                    r0, r1 = min_d, max_d
                else:
                    r0, r1 = st.slider(
                        "เลือกช่วงวันสำหรับ pricing analysis",
                        min_value=min_d,
                        max_value=max_d,
                        value=(min_d, max_d),
                        step=1,
                        key="fullfile_day_range",
                    )

                dfR = std_price_df[(std_price_df["Day"] >= r0) & (std_price_df["Day"] <= r1)].copy()
                sugg = suggest_standard_price(dfR)

                # capacity-aware warning
                cap_warn = False
                last_fill = float(sugg.get("last_fill_rate", 1.0))
                last_ewl = float(sugg.get("last_ewl", 0.0))
                last_backlog = float(sugg.get("last_backlog", 0.0))
                if last_ewl >= 95 or last_fill < 0.98 or last_backlog > 0:
                    cap_warn = True

                method_name = "Regression (Price↔Demand)" if float(sugg.get("method", 0)) == 1.0 else "Fallback (Market + Backlog/Fill)"
                st.markdown("### ✅ Suggested Standard Product Price")
                st.info(f"Suggested Price: **{money(sugg.get('suggested_price', 0.0))}** | Method: {method_name} | R²: {num(sugg.get('r2', 0.0))}")

                if cap_warn:
                    st.warning("⚠️ Capacity constraint detected (EWL สูง / fill rate ต่ำ / backlog > 0) → demand ที่เห็นอาจถูกจำกัดด้วยกำลังผลิต ทำให้ regression ‘หลอก’ ได้")

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Avg Fill Rate (proxy)", num(float(dfR["FillRateProxy"].mean())))
                c2.metric("Avg Backlog (proxy)", num(float(dfR["BacklogProxy"].mean())))
                c3.metric("Price unique values", str(int(dfR["Price"].nunique())))
                c4.metric("Days", str(int(len(dfR))))

                st.markdown("### 📈 Price vs DemandProxy (sorted-by-price view)")
                sc = dfR.sort_values("Price")[["Price", "DemandProxy"]].reset_index(drop=True)
                st.caption("หมายเหตุ: Streamlit ไม่มี scatter native → ใช้เส้นเรียงตามราคาเพื่อดูแนวโน้ม demand")
                st.line_chart(sc.set_index("Price")[["DemandProxy"]], height=220)

                st.markdown("### 🧱 Backlog & Fill Rate Over Time")
                st.line_chart(dfR.set_index("Day")[["BacklogProxy", "FillRateProxy"]], height=220)

                st.markdown("### 🔍 ช่วงที่ ‘พัง’ (Top 10 days)")
                bad = dfR.copy()
                bad["BadScore"] = bad["BacklogProxy"] + (1.0 - bad["FillRateProxy"]).clip(lower=0.0) * bad["DemandProxy"]
                st.dataframe(
                    bad.sort_values("BadScore", ascending=False)[
                        ["Day", "Price", "Market", "DemandProxy", "Deliveries", "BacklogProxy", "FillRateProxy", "BadScore", "EWL", "MP_Out"]
                    ].head(10),
                    use_container_width=True
                )

                with st.expander("Raw suggestion JSON"):
                    st.json(sugg)

# --------------------
# Tab 5: Why + What-if + Loan (ยัง conservative ไม่เดาเกมเกินข้อมูล)
# --------------------
with tabs[5]:
    st.subheader("Why + What-if + Loan (Conservative)")

    inv: InventoryInputs = S["inventory"]
    fin: FinancialInputs = S["financial"]
    wf: WorkforceInputs = S["workforce"]
    std: StandardLineInputs = S["standard"]
    cus: CustomLineInputs = S["custom"]
    mp: MachinePrices = S["machine_prices"]

    status, checklist, metrics, caprec, reasons = build_status_and_checklist(inv, fin, wf, std, cus, mp)

    st.markdown("### 📌 Why (สรุปเหตุผลจาก Snapshot)")
    for r in reasons:
        st.write(f"- {r}")

    st.markdown("### 🔮 What-if (ผลจากการปรับตาม Suggestion)")
    st.caption("คำนวณแบบ conservative: ให้ผล ‘ขั้นต่ำ’ โดยไม่เดาค่าใช้จ่ายเกมทั้งหมด")

    # Pricing what-if (only if file exists)
    if S["last_uploaded_bytes"] is None:
        st.info("ถ้าต้องการ what-if เรื่องราคา ให้ upload ไฟล์ก่อน (เพื่อ fit regression/fallback)")
        fit_info = {"method": 0.0}
        suggested_price = std.product_price
    else:
        std_ts, cus_ts, inv_ts, fin_ts, wf_ts = make_timeseries_from_excel(S["last_uploaded_bytes"])
        if std_ts is not None and not std_ts.empty:
            dfP = build_standard_price_dataset(std_ts)
            if not dfP.empty:
                suggested = suggest_standard_price(dfP)
                fit_info = suggested
                suggested_price = float(suggested.get("suggested_price", std.product_price))
            else:
                fit_info = {"method": 0.0}
                suggested_price = std.product_price
        else:
            fit_info = {"method": 0.0}
            suggested_price = std.product_price

    current_demand_proxy = std_daily_demand(std)
    current_deliveries = std.deliveries

    wi = conservative_what_if_std(
        current_price=float(std.product_price),
        suggested_price=float(suggested_price),
        fit_info=fit_info,
        current_demand_proxy=float(current_demand_proxy),
        current_deliveries=float(current_deliveries),
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Std Price (now → sugg)", f"{money(std.product_price)} → {money(suggested_price)}")
    c2.metric("Revenue/day (proxy) Δ", money(wi["revenue_delta"]))
    c3.metric("Sold/day (proxy) now→after", f"{num(wi['sold_before'])} → {num(wi['sold_after'])}")

    with st.expander("What-if details"):
        st.json(wi)

    st.markdown("### 🏦 Loan helper (Decision support)")
    st.caption("เกมกู้เงินได้ → เราเทียบ ‘กำไรเพิ่ม/วัน’ กับ ‘ต้นทุนเงินกู้/วัน’ แบบง่าย ๆ")

    loan_amount = st.number_input("Loan amount (ทดลองใส่)", value=0.0, step=1000.0)
    expected_profit_increase_per_day = st.number_input("Expected profit increase per day (คุณประเมินเอง)", value=0.0, step=100.0)

    # cost of loan per day (approx): interest + (commission spread across N days)
    apr = float(fin.normal_debt_apr)
    commission = float(fin.loan_commission_rate)
    commission_days = st.number_input("Spread commission over days", value=30, step=5)

    interest_per_day = loan_amount * (apr / 365.0)
    commission_per_day = (loan_amount * commission) / max(1, int(commission_days))
    loan_cost_per_day = interest_per_day + commission_per_day

    d1, d2, d3 = st.columns(3)
    d1.metric("Loan cost/day (approx)", money(loan_cost_per_day))
    d2.metric("Expected profit/day increase", money(expected_profit_increase_per_day))
    d3.metric("Net/day", money(expected_profit_increase_per_day - loan_cost_per_day))

    if expected_profit_increase_per_day > loan_cost_per_day and loan_amount > 0:
        st.success("เชิงตัวเลข: กู้ ‘อาจคุ้ม’ (กำไรเพิ่ม/วัน > ต้นทุนเงิน/วัน) — แต่ยังต้องดู risk ว่าทำได้จริงไหม")
    elif loan_amount > 0:
        st.warning("เชิงตัวเลข: กู้ ‘ยังไม่คุ้ม’ (กำไรเพิ่ม/วัน <= ต้นทุนเงิน/วัน) หรือยังไม่รู้กำไรเพิ่มจริง")

    st.markdown("### ✅ Action now (ถ้า ‘ไม่มีเงินซื้อเครื่องแล้ว’)")
    st.write("- โฟกัส **ลด backlog/lead time** ก่อนด้วยการปรับ allocation (ฟรี) และการตั้ง ROP/ROQ ให้ลด stockout")
    st.write("- ถ้าจะกู้: ให้กู้เพื่อแก้คอขวดที่ให้ **กำไร/วันเพิ่มจริง** (ไม่ใช่ซื้อทุกอย่าง)")
    st.write("- ถ้า Standard EWL สูงมาก: price-fit อาจไม่ช่วยยอดขาย เพราะขายติด capacity → ต้องแก้คอขวดก่อน")


# Footer
st.caption("")
