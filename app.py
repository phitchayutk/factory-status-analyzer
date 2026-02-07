# ============================================================
# Factory Status Analyzer (Game Excel Export) — CLEAN COPY-PASTE ✅
# ✅ Robust import (alias columns, pick best day)
# ✅ Fix: COL dict, dataclass duplicates, upload bytes stored
# ✅ Add: Standard Product Price + Market Price + Deliveries
# ✅ Add: Dashboard Trend Graphs (Finance / Inventory / Lines)
# ✅ ROQ/ROP: ROQ = EOQ (no safety), ROP = D*LeadTime (no safety)
# ============================================================

import io
import math
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


def read_sheet(xl: pd.ExcelFile, *names: str) -> Optional[pd.DataFrame]:
    """Read first matching sheet name; normalize column names (strip)."""
    for n in names:
        if n in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=n)
            df.columns = [str(c).strip() for c in df.columns]
            return df
    return None


def pick_col(df: Optional[pd.DataFrame], aliases: List[str]) -> Optional[str]:
    """Find a column name by aliases (exact first, then case-insensitive)."""
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


def getv(row: pd.Series, df: pd.DataFrame, aliases: List[str], default=0.0) -> float:
    """Get value from row using alias-matched column."""
    col = pick_col(df, aliases)
    if not col:
        return float(default)
    return to_float(row.get(col, default), default)


def excel_file_from_bytes(xbytes: bytes) -> pd.ExcelFile:
    """Always safe for pandas: wrap bytes as BytesIO."""
    return pd.ExcelFile(io.BytesIO(xbytes))


def safe_day_series(df: Optional[pd.DataFrame]) -> pd.Series:
    """Return clean int day series from df or empty."""
    if df is None:
        return pd.Series([], dtype=int)
    dcol = pick_col(df, COL["DAY"])
    if not dcol:
        return pd.Series([], dtype=int)
    vals = pd.to_numeric(df[dcol], errors="coerce").dropna()
    if vals.empty:
        return pd.Series([], dtype=int)
    return vals.astype(int)


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
# Column aliases (robust import)
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

    "STD_Q1": ["Standard Queue 1-Level", "Standard Q1-Level", "Queue 1-Level", "Queue1 Level"],
    "STD_Q2": ["Standard Queue 2-Level", "Standard Q2-Level", "Queue 2-Level", "Queue2 Level"],
    "STD_Q3": ["Standard Queue 3-Level", "Standard Q3-Level", "Queue 3-Level", "Queue3 Level"],
    "STD_Q4": ["Standard Queue 4-Level", "Standard Q4-Level", "Queue 4-Level", "Queue4 Level"],
    "STD_Q5": ["Standard Queue 5-Level", "Standard Q5-Level", "Queue 5-Level", "Queue5 Level"],

    "STD_MACHINES": ["Standard Station 1-Number of Machines", "Station 1-Number of Machines", "Number of Machines"],
    "STD_EWL": ["Standard Manual Processing-Effective Work Load (%)", "Effective Work Load (%)", "Effective Work Load"],
    "STD_S1_OUT": ["Standard Station 1-Output", "Station 1-Output", "Output"],
    "STD_IB_OUT": ["Standard Initial Batching-Output", "Initial Batching-Output"],
    "STD_MP_OUT": ["Standard Manual Processing-Output", "Manual Processing-Output"],
    "STD_FB_OUT": ["Standard Final Batching-Output", "Final Batching-Output"],

    "CUS_DEMAND": ["Custom Orders-Demand", "Daily Demand", "Demand"],
    "CUS_ACCEPT": ["Custom Orders-Accepted Orders", "Custom Accepted Orders", "Accepted Orders"],
    "CUS_ACCUM": ["Custom Orders-Accumulated Orders", "Custom Accumulated Orders", "Accumulated Orders"],
    "CUS_DELIV": ["Custom Deliveries-Deliveries", "Deliveries", "Deliveries Out"],
    "CUS_LT": ["Custom Deliveries-Average Lead Time", "Average Lead Time", "Lead Time"],
    "CUS_PRICE": ["Custom Deliveries-Actual Price", "Actual Price"],

    "CUS_Q1": ["Custom Queue 1-Level", "Queue 1-Level", "Level"],
    "CUS_Q2_1": ["Custom Queue 2-Level First Pass", "Level First Pass", "Q2 First Pass"],
    "CUS_Q2_2": ["Custom Queue 2-Level Second Pass", "Level Second Pass", "Q2 Second Pass"],
    "CUS_Q3": ["Custom Queue 3-Level", "Queue 3-Level", "Level"],

    "CUS_S1_OUT": ["Custom Station 1-Output", "Output"],
    "CUS_S2_OUT_1": ["Custom Station 2-Output First Pass", "Output First Pass"],
    "CUS_S2_MACH": ["Custom Station 2-Number of Machines", "Number of Machines"],
    "CUS_S3_OUT": ["Custom Station 3-Output", "Output"],
    "CUS_S3_MACH": ["Custom Station 3-Number of Machines", "Number of Machines"],

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
# Cheat defaults
# ============================================================
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
# Import utilities
# ============================================================
def pick_best_day(std_df, cus_df, fin_df) -> int:
    """Pick latest day that has real activity."""
    all_days = pd.concat(
        [safe_day_series(std_df), safe_day_series(cus_df), safe_day_series(fin_df)],
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

        if std_df is not None:
            dcol = pick_col(std_df, COL["DAY"])
            if dcol:
                r = std_df.loc[pd.to_numeric(std_df[dcol], errors="coerce").fillna(-1).astype(int) == d]
                if not r.empty:
                    row = r.iloc[0]
                    s += abs(getv(row, std_df, COL["STD_ACCEPT"]))
                    s += abs(getv(row, std_df, COL["STD_DELIV"]))

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

    # Standard
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
        std.effective_work_load_pct = getv(std_r, std_df, COL["STD_EWL"], 0.0)

        std.station1_output = getv(std_r, std_df, COL["STD_S1_OUT"], 0.0)
        std.initial_batch_output = getv(std_r, std_df, COL["STD_IB_OUT"], 0.0)
        std.manual_processing_output = getv(std_r, std_df, COL["STD_MP_OUT"], 0.0)
        std.final_batch_output = getv(std_r, std_df, COL["STD_FB_OUT"], 0.0)

    std.parts_per_unit = CHEAT_DEFAULTS["std_parts_per_unit"]

    # Custom
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
# Engine: Inventory / Flow / Recommendations
# ============================================================
def recommend_reorder_policy(inv: InventoryInputs, std: StandardLineInputs, cus: CustomLineInputs) -> Dict[str, float]:
    std_d = std_daily_demand(std)
    std_parts = std_d * std.parts_per_unit
    cus_parts = cus.daily_demand * cus.parts_per_unit
    D = std_parts + cus_parts  # parts/day

    h = 1.0  # holding cost per part/day (game)
    S = inv.order_fee

    rop = D * inv.lead_time_days
    roq = math.sqrt((2.0 * D * S) / h) if D > 0 else 0.0

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

    if bottleneck == "S1":
        base = max(1.0, s1_total)
    elif bottleneck == "S2":
        base = max(1.0, s2_total)
    else:
        base = max(1.0, s3_total)

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
) -> Tuple[str, List[Dict[str, str]], Dict[str, float], Dict[str, float]]:

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
        "std_product_price": std.product_price,
        "std_market_price": std.market_price,
    }

    checklist: List[Dict[str, str]] = []
    severity = 0

    if rec_inv["parts_per_day"] > 0 and inv_diag["coverage_days"] < inv.lead_time_days:
        severity += 2
        checklist.append({
            "area": "Inventory",
            "finding": f"สต็อกคุ้มครองได้ ~{num(inv_diag['coverage_days'])} วัน (< lead time {num(inv.lead_time_days)}d)",
            "action": "ตั้ง ROP ให้พอช่วง lead time (ไม่กัน safety)",
            "recommended_value": f"ROP≈{num(rec_inv['recommended_rop'])} | ROQ≈{num(rec_inv['recommended_roq'])}",
        })

    if metrics["std_daily_demand"] > 0 and std_diag["std_demand_gap"] > 0:
        severity += 1
        checklist.append({
            "area": "Standard Line",
            "finding": f"Standard ส่งมอบไม่ทัน demand (gap {num(std_diag['std_demand_gap'])}/day)",
            "action": "ดูคิว+คอขวด (Initial/Manual/Final) แล้วเพิ่ม capacity จุดตัน",
            "recommended_value": f"WIP≈{num(std_diag['std_wip_proxy'])}",
        })

    if cus.daily_demand > 0 and cus_diag["custom_demand_gap"] > 0:
        severity += 2
        checklist.append({
            "area": "Custom Line",
            "finding": f"Custom ส่งมอบไม่ทัน demand (gap {num(cus_diag['custom_demand_gap'])}/day)",
            "action": "แก้คอขวด + ปรับ Station2 allocation",
            "recommended_value": f"Station2 First Pass≈{num(alloc['suggested_alloc_first_pass_pct'])}%",
        })

    if cus.average_lead_time >= 10:
        severity += 1
        checklist.append({
            "area": "Custom Lead Time",
            "finding": f"Average Lead Time สูง ({num(cus.average_lead_time)} days) = WIP/คิวค้าง",
            "action": "ลดคิวที่พองที่สุดก่อน (Q2 second pass มักทำให้ lead time พุ่ง)",
            "recommended_value": f"Q2(first)={num(cus.queue2_level_first_pass)} | Q2(second)={num(cus.queue2_level_second_pass)}",
        })

    if caprec["custom_gap"] > 0:
        severity += 1
        checklist.append({
            "area": "Capacity + Workforce",
            "finding": f"คอขวดโดยประมาณ: {caprec['bottleneck_stage']} | gap≈{num(caprec['custom_gap'])}/day",
            "action": "เพิ่มเครื่อง/คนให้ตรงคอขวด (Hire ได้แค่ Rookie)",
            "recommended_value": (
                f"+S1:{int(caprec['add_station1'])}, +S2:{int(caprec['add_station2'])}, +S3:{int(caprec['add_station3'])} | "
                f"Hire Rookie:{int(caprec['hire_rookies'])} | "
                f"CapEx≈{money(caprec['capex_estimate'])}"
            ),
        })

    status = "CRITICAL" if severity >= 5 else ("WARNING" if severity >= 2 else "OK")
    return status, checklist, metrics, caprec


# ============================================================
# Timeseries + Profit Proxy Trends
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
        out = out[out["Day"] >= 0]
        return out

    return tuple(map(norm_day, [std_df, cus_df, inv_df, fin_df]))


def _series_from_to_date(fin_df: pd.DataFrame, aliases: List[str]) -> pd.Series:
    c = pick_col(fin_df, aliases)
    if not c:
        return pd.Series([0.0] * len(fin_df), index=fin_df.index)
    return pd.to_numeric(fin_df[c], errors="coerce").fillna(0.0).astype(float)


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
        out["Cash_On_Hand"] = pd.to_numeric(df[cash_col], errors="coerce").fillna(0.0)
    if debt_col:
        out["Debt"] = pd.to_numeric(df[debt_col], errors="coerce").fillna(0.0)

    return out


# ============================================================
# Streamlit UI
# ============================================================
st.set_page_config(page_title="Factory Status Analyzer", layout="wide")
st.title("🏭 Factory Status Analyzer")

tabs = st.tabs(["0) Import Excel", "1) Input", "2) Dashboard", "3) Checklist + Recommendations"])

# session state defaults
if "inventory" not in st.session_state:
    st.session_state.inventory = InventoryInputs()
if "financial" not in st.session_state:
    st.session_state.financial = FinancialInputs()
if "workforce" not in st.session_state:
    st.session_state.workforce = WorkforceInputs()
if "standard" not in st.session_state:
    st.session_state.standard = StandardLineInputs()
if "custom" not in st.session_state:
    st.session_state.custom = CustomLineInputs()
if "machine_prices" not in st.session_state:
    st.session_state.machine_prices = MachinePrices()
if "import_day" not in st.session_state:
    st.session_state.import_day = None
if "last_uploaded_bytes" not in st.session_state:
    st.session_state.last_uploaded_bytes = None


with tabs[0]:
    st.subheader("Import Excel (Export จากเกม)")
    up = st.file_uploader("อัปโหลดไฟล์ .xlsx ที่ Export จากเกม", type=["xlsx"])

    if up is not None:
        xbytes = up.getvalue()
        st.session_state.last_uploaded_bytes = xbytes  # ✅ สำคัญ: ทำให้ Dashboard Graph ใช้ได้

        tmp_xl = excel_file_from_bytes(xbytes)
        std_df = read_sheet(tmp_xl, "Standard")
        cus_df = read_sheet(tmp_xl, "Custom")
        fin_df = read_sheet(tmp_xl, "Finance", "Financial")

        # --- compute max_day robust ---
        all_days = pd.concat(
            [safe_day_series(std_df), safe_day_series(cus_df), safe_day_series(fin_df)],
            ignore_index=True,
        )
        max_day = int(all_days.max()) if not all_days.empty else 0

        suggested = pick_best_day(std_df, cus_df, fin_df)
        st.caption(f"ระบบแนะนำเลือก Day = {suggested} (วันล่าสุดที่มีข้อมูลจริง)")

        day = st.number_input(
            "เลือก Day ที่ต้องการวิเคราะห์",
            min_value=0,
            max_value=max_day,
            value=int(suggested),
            step=1,
        )

        if st.button("✅ Load day นี้เข้าแบบฟอร์ม"):
            loaded = load_inputs_from_excel(xbytes, day=int(day))
            st.session_state.inventory = loaded["inventory"]
            st.session_state.financial = loaded["financial"]
            st.session_state.workforce = loaded["workforce"]
            st.session_state.standard = loaded["standard"]
            st.session_state.custom = loaded["custom"]
            st.session_state.machine_prices = loaded["machine_prices"]
            st.session_state.import_day = loaded["day"]
            st.success(f"โหลดข้อมูลจาก Excel สำเร็จ (Day {loaded['day']}) — ไปแท็บ Dashboard ได้เลย")


with tabs[1]:
    st.subheader("Input (ยังแก้ได้หลัง import)")

    inv = st.session_state.inventory
    fin = st.session_state.financial
    work = st.session_state.workforce
    std = st.session_state.standard
    cus = st.session_state.custom
    mp = st.session_state.machine_prices

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("### 📦 Inventory")
        inv.inventory_level_parts = st.number_input("Inventory Level (parts)", value=float(inv.inventory_level_parts), step=1.0)
        inv.cost_per_part = st.number_input("Cost Per Part", value=float(inv.cost_per_part), step=1.0)
        inv.order_fee = st.number_input("Order Fee", value=float(inv.order_fee), step=100.0)
        inv.lead_time_days = st.number_input("Lead Time (days)", value=float(inv.lead_time_days), step=1.0)
        inv.reorder_point = st.number_input("Current ROP (optional)", value=float(inv.reorder_point), step=1.0)
        inv.reorder_quantity = st.number_input("Current ROQ (optional)", value=float(inv.reorder_quantity), step=1.0)

        st.markdown("### 💰 Finance")
        fin.cash_on_hand = st.number_input("Cash On Hand", value=float(fin.cash_on_hand), step=1000.0)
        fin.debt = st.number_input("Debt", value=float(fin.debt), step=1000.0)

    with c2:
        st.markdown("### 👷 Workforce")
        work.rookies = st.number_input("Rookies", value=float(work.rookies), step=1.0)
        work.experts = st.number_input("Experts", value=float(work.experts), step=1.0)
        st.caption("Cheat: Rookies 80/day, Experts 150/day, Rookie productivity 40%, 15 days → Expert")

        st.markdown("### 🧱 Standard (Key)")
        std.accepted_orders = st.number_input("Std Accepted Orders", value=float(std.accepted_orders), step=1.0)
        std.deliveries = st.number_input("Std Deliveries", value=float(std.deliveries), step=1.0)
        std.daily_demand_override = st.number_input("Std Daily Demand Override", value=float(std.daily_demand_override), step=1.0)

        std.product_price = st.number_input("Std Product Price", value=float(std.product_price), step=0.01)
        std.market_price = st.number_input("Std Market Price", value=float(std.market_price), step=0.01)
        std.order_size_units = st.number_input("Std Order Size (units)", value=float(std.order_size_units), step=1.0)
        std.order_frequency_days = st.number_input("Std Order Frequency (days)", value=float(std.order_frequency_days), step=1.0)

    with c3:
        st.markdown("### 🧩 Custom (Key)")
        cus.daily_demand = st.number_input("Custom Daily Demand", value=float(cus.daily_demand), step=0.01)
        cus.deliveries = st.number_input("Custom Deliveries", value=float(cus.deliveries), step=0.01)
        cus.average_lead_time = st.number_input("Custom Avg Lead Time", value=float(cus.average_lead_time), step=0.01)

        cus.queue2_level_first_pass = st.number_input("Q2 First Pass", value=float(cus.queue2_level_first_pass), step=1.0)
        cus.queue2_level_second_pass = st.number_input("Q2 Second Pass", value=float(cus.queue2_level_second_pass), step=1.0)

        cus.station1_output = st.number_input("Custom Station1 Output", value=float(cus.station1_output), step=0.01)
        cus.station2_machines = st.number_input("Custom Station2 Machines", value=float(cus.station2_machines), step=1.0)
        cus.station2_output_first_pass = st.number_input("Custom Station2 Output First Pass", value=float(cus.station2_output_first_pass), step=0.01)
        cus.station3_machines = st.number_input("Custom Station3 Machines", value=float(cus.station3_machines), step=1.0)
        cus.station3_output = st.number_input("Custom Station3 Output", value=float(cus.station3_output), step=0.01)

        st.markdown("### 🏭 Machine Prices (Cheat)")
        mp.station1_buy = st.number_input("S1 buy", value=float(mp.station1_buy), step=1000.0)
        mp.station2_buy = st.number_input("S2 buy", value=float(mp.station2_buy), step=1000.0)
        mp.station3_buy = st.number_input("S3 buy", value=float(mp.station3_buy), step=1000.0)


with tabs[2]:
    st.subheader("Dashboard")

    inv = st.session_state.inventory
    fin = st.session_state.financial
    work = st.session_state.workforce
    std = st.session_state.standard
    cus = st.session_state.custom
    mp = st.session_state.machine_prices

    status, checklist, metrics, caprec = build_status_and_checklist(inv, fin, work, std, cus, mp)

    tag = f"(Imported Day {st.session_state.import_day})" if st.session_state.import_day is not None else ""
    color = {"OK": "🟢", "WARNING": "🟠", "CRITICAL": "🔴"}[status]
    st.markdown(f"## {color} STATUS: **{status}** {tag}")

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Std daily demand", num(metrics.get("std_daily_demand", 0.0)))
    k2.metric("Parts/day (total)", num(metrics.get("parts_per_day", 0.0)))
    k3.metric("Inventory coverage (days)", num(metrics.get("coverage_days", 0.0)))
    k4.metric("Custom gap", num(metrics.get("custom_demand_gap", 0.0)))
    k5.metric("Std Price / Market", f"{num(metrics.get('std_product_price',0.0))} / {num(metrics.get('std_market_price',0.0))}")

    st.markdown("### Metrics Table")
    df = pd.DataFrame([metrics]).T.reset_index()
    df.columns = ["metric", "value"]
    st.dataframe(df, use_container_width=True)

    st.markdown("### 📈 Trends (จากทั้งไฟล์)")

    if st.session_state.last_uploaded_bytes is None:
        st.info("อัปโหลดไฟล์ในแท็บ Import ก่อน แล้วกราฟจะขึ้นอัตโนมัติ")
    else:
        std_ts, cus_ts, inv_ts, fin_ts = make_timeseries_from_excel(
            st.session_state.last_uploaded_bytes
        )

        # ==========================
        # FINANCE
        # ==========================
        if fin_ts is not None:
            fin_daily = finance_daily_delta(fin_ts)

            cols1 = [c for c in ["Cash_On_Hand", "Debt"] if c in fin_daily.columns]
            if cols1:
                st.subheader("💰 Finance — Cash & Debt")
                st.line_chart(fin_daily.set_index("Day")[cols1], height=220)

            cols2 = [
                c for c in
                ["Sales_per_Day", "Costs_Proxy_per_Day", "Profit_Proxy_per_Day"]
                if c in fin_daily.columns
            ]
            if cols2:
                st.subheader("📊 Finance — Daily Profit Proxy")
                st.line_chart(fin_daily.set_index("Day")[cols2], height=220)

        # ==========================
        # INVENTORY
        # ==========================
        if inv_ts is not None:
            inv_col = pick_col(inv_ts, COL["INV_LEVEL"])
            if inv_col:
                st.subheader("📦 Inventory Level Over Time")
                st.line_chart(inv_ts.set_index("Day")[[inv_col]], height=200)

        # ==========================
        # CUSTOM LINE
        # ==========================
        if cus_ts is not None:
            dcol = pick_col(cus_ts, COL["CUS_DEMAND"])
            delcol = pick_col(cus_ts, COL["CUS_DELIV"])
            ltcol = pick_col(cus_ts, COL["CUS_LT"])
            q2_1 = pick_col(cus_ts, COL["CUS_Q2_1"])
            q2_2 = pick_col(cus_ts, COL["CUS_Q2_2"])

            cols = [c for c in [dcol, delcol] if c]
            if cols:
                st.subheader("🧩 Custom Line — Demand vs Deliveries")
                st.line_chart(cus_ts.set_index("Day")[cols], height=220)

            cols = [c for c in [q2_1, q2_2] if c]
            if cols:
                st.subheader("🧵 Custom Line — Queue 2 (First vs Second Pass)")
                st.line_chart(cus_ts.set_index("Day")[cols], height=220)

            if ltcol:
                st.subheader("⏱️ Custom Line — Average Lead Time")
                st.line_chart(cus_ts.set_index("Day")[[ltcol]], height=200)

        # ==========================
        # STANDARD LINE
        # ==========================
        if std_ts is not None:
            s_acc = pick_col(std_ts, COL["STD_ACCEPT"])
            s_del = pick_col(std_ts, COL["STD_DELIV"])
            s_pp = pick_col(std_ts, COL["STD_PRICE"])
            s_mp = pick_col(std_ts, COL["STD_MKT"])

            cols = [c for c in [s_acc, s_del] if c]
            if cols:
                st.subheader("🏭 Standard Line — Accepted vs Delivered Orders")
                st.line_chart(std_ts.set_index("Day")[cols], height=220)

            cols = [c for c in [s_pp, s_mp] if c]
            if cols:
                st.subheader("💵 Standard Line — Product Price vs Market Price")
                st.line_chart(std_ts.set_index("Day")[cols], height=200)


with tabs[3]:
    st.subheader("Checklist + Recommendations")

    inv = st.session_state.inventory
    fin = st.session_state.financial
    work = st.session_state.workforce
    std = st.session_state.standard
    cus = st.session_state.custom
    mp = st.session_state.machine_prices

    status, checklist, metrics, caprec = build_status_and_checklist(inv, fin, work, std, cus, mp)

    if not checklist:
        st.success("ไม่พบปัญหาเด่นจากค่าที่มี — หรือ demand เป็น 0")
    else:
        st.dataframe(pd.DataFrame(checklist), use_container_width=True)

    st.markdown("### Recommended Settings (copy these into game)")
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
    }
    st.json(rec_settings)
