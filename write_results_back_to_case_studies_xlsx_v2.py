# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import math
import shutil
import pandas as pd

# ============================================================
# USER PATH
# ============================================================

ROOT = Path(r"C:\Users\ED281169\Documents\EnergyScope\EnergyScopeFR_Biomass_supply_curves")
XLSX_PATH = ROOT / "Case studies SNBC3 + ENSPRESO assumptions.xlsx"
CASE_STUDIES_DIR = ROOT / "case_studies"
SHEET_NAME = "Scenarios"

MAKE_BACKUP = True

# ============================================================
# UNIT CONVERSIONS
# ============================================================

ENERGY_DIV = 1000.0       # GWh -> TWh
CO2_DIV = 1000.0          # ktCO2 -> MtCO2
DUAL_ENERGY_MULT = 1000.0 # M€/GWh -> €/MWh
DUAL_CO2_MULT = 1000.0    # M€/ktCO2 -> €/tCO2

# ============================================================
# GROUPS
# ============================================================

BIOMASS_GROUPS = {
    "Agri residues": [
        "APPLES", "CEREALSTRAW", "MAIZESTRAW", "OLIVE_PITS",
        "OSR", "RICESTRAW", "VINEYARDS"
    ],
    "Forest residues": [
        "CP_RES", "CP_RW", "FUELWOOD_RES", "FUELWOODRW",
        "LANDSCAPECARE", "OTHERSECRESID", "SAWDUST"
    ],
    "MSW": ["MSW"],
    "Grassy energy crops": ["MISCANTHUS", "SWITCHGRASS"],
    "Woody energy crops": ["WILLOW", "POPLAR"],
}

# ============================================================
# HELPERS
# ============================================================

def strip_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    keep_cols = [c for c in df.columns if not str(c).startswith("Unnamed")]
    return df[keep_cols]


def read_txt(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=r"\t+", engine="python")
    return strip_df(df)


def read_named_table(path: Path, name_col: str = "Name") -> pd.DataFrame:
    df = read_txt(path)
    if name_col not in df.columns:
        raise ValueError(f"'{name_col}' column not found in {path.name}. Found: {list(df.columns[:10])}")
    df[name_col] = df[name_col].astype(str).str.strip()
    df = df.set_index(name_col)
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def read_year_balance(path: Path) -> pd.DataFrame:
    df = read_txt(path)
    if "Tech" not in df.columns:
        raise ValueError(f"'Tech' column not found in {path.name}. Found: {list(df.columns[:10])}")
    df["Tech"] = df["Tech"].astype(str).str.strip()
    df = df.set_index("Tech")
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def val(df: pd.DataFrame, idx: str, col: str, default: float = 0.0) -> float:
    if idx in df.index and col in df.columns:
        v = df.loc[idx, col]
        if isinstance(v, pd.Series):
            v = v.iloc[0]
        if pd.isna(v):
            return default
        return float(v)
    return default


def abs_val(df: pd.DataFrame, idx: str, col: str, default: float = 0.0) -> float:
    return abs(val(df, idx, col, default))


def sum_index_values(df: pd.DataFrame, names: list[str], col: str) -> float:
    return sum(val(df, n, col, 0.0) for n in names)


def mean_abs_dual(df: pd.DataFrame, names: list[str], dual_col: str = "Size_limit_max") -> float:
    vals = []
    for n in names:
        if n in df.index and dual_col in df.columns:
            v = df.loc[n, dual_col]
            if isinstance(v, pd.Series):
                v = v.iloc[0]
            if pd.notna(v):
                vals.append(abs(float(v)))
    return math.nan if not vals else sum(vals) / len(vals)

def read_general_dual(path: Path) -> dict:
    vals = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 2:
                key = parts[0].strip()
                try:
                    vals[key] = float(parts[1])
                except ValueError:
                    pass
    return vals

def clean_yes_no(v):
    if pd.isna(v):
        return ""
    return str(v).strip()


def clean_str(v):
    if pd.isna(v):
        return ""
    s = str(v).strip()
    if s.endswith(".0"):
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    return s


def build_case_name(row: pd.Series) -> str:
    """
    Rebuild scenario name if column Name is empty.
    Mirrors the naming logic used in the batch.
    """
    run_id = row.get("run_id", "")
    try:
        run_id = int(run_id)
    except Exception:
        run_id = clean_str(run_id)

    return (
        f"E-biofuel{clean_yes_no(row.get('E-biofuel_option'))}"
        f"_ReFuel_blinding{clean_yes_no(row.get('ReFuel_blinding_mandates'))}"
        f"_ReFuel_sustainability{clean_yes_no(row.get('ReFuel_sustainability_criteria'))}"
        f"_ENSPRESO{clean_str(row.get('ENSPRESO_scenario'))}"
        f"_Energy_crops{clean_str(row.get('Energy_crops_impacts')) or clean_str(row.get('Energy_crops_impacts '))}"
        f"_Geological{clean_str(row.get('Geological_sequestration'))}"
        f"_SNBC_3_Updated_{run_id}"
    )


def get_case_name(row: pd.Series) -> str:
    """
    Use Name if present and not empty; otherwise rebuild it.
    """
    if "Name" in row and pd.notna(row["Name"]) and str(row["Name"]).strip() not in ("", "None", "nan"):
        return str(row["Name"]).strip()
    return build_case_name(row)

def sum_positive_column(df: pd.DataFrame, col: str) -> float:
    if col not in df.columns:
        return 0.0
    s = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return float(s[s > 0].sum())

# ============================================================
# EXTRACTION
# ============================================================

def extract_metrics(case_name: str) -> dict:
    outdir = CASE_STUDIES_DIR / case_name / "output"
    if not outdir.exists():
        return {
            "Extraction status": "missing_output",
            "Extraction error": str(outdir),
        }

    try:
        cost = read_named_table(outdir / "cost_breakdown.txt")
        yb = read_year_balance(outdir / "year_balance.txt")
        rb = read_named_table(outdir / "resources_breakdown.txt")
        rd = read_named_table(outdir / "resources_dual.txt")
        general_dual = read_general_dual(outdir / "general_dual.txt")

        total_cost = (
            cost["C_inv"].fillna(0).sum()
            + cost["C_maint"].fillna(0).sum()
            + cost["C_op"].fillna(0).sum()
        )

        # CO2 convention fixed by user:
        # CO2 capturé = CO2_CAPTURED
        # CCU = CO2_TO_FT + CO2_TO_METHANE + CO2_TO_METHANOL
        # CCS = CO2_CAPTURED - CCU
        
        ccu_ft = abs_val(yb, "CO2_TO_FT", "CO2_CAPTURED")
        ccu_methane = abs_val(yb, "CO2_TO_METHANE", "CO2_CAPTURED")
        ccu_methanol = abs_val(yb, "CO2_TO_METHANOL", "CO2_CAPTURED")
        ccs = abs_val(yb, "SEQUESTRATION", "CO2_CAPTURED")
        dac = abs_val(yb, "DAC_LT", "CO2_ATMOSPHERE")
        point_source_capture = abs_val(yb, "INDUSTRY_CCS", "CO2_CENTRALISED")*0.9 #assuming 90% capture ratio
        CO2_decentralised_atm = abs_val(yb, "CO2_EMISSIONS", "CO2_DECENTRALISED") 
        CO2_centralised_atm = abs_val(yb, "CO2_ATMOSPHERE", "CO2_CENTRALISED")
        Other_GHG_atm = abs_val(yb, "GHG_EMISSIONS", "OTHER_GHG")
        CO2_point_source_CC = abs_val(yb, "INDUSTRY_CCS", "CO2_CENTRALISED")
        total_electricity_production = sum_positive_column(yb, "ELECTRICITY")



        carbon_dual = general_dual.get("Other_GHGs_emission_dual", math.nan)
        carbon_dual = abs(carbon_dual) * DUAL_CO2_MULT if pd.notna(carbon_dual) else math.nan

        return {
            "Extraction status": "ok",
            "Extraction error": "",

            # 1) Total cost
            "Total cost (M€)": total_cost,

            # 2) FT
            "WOOD_TO_FT (TWh)": abs_val(yb, "WOOD_TO_FT", "FT_FUEL") / ENERGY_DIV,
            "E_WOOD_TO_FT (TWh)": abs_val(yb, "E_WOOD_TO_FT", "FT_FUEL") / ENERGY_DIV,
            "CO2_TO_FT (TWh)": abs_val(yb, "CO2_TO_FT", "FT_FUEL") / ENERGY_DIV,

            # 3) Methanol
            "WOOD_TO_METHANOL (TWh)": abs_val(yb, "WOOD_TO_METHANOL", "METHANOL") / ENERGY_DIV,
            "E_WOOD_TO_METHANOL (TWh)": abs_val(yb, "E_WOOD_TO_METHANOL", "METHANOL") / ENERGY_DIV,
            "CO2_TO_METHANOL (TWh)": abs_val(yb, "CO2_TO_METHANOL", "METHANOL") / ENERGY_DIV,

            # 4) SNG
            "WOOD_TO_SNG (TWh)": abs_val(yb, "WOOD_TO_METHANE", "GAS") / ENERGY_DIV,
            "E_WOOD_TO_SNG (TWh)": abs_val(yb, "E_WOOD_TO_METHANE", "GAS") / ENERGY_DIV,
            "CO2_TO_SNG (TWh)": abs_val(yb, "CO2_TO_METHANE", "GAS") / ENERGY_DIV,

            # 4bis) HVC Biomass needs (TWH)
            "HVC Biomass needs (TWh)": abs_val(yb, "BIOMASS_TO_HVC", "WOOD") / ENERGY_DIV,

            # 4ter) Total electricity production
            "Total electricity production (TWh)": total_electricity_production / ENERGY_DIV,

            # 5) CCU / CCS
            "CCU CO2_TO_FT (MtCO2eq)": ccu_ft / CO2_DIV,
            "CCU CO2_TO_METHANE (MtCO2eq)": ccu_methane / CO2_DIV,
            "CCU CO2_TO_METHANOL (MtCO2eq)": ccu_methanol / CO2_DIV,
            "CCS SEQUESTRATION (MtCO2eq)": ccs / CO2_DIV,
            "DAC (MtCO2eq)": dac / CO2_DIV,
            "POINT SOURCE CAPTURE (MtCO2eq)": point_source_capture / CO2_DIV,
            "CO2_DECENTRALISED ATMOSPHERE (MtCO2eq)": CO2_decentralised_atm / CO2_DIV,
            "CO2_CENTRALISED ATMOSPHERE (MtCO2eq)": CO2_centralised_atm / CO2_DIV,
            "OTHER GHG ATMOSPHERE (MtCO2eq)": Other_GHG_atm / CO2_DIV,
            "CO2 POINT SOURCE CAPTURE (MtCO2eq)": CO2_point_source_CC / CO2_DIV,
           

            # 6) Biomass groups
            "Agri residues used (TWh)": sum_index_values(rb, BIOMASS_GROUPS["Agri residues"], "Used") / ENERGY_DIV,
            "Forest residues used (TWh)": sum_index_values(rb, BIOMASS_GROUPS["Forest residues"], "Used") / ENERGY_DIV,
            "MSW used (TWh)": sum_index_values(rb, BIOMASS_GROUPS["MSW"], "Used") / ENERGY_DIV,
            "Grassy energy crops used (TWh)": sum_index_values(rb, BIOMASS_GROUPS["Grassy energy crops"], "Used") / ENERGY_DIV,
            "Woody energy crops used (TWh)": sum_index_values(rb, BIOMASS_GROUPS["Woody energy crops"], "Used") / ENERGY_DIV,

            # 7) Biomethane
            "Biomethane (TWh)": abs_val(yb, "BIO_HYDROLYSIS", "GAS") / ENERGY_DIV,

            # 8) Biomass sequestration needs
            "BIOMASS_SEQUESTRATION needs (MtCO2)": abs_val(yb, "BIOMASS_SEQUESTRATION", "WOOD") / CO2_DIV,

            # 9) Carbon dual
            "Carbon dual global (€/tCO2)": carbon_dual,

            # 10) Biomass shadow prices
            "Shadow price agri residues (€/MWh)": mean_abs_dual(rd, BIOMASS_GROUPS["Agri residues"]) * DUAL_ENERGY_MULT,
            "Shadow price forest residues (€/MWh)": mean_abs_dual(rd, BIOMASS_GROUPS["Forest residues"]) * DUAL_ENERGY_MULT,
            "Shadow price MSW (€/MWh)": mean_abs_dual(rd, BIOMASS_GROUPS["MSW"]) * DUAL_ENERGY_MULT,
            "Shadow price grassy crops (€/MWh)": mean_abs_dual(rd, BIOMASS_GROUPS["Grassy energy crops"]) * DUAL_ENERGY_MULT,
            "Shadow price woody crops (€/MWh)": mean_abs_dual(rd, BIOMASS_GROUPS["Woody energy crops"]) * DUAL_ENERGY_MULT,
        }

    except Exception as e:
        return {
            "Extraction status": "error",
            "Extraction error": f"{type(e).__name__}: {e}",
        }

# ============================================================
# MAIN
# ============================================================

if MAKE_BACKUP:
    backup = XLSX_PATH.with_name(XLSX_PATH.stem + "_backup_before_results_v5.xlsx")
    if not backup.exists():
        shutil.copy2(XLSX_PATH, backup)

df = pd.read_excel(XLSX_PATH, sheet_name=SHEET_NAME)
df.columns = [str(c).strip() for c in df.columns]

# Build / refresh scenario names robustly
df["Scenario_name_for_outputs"] = df.apply(get_case_name, axis=1)

results = []
for case_name in df["Scenario_name_for_outputs"]:
    results.append(extract_metrics(case_name))

results_df = pd.DataFrame(results)

# remove previous result columns if they already exist
for col in results_df.columns:
    if col in df.columns:
        df = df.drop(columns=col)

df_out = pd.concat([df, results_df], axis=1)

with pd.ExcelWriter(
    XLSX_PATH,
    engine="openpyxl",
    mode="a",
    if_sheet_exists="replace"
) as writer:
    df_out.to_excel(writer, sheet_name=SHEET_NAME, index=False)

print("Workbook updated:", XLSX_PATH)
print(df_out["Extraction status"].value_counts(dropna=False))