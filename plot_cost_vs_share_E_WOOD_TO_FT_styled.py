# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

ROOT = Path(r"C:\Users\ED281169\Documents\EnergyScope\EnergyScopeFR_Biomass_supply_curves")
SCENARIO_XLSX = ROOT / "Case studies SNBC3 + ENSPRESO assumptions.xlsx"
CASE_STUDIES_DIR = ROOT / "case_studies"
OUT_DIR = ROOT / "postprocessing_216_runs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

EPS = 1e-9

COLOR_MAP = {
    "LOW": "tab:blue",
    "MED": "tab:orange",
    "HIGH": "tab:green",
}

MARKER_MAP = {
    "Yes": "s",   # square
    "No": "x",    # cross
}

SIZE_MAP = {
    "LOW": 35,
    "MED": 75,
    "HIGH": 140,
}


def read_txt_table(path: Path):
    return pd.read_csv(path, sep=r"\t+", engine="python")


def safe_sum(series):
    return float(pd.to_numeric(series, errors="coerce").fillna(0).sum())





def get_total_cost(output_dir: Path) -> float:
    df = read_txt_table(output_dir / "cost_breakdown.txt")
    return safe_sum(df["C_inv"]) + safe_sum(df["C_maint"]) + safe_sum(df["C_op"])


def read_layer(path):
    df = pd.read_csv(path, sep="\t", engine="python")
    df.columns = [str(c).strip() for c in df.columns]
    return df

def abs_sum(df, col):
    s = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return float(s.abs().sum())

def get_share_e_wood_to_ft(output_dir):
    df = read_layer(output_dir / "hourly_data" / "layer_FT_FUEL.txt")

    e = abs_sum(df, "E_WOOD_TO_FT")
    w = abs_sum(df, "WOOD_TO_FT")
    c = abs_sum(df, "CO2_TO_FT")

    total = e + w + c
    if total == 0:
        return float("nan")

    return c / total


def main():
    scenarios = pd.read_excel(SCENARIO_XLSX, sheet_name="Scenarios")
    scenarios.columns = [c.strip() if isinstance(c, str) else c for c in scenarios.columns]

    rows = []
    for _, row in scenarios.iterrows():
        name = row["Name"]
        output_dir = CASE_STUDIES_DIR / name / "output"

        if not output_dir.exists():
            rows.append({
                "Name": name,
                "total_cost_MEUR_per_y": np.nan,
                "share_CO2_TO_FT": np.nan,
                "status": "missing_output",
            })
            continue

        try:
            total_cost = get_total_cost(output_dir)
            share = get_share_e_wood_to_ft(output_dir)
            rows.append({
                "Name": name,
                "total_cost_MEUR_per_y": total_cost,
                "share_CO2_TO_FT": share,
                "status": "ok",
            })
        except Exception as e:
            rows.append({
                "Name": name,
                "total_cost_MEUR_per_y": np.nan,
                "share_CO2_TO_FT": np.nan,
                "status": f"error: {e}",
            })

    df = pd.DataFrame(rows)
    df = scenarios.merge(df, on="Name", how="left")
    df.to_csv(OUT_DIR / "cost_vs_share_CO2_TO_FT_styled.csv", index=False)

    plot_df = df[
        (df["status"] == "ok") &
        df["share_CO2_TO_FT"].notna() &
        df["total_cost_MEUR_per_y"].notna()
    ].copy()

    plt.figure(figsize=(8, 6))

    for _, r in plot_df.iterrows():
        x = r["share_CO2_TO_FT"] * 100.0
        y = r["total_cost_MEUR_per_y"]
        color = COLOR_MAP.get(str(r["ENSPRESO_scenario"]).strip(), "grey")
        marker = MARKER_MAP.get(str(r["ReFuel_sustainability_criteria"]).strip(), "o")
        size = SIZE_MAP.get(str(r["Geological_sequestration"]).strip(), 60)

        if marker == "x":
            plt.scatter(x, y, c=color, marker=marker, s=size, linewidths=1.6)
        else:
            plt.scatter(x, y, c=color, marker=marker, s=size, edgecolors="black", linewidths=0.5)

    plt.xlabel("Share of CO2_TO_FT in FT_FUEL (%)")
    plt.ylabel("Total cost (M€/y)")
    plt.title("Total cost vs CO2FT share")

    color_handles = [
        Line2D([0], [0], marker="o", color="w", label=f"ENSPRESO {k}",
               markerfacecolor=v, markeredgecolor="black", markersize=8)
        for k, v in COLOR_MAP.items()
    ]

    marker_handles = [
        Line2D([0], [0], marker="s", color="black", linestyle="None", label="Sustainability: Yes",
               markerfacecolor="white", markersize=8),
        Line2D([0], [0], marker="x", color="black", linestyle="None", label="Sustainability: No",
               markersize=8),
    ]

    size_handles = [
        plt.scatter([], [], c="grey", s=SIZE_MAP["LOW"], label="Geo. seq.: LOW"),
        plt.scatter([], [], c="grey", s=SIZE_MAP["MED"], label="Geo. seq.: MED"),
        plt.scatter([], [], c="grey", s=SIZE_MAP["HIGH"], label="Geo. seq.: HIGH"),
    ]

    first_legend = plt.legend(handles=color_handles, title="Color", loc="upper left", bbox_to_anchor=(1.02, 1.0))
    plt.gca().add_artist(first_legend)

    second_legend = plt.legend(handles=marker_handles, title="Marker", loc="upper left", bbox_to_anchor=(1.02, 0.68))
    plt.gca().add_artist(second_legend)

    plt.legend(handles=size_handles, title="Point size", loc="upper left", bbox_to_anchor=(1.02, 0.42))

    plt.tight_layout()
    plt.savefig(OUT_DIR / "fig_cost_vs_share_CO2_TO_FT_styled.png", dpi=300, bbox_inches="tight")
    plt.close()

    return df


if __name__ == "__main__":
    df = main()
    print(df.head())
