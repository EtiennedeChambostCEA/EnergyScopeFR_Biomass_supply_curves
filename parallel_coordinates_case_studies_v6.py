from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

EXCEL_PATH = "Case studies analysis.xlsx"
SHEET_NAME = "Scenario results"

df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

hypothesis_cols = ['E-biofuel_option', 'ReFuel_blinding_mandates', 'ReFuel_sustainability_criteria', 'Energy_crops_impacts', 'Geological_sequestration', 'ENSPRESO_scenario']
result_cols = ['Total cost (M€)', 'Carbon dual global (€/tCO2)', 'CCU/CCUS (%)', 'Total Fuels + HVC biomass needs (TWh)', 'Sustain. fuels H2 needs (TWh)', 'Sustain. fuels CO2 needs (MtCO2)', 'BIOMASS_SEQUESTRATION needs (MtCO2)']

display_names = {'E-biofuel_option': 'E-biofuel', 'ReFuel_blinding_mandates': 'ReFuelEU blend', 'ReFuel_sustainability_criteria': 'ReFuelEU sustainability', 'Energy_crops_impacts': 'Energy crops impacts', 'Geological_sequestration': 'Geological sequestration', 'ENSPRESO_scenario': 'ENSPRESO', 'Total cost (M€)': 'Total cost (B€)', 'Carbon dual global (€/tCO2)': 'Carbon dual (€/tCO2)', 'CCU/CCUS (%)': 'CCU/CCUS (%)', 'Total Fuels + HVC biomass needs (TWh)': 'Total Fuels + HVC biomass needs (TWh)', 'Sustain. fuels H2 needs (TWh)': 'Sustain. fuels H2 needs (TWh)', 'Sustain. fuels CO2 needs (MtCO2)': 'Sustain. fuels CO2 needs (MtCO2)', 'BIOMASS_SEQUESTRATION needs (MtCO2)': 'Biomass sequestration needs (MtCO2)'}

category_orders = {'E-biofuel_option': ['No', 'Yes'], 'ReFuel_blinding_mandates': ['Yes', 'No'], 'ReFuel_sustainability_criteria': ['Yes', 'No'], 'Energy_crops_impacts': ['HIGH', 'MED', 'LOW'], 'Geological_sequestration': ['LOW', 'MED', 'HIGH'], 'ENSPRESO_scenario': ['LOW', 'MED', 'HIGH']}

all_cols = hypothesis_cols + result_cols
plot_df = pd.DataFrame(index=df.index)
tick_info = {}

for col in all_cols:
    if col in hypothesis_cols:
        ordered_values = category_orders[col]
        mapped = {v: 1 - i / (len(ordered_values) - 1) for i, v in enumerate(ordered_values)}
        plot_df[col] = df[col].astype(str).map(mapped)
        tick_info[col] = {"positions": [mapped[v] for v in ordered_values], "labels": ordered_values}
    else:
        series = pd.to_numeric(df[col], errors="coerce")
        display_series = series / 1000.0 if col == "Total cost (M€)" else series.copy()
        vmin, vmax = display_series.min(), display_series.max()
        plot_df[col] = (display_series - vmin) / (vmax - vmin) if vmax != vmin else 0.5
        labels = [f"{vmin:.0f}", f"{vmax:.0f}"] if col == "Total cost (M€)" else [f"{vmin:.1f}", f"{vmax:.1f}"]
        tick_info[col] = {"positions": [0.0, 1.0], "labels": labels}

cost_billion = pd.to_numeric(df["Total cost (M€)"], errors="coerce") / 1000.0
norm = mpl.colors.Normalize(vmin=cost_billion.min(), vmax=cost_billion.max())
cmap = plt.cm.viridis

fig, ax = plt.subplots(figsize=(20, 10))
x = np.arange(len(all_cols))
rng = np.random.default_rng(7)

split_x = len(hypothesis_cols) - 0.5
ax.axvspan(-0.5, split_x, facecolor="#f7f7f7", alpha=0.55, zorder=-3)
ax.axvspan(split_x, len(all_cols)-0.5, facecolor="#ffffff", alpha=1.0, zorder=-3)
ax.axvline(split_x, color="black", linewidth=3.2, alpha=0.9, zorder=-1)
ax.axvline(split_x - 0.06, color="black", linewidth=0.9, alpha=0.35, zorder=-1)
ax.axvline(split_x + 0.06, color="black", linewidth=0.9, alpha=0.35, zorder=-1)

for i in df.index:
    y = plot_df.loc[i, all_cols].astype(float).to_numpy().copy()
    for j, col in enumerate(hypothesis_cols):
        if not np.isnan(y[j]):
            y[j] += rng.uniform(-0.028, 0.028)
    ax.plot(x, y, color=cmap(norm(cost_billion.loc[i])), alpha=0.35, linewidth=1.25, zorder=1)

for i, col in enumerate(all_cols):
    lw = 1.8 if col in hypothesis_cols else 1.4
    alp = 0.9 if col in hypothesis_cols else 0.7
    ax.axvline(i, color="black", linewidth=lw, alpha=alp, zorder=0)

for i, col in enumerate(all_cols):
    info = tick_info[col]
    for pos, lab in zip(info["positions"], info["labels"]):
        if col in hypothesis_cols:
            ax.text(i - 0.18, pos, str(lab), ha="right", va="center", fontsize=12.5, zorder=5,
                    bbox=dict(facecolor="white", edgecolor="none", alpha=0.92, pad=0.8), clip_on=False)
        else:
            ax.text(i + 0.08, pos, str(lab), ha="left", va="center", fontsize=13.5, zorder=5,
                    bbox=dict(facecolor="white", edgecolor="none", alpha=0.88, pad=0.6))

ax.set_xticks(x)
ax.set_xticklabels([display_names[c] for c in all_cols], rotation=42, ha="right", fontsize=12.5)
ax.set_xlim(-0.6, len(all_cols) - 0.35)
ax.set_ylim(-0.08, 1.10)
ax.set_yticks([])
ax.spines["left"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["top"].set_visible(False)
ax.spines["bottom"].set_linewidth(1.0)
ax.set_title("Parallel coordinates of scenario assumptions and outcomes", fontsize=17, pad=14)

left_center = (len(hypothesis_cols) - 1) / 2
right_center = len(hypothesis_cols) + (len(result_cols) - 1) / 2
ax.text(left_center, 1.065, "Scenario assumptions", ha="center", va="bottom", fontsize=14,
        bbox=dict(facecolor="#e8e8e8", edgecolor="black", linewidth=0.8, pad=3.0, alpha=0.98))
ax.text(right_center, 1.065, "Model outputs", ha="center", va="bottom", fontsize=14,
        bbox=dict(facecolor="#f4f4f4", edgecolor="black", linewidth=0.8, pad=3.0, alpha=0.98))
ax.text(split_x, 1.005, "|", ha="center", va="center", fontsize=26, fontweight="bold", alpha=0.9)

sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
sm.set_array([])
cbar = plt.colorbar(sm, ax=ax, pad=0.02)
cbar.set_label("Total cost (B€)", fontsize=13)
cbar.ax.tick_params(labelsize=11)

plt.tight_layout()
plt.savefig("parallel_coordinates_case_studies_v6.png", dpi=220, bbox_inches="tight")
plt.show()
