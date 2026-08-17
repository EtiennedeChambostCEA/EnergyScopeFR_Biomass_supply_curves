
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

EXCEL_PATH = Path("Case studies analysis.xlsx")
SHEET_NAME = "Scenario results"

df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME).copy()

ID_COL = "run_id"
NAME_COL = "Scenario_name_for_outputs"

hypothesis_cols = [
    "E-biofuel_option",
    "ReFuel_blinding_mandates",
    "ReFuel_sustainability_criteria",
    "Energy_crops_impacts",
    "Geological_sequestration",
    "ENSPRESO_scenario",
]

result_cols = [
    "Total cost (M€)",
    "Carbon dual global (€/tCO2)",
    "CCU/CCUS (%)",
    "Total Fuels + HVC biomass needs (TWh)",
    "Sustain. fuels H2 needs (TWh)",
    "Sustain. fuels CO2 needs (MtCO2)",
    "BIOMASS_SEQUESTRATION needs (MtCO2)",
]

display_names = {
    "E-biofuel_option": "E-biofuel",
    "ReFuel_blinding_mandates": "ReFuelEU blend",
    "ReFuel_sustainability_criteria": "ReFuelEU sustainability",
    "Energy_crops_impacts": "Energy crops impacts",
    "Geological_sequestration": "Geological sequestration",
    "ENSPRESO_scenario": "ENSPRESO",
    "Total cost (M€)": "Total cost (B€)",
    "Carbon dual global (€/tCO2)": "Carbon dual (€/tCO2)",
    "CCU/CCUS (%)": "CCU/CCUS (%)",
    "Total Fuels + HVC biomass needs (TWh)": "Total Fuels + HVC biomass needs (TWh)",
    "Sustain. fuels H2 needs (TWh)": "Sustain. fuels H2 needs (TWh)",
    "Sustain. fuels CO2 needs (MtCO2)": "Sustain. fuels CO2 needs (MtCO2)",
    "BIOMASS_SEQUESTRATION needs (MtCO2)": "Biomass sequestration needs (MtCO2)",
}

category_orders = {
    "E-biofuel_option": ["No", "Yes"],
    "ReFuel_blinding_mandates": ["Yes", "No"],
    "ReFuel_sustainability_criteria": ["Yes", "No"],
    "Energy_crops_impacts": ["HIGH", "MED", "LOW"],
    "Geological_sequestration": ["LOW", "MED", "HIGH"],
    "ENSPRESO_scenario": ["LOW", "MED", "HIGH"],
}

def filter_df(dataframe, conditions):
    mask = pd.Series(True, index=dataframe.index)
    for col, value in conditions.items():
        mask &= dataframe[col].astype(str).eq(str(value))
    return dataframe.loc[mask].copy()

def normalize(series):
    s = pd.to_numeric(series, errors="coerce")
    if s.max() == s.min():
        return pd.Series(0.0, index=s.index)
    return (s - s.min()) / (s.max() - s.min())

def pick_one(candidates, sort_by, ascending=True, already_selected_ids=None, narrative="", method=""):
    if already_selected_ids is None:
        already_selected_ids = set()
    if candidates.empty:
        return None
    nondup = candidates.loc[~candidates[ID_COL].isin(already_selected_ids)].copy()
    base_sel = nondup if not nondup.empty else candidates.copy()
    picked = base_sel.sort_values(sort_by, ascending=ascending).iloc[[0]].copy()
    picked["Narrative"] = narrative
    picked["Selection_method"] = method
    return picked

selected = []
selected_ids = set()

optimistic_exact = filter_df(df, {
    "E-biofuel_option": "Yes",
    "ReFuel_blinding_mandates": "No",
    "ReFuel_sustainability_criteria": "No",
    "ENSPRESO_scenario": "HIGH",
    "Energy_crops_impacts": "LOW",
    "Geological_sequestration": "HIGH",
})
opt = pick_one(optimistic_exact, ["Total cost (M€)"], True, selected_ids,
               "1 - Optimistic", "Exact least-constrained combination + minimum total cost")
if opt is None:
    tmp = df.copy()
    tmp["less_constrained_score"] = (
        (tmp["E-biofuel_option"] == "Yes").astype(int)
        + (tmp["ReFuel_blinding_mandates"] == "No").astype(int)
        + (tmp["ReFuel_sustainability_criteria"] == "No").astype(int)
        + (tmp["ENSPRESO_scenario"] == "HIGH").astype(int)
        + (tmp["Energy_crops_impacts"] == "LOW").astype(int)
        + (tmp["Geological_sequestration"] == "HIGH").astype(int)
    )
    opt = pick_one(tmp, ["less_constrained_score", "Total cost (M€)"], [False, True], selected_ids,
                   "1 - Optimistic", "Fallback: highest less-constrained score + minimum total cost")
selected.append(opt); selected_ids.add(opt.iloc[0][ID_COL])

ref = pick_one(
    filter_df(df, {
        "E-biofuel_option": "No",
        "ReFuel_blinding_mandates": "Yes",
        "ReFuel_sustainability_criteria": "Yes",
        "ENSPRESO_scenario": "MED",
        "Energy_crops_impacts": "MED",
        "Geological_sequestration": "MED",
    }),
    ["Total cost (M€)"], True, selected_ids,
    "2 - Reference", "Exact MED/MED/MED + ReFuelEU constraints + no e-biofuel"
)
selected.append(ref); selected_ids.add(ref.iloc[0][ID_COL])

tradeoff = pick_one(
    filter_df(df, {
        "E-biofuel_option": "Yes",
        "ReFuel_blinding_mandates": "No",
        "ReFuel_sustainability_criteria": "Yes",
        "ENSPRESO_scenario": "MED",
        "Energy_crops_impacts": "MED",
        "Geological_sequestration": "MED",
    }),
    ["Total cost (M€)"], True, selected_ids,
    "3 - E-bio trade-off + regulation adaptation",
    "Exact reference-like setup with e-bio ON and no blending mandate"
)
selected.append(tradeoff); selected_ids.add(tradeoff.iloc[0][ID_COL])

tmp = df.copy()
tmp["h2_norm"] = normalize(tmp["Sustain. fuels H2 needs (TWh)"])
tmp["co2_norm"] = normalize(tmp["Sustain. fuels CO2 needs (MtCO2)"])
tmp["h2_co2_score"] = 0.5 * tmp["h2_norm"] + 0.5 * tmp["co2_norm"]
h2_rel = pick_one(
    tmp, ["h2_co2_score", "Sustain. fuels H2 needs (TWh)", "Sustain. fuels CO2 needs (MtCO2)"],
    [False, False, False], selected_ids,
    "4 - H2 reliance", "Highest combined normalized H2 and CO2 needs"
)
selected.append(h2_rel); selected_ids.add(h2_rel.iloc[0][ID_COL])

biomass_rel = pick_one(
    df, ["Total Fuels + HVC biomass needs (TWh)", "BIOMASS_SEQUESTRATION needs (MtCO2)"],
    [False, False], selected_ids,
    "5 - Biomass reliance", "Highest total biomass needs (tie-breaker: biomass sequestration needs)"
)
selected.append(biomass_rel); selected_ids.add(biomass_rel.iloc[0][ID_COL])

selected_df = pd.concat(selected, ignore_index=True)
selected_df["Total cost (B€)"] = selected_df["Total cost (M€)"] / 1000.0
selected_df.to_csv("selected_typical_scenarios_for_plot.csv", index=False)

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

fig, ax = plt.subplots(figsize=(22, 11))
x = np.arange(len(all_cols))
rng = np.random.default_rng(7)
split_x = len(hypothesis_cols) - 0.5

ax.axvspan(-0.5, split_x, facecolor="#f7f7f7", alpha=0.65, zorder=-3)
ax.axvspan(split_x, len(all_cols)-0.5, facecolor="#ffffff", alpha=1.0, zorder=-3)
ax.axvline(split_x, color="black", linewidth=3.4, alpha=0.9, zorder=-1)
ax.axvline(split_x - 0.06, color="black", linewidth=0.9, alpha=0.35, zorder=-1)
ax.axvline(split_x + 0.06, color="black", linewidth=0.9, alpha=0.35, zorder=-1)

for i in df.index:
    y = plot_df.loc[i, all_cols].astype(float).to_numpy().copy()
    for j, col in enumerate(hypothesis_cols):
        if not np.isnan(y[j]):
            y[j] += rng.uniform(-0.028, 0.028)
    ax.plot(x, y, color="#bdbdbd", alpha=0.18, linewidth=0.9, zorder=1)

for i, col in enumerate(all_cols):
    lw = 1.8 if col in hypothesis_cols else 1.4
    alp = 0.95 if col in hypothesis_cols else 0.75
    ax.axvline(i, color="black", linewidth=lw, alpha=alp, zorder=0)

for i, col in enumerate(all_cols):
    info = tick_info[col]
    for pos, lab in zip(info["positions"], info["labels"]):
        if col in hypothesis_cols:
            ax.text(i - 0.18, pos, str(lab), ha="right", va="center", fontsize=12.5, zorder=5,
                    bbox=dict(facecolor="white", edgecolor="none", alpha=0.94, pad=0.8), clip_on=False)
        else:
            ax.text(i + 0.08, pos, str(lab), ha="left", va="center", fontsize=13.3, zorder=5,
                    bbox=dict(facecolor="white", edgecolor="none", alpha=0.9, pad=0.6))

selected_colors = {
    "1 - Optimistic": "#1b9e77",
    "2 - Reference": "#7570b3",
    "3 - E-bio trade-off + regulation adaptation": "#d95f02",
    "4 - H2 reliance": "#e7298a",
    "5 - Biomass reliance": "#66a61e",
}

legend_lines = []
legend_labels = []

for _, row in selected_df.iterrows():
    scenario_id = row[ID_COL]
    y = plot_df.loc[df[ID_COL] == scenario_id, all_cols].iloc[0].astype(float).to_numpy().copy()
    local_rng = np.random.default_rng(abs(hash(str(scenario_id))) % (2**32))
    for j, col in enumerate(hypothesis_cols):
        if not np.isnan(y[j]):
            y[j] += local_rng.uniform(-0.015, 0.015)
    label = row["Narrative"]
    color = selected_colors[label]
    line, = ax.plot(x, y, color=color, linewidth=3.2, alpha=0.98, zorder=4, solid_capstyle="round")
    legend_lines.append(line)
    legend_labels.append(label)

ax.set_xticks(x)
ax.set_xticklabels([display_names[c] for c in all_cols], rotation=42, ha="right", fontsize=12.5)
ax.set_xlim(-0.6, len(all_cols) - 0.35)
ax.set_ylim(-0.08, 1.10)
ax.set_yticks([])
ax.spines["left"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["top"].set_visible(False)
ax.spines["bottom"].set_linewidth(1.0)
ax.set_title("Parallel coordinates with highlighted representative scenarios", fontsize=17, pad=14)

left_center = (len(hypothesis_cols) - 1) / 2
right_center = len(hypothesis_cols) + (len(result_cols) - 1) / 2
ax.text(left_center, 1.065, "Scenario assumptions", ha="center", va="bottom", fontsize=14,
        bbox=dict(facecolor="#e8e8e8", edgecolor="black", linewidth=0.8, pad=3.0, alpha=0.98))
ax.text(right_center, 1.065, "Model outputs", ha="center", va="bottom", fontsize=14,
        bbox=dict(facecolor="#f4f4f4", edgecolor="black", linewidth=0.8, pad=3.0, alpha=0.98))

leg = ax.legend(legend_lines, legend_labels, loc="upper left", bbox_to_anchor=(1.01, 1.0),
                frameon=True, title="Selected scenarios", fontsize=11, title_fontsize=12)
leg.get_frame().set_alpha(0.95)

plt.tight_layout()
plt.savefig("parallel_coordinates_selected_scenarios.png", dpi=220, bbox_inches="tight")
plt.show()
