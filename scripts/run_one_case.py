# run_one_case.py

import os
import sys
from pathlib import Path
import pandas as pd
import warnings


project_path = Path(__file__).resolve().parents[0]  # dossier du script
# si ton script est dans /scripts/ alors parents[1] = racine repo
REPO_ROOT = project_path if (project_path / "config_ref.yaml").exists() else project_path.parents[0]

warnings.filterwarnings("ignore")

# =========================
# PARAMS
# =========================
TECH_NAME = "WOOD_GROWTH"
TWHY_TO_GW = 1000.0 / 8760.0

PROJECT_PATH = Path(r"C:\Users\ED281169\Documents\EnergyScope\EnergyScopeFR_Biomass_supply_curves")


def set_fmax(csv_path, tech_name, new_fmax):
    df = pd.read_csv(csv_path, sep=";", dtype=str, encoding="utf-8")
    df.columns = [c.strip() for c in df.columns]

    mask = df["Technologies param"].astype(str).str.strip().eq(tech_name)
    if mask.sum() != 1:
        raise ValueError(f"{tech_name} not found")

    old_val = str(df.loc[mask, "f_max"].iloc[0])
    use_comma = ("," in old_val) and ("." not in old_val)

    val = f"{new_fmax:.10f}"
    if use_comma:
        val = val.replace(".", ",")

    df.loc[mask, "f_max"] = val
    df.to_csv(csv_path, sep=";", index=False, encoding="utf-8")


if __name__ == "__main__":
    # =========================
    # INPUT ARGUMENT
    # =========================
    twh = int(sys.argv[1])
    fmax_val = twh * TWHY_TO_GW

    print(f"\n=== RUN {twh} TWh → f_max={fmax_val:.6f} ===")

    # =========================
    # MODIFY CSV
    # =========================
    tech_csv = PROJECT_PATH / "data" / "Technologies.csv"
    set_fmax(tech_csv, TECH_NAME, fmax_val)

    # =========================
    # RUN ENERGYSCOPE
    # =========================
    sys.path.append(str(PROJECT_PATH))
    import my_energyscope as es

    project_path = PROJECT_PATH

    config = es.load_config(config_fn='config_ref.yaml', project_path=project_path)

    config['Working_directory'] = str(project_path)
    config['case_studies'] = os.path.join(project_path, 'Demand  curve')
    config['case_study'] = f"test__WOOD_GROWTH_{twh}TWh"

    # reset log proprement
    config['ampl_options']['log_file'] = os.path.join(
        config['case_studies'],
        config['case_study'],
        "log.txt"
    )

    # disable supply curves
    cfg = config.setdefault('biomass_supply_curve', {})
    cfg.update({'scenario': 'LOW', 'year': 2050, 'nuts0': 'FR', 'enable': False})
    config['biomass_supply_curve'] = cfg

    # RUN
    es.import_data(config)
    es.print_data(config)
    es.run_es(config)

    print("Done:", config['case_study'])