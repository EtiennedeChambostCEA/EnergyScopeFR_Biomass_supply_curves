# -*- coding: utf-8 -*-
"""
Helper to inject ENSPRESO biomass data into in-memory DataFrames
before printing .dat files.

Usage:
    from my_energyscope.preprocessing.es_pre.enspreso import apply_enspreso
    apply_enspreso(config, all_df)  # all_df est le dict {'Resources':..., 'Layers_in_out':..., 'Technologies':...}
"""

import logging
from pathlib import Path
import pandas as pd

# Mapping ENSPRESO B-Com -> Resources.csv names
B_COM_TO_RESOURCE = {
    'olive pits': 'OLIVE_PITS',
    'apples': 'APPLES',
    'cerealstraw': 'CEREALSTRAW',
    'cherries': 'CHERRIES',
    'maizestraw': 'MAIZESTRAW',
    'olives': 'OLIVES',
    'osr': 'OSR',
    'ricestraw': 'RICESTRAW',
    'vineyards': 'VINEYARDS',
    'miscanthus': 'MISCANTHUS',
    'switchgrass': 'SWITCHGRASS',
    'willow': 'WILLOW',
    'poplar': 'POPLAR',
    'c&p_res': 'CP_RES',
    'fuelwood res': 'FUELWOOD_RES',
    'fuelwoodrw': 'FUELWOODRW',
    'landscapecare': 'LANDSCAPECARE',
    'msw': 'MSW',
    'othersecresid': 'OTHERSECRESID',
    'sawdust': 'SAWDUST',
    'c&p_rw': 'CP_RW',
    'grass for biogas': 'GRASS_FOR_BIOGAS',
    'maize silage': 'MAIZE_SILAGE',
    'sugarbeettops': 'SUGARBEETTOPS',
    'manure_liq': 'MANURE_LIQ',
    'manure_sol': 'MANURE_SOL',
    'sludge': 'SLUDGE',
}

# List of ENSPRESO biomass resources to drop when disabled
BIOMASS_RESOURCES = list(B_COM_TO_RESOURCE.values())

GROWTH_TECHS = ['WOOD_GROWTH', 'WET_BIOMASS_GROWTH']
ORIGINAL_GROWTH_KEY = '_enspreso_growth_f_max'


def _store_growth_max(config, technologies):
    if ORIGINAL_GROWTH_KEY in config:
        return
    config[ORIGINAL_GROWTH_KEY] = {
        tech: technologies.loc[tech, 'f_max']
        for tech in GROWTH_TECHS
        if tech in technologies.index
    }


def _restore_growth_max(config, technologies):
    if ORIGINAL_GROWTH_KEY not in config:
        return
    saved = config[ORIGINAL_GROWTH_KEY]
    for tech, val in saved.items():
        if tech in technologies.index:
            technologies.loc[tech, 'f_max'] = val


def apply_enspreso(config, all_df=None):
    """
    Update in-memory DataFrames using ENSPRESO supply curves.

    - If enable=False: remove ENSPRESO biomass resources from Resources/Layers_in_out,
      and restore growth tech f_max.
    - If enable=True: inject potentials/cost/GHG for mapped resources, set OTHER_GHG,
      and set growth tech f_max=0.

    Parameters
    ----------
    config : dict containing at least 'biomass_supply_curve'
    all_df : dict with keys 'Resources', 'Layers_in_out', 'Technologies';
             if None, uses config['all_data'].
    """
    if 'biomass_supply_curve' not in config:
        logging.info('biomass_supply_curve not set; skipping ENSPRESO')
        return

    if all_df is None:
        all_df = config.get('all_data', {})

    resources = all_df.get('Resources')
    layers = all_df.get('Layers_in_out')
    technologies = all_df.get('Technologies')

    if resources is None or layers is None or technologies is None:
        logging.warning('Missing required dataframes; skipping ENSPRESO')
        return

    cfg = config['biomass_supply_curve']
    enabled = bool(cfg.get('enable', False))

    # Always store original growth f_max to allow restoration
    _store_growth_max(config, technologies)

    if not enabled:
        # Disable: drop biomass resources and restore growth techs
        to_drop = [r for r in BIOMASS_RESOURCES if r in resources.index]
        if to_drop:
            resources.drop(index=to_drop, inplace=True, errors='ignore')
        if to_drop:
            layers.drop(index=to_drop, inplace=True, errors='ignore')
        _restore_growth_max(config, technologies)
        logging.info('ENSPRESO disabled: biomass resources removed and growth techs restored')
        return

    # Enabled: load ENSPRESO sheet
    xlsx_path = Path(cfg.get('xlsx_path', 'ENSPRESO_supply_curves_NUTS0.xlsx'))
    scenario = cfg.get('scenario', 'LOW')
    year = str(cfg.get('year', 2020))
    nuts0 = cfg.get('nuts0', 'FR')
    sheet_name = f'{scenario} {year} NUTS0'

    try:
        xl = pd.ExcelFile(xlsx_path)
        df = xl.parse(sheet_name)
    except Exception as exc:
        logging.error('Could not read ENSPRESO sheet %s: %s', sheet_name, exc)
        return

    df = df[df['NUTS0'].astype(str).str.upper() == str(nuts0).upper()]
    # Identify columns robustly
    potential_col = [c for c in df.columns if 'Potential' in c and 'TWh' in c][0]
    cost_col = [c for c in df.columns if 'Cost' in c and 'MWh' in c][0]
    ghg_col = [c for c in df.columns if 'GHG' in c and 'MWh' in c][0]

    # Update resources and layers
    for bcom_raw, res_name in B_COM_TO_RESOURCE.items():
        mask = df['B-Com'].str.lower() == bcom_raw
        if mask.any() and res_name in resources.index:
            row = df[mask].iloc[0]
            resources.loc[res_name, 'avail'] = row[potential_col]*1000
            resources.loc[res_name, 'c_op'] = row[cost_col]/1000
            resources.loc[res_name, 'gwp_op'] = row[ghg_col]/1000
            if res_name in layers.index and 'OTHER_GHG' in layers.columns:
                layers.loc[res_name, 'OTHER_GHG'] = row[ghg_col]/1000
        # silently ignore if not found or not present in CSV

    # Block growth techs
    for tech in GROWTH_TECHS:
        if tech in technologies.index:
            technologies.loc[tech, 'f_max'] = 0

    logging.info('ENSPRESO enabled: resources updated from %s, growth techs blocked', sheet_name)
