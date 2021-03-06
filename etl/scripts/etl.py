# etl for gcp 2020

import os
import pandas as pd

from ddf_utils.str import to_concept_id

CONVERT_RATIO = 3.664

SOURCE_DIR = '../source/'
NATION_FILE = os.path.join(SOURCE_DIR, 'nation.xlsx')
GLOBAL_FILE = os.path.join(SOURCE_DIR, 'global.xlsx')
OUTPUT_DIR = '../../'

EXCEL_CONFIG_NATION = {
    'Territorial Emissions': {
        'skiprows': 10
    },
    'Consumption Emissions': {
        'skiprows': 7
    },
    'Emissions Transfers': {
        'skiprows': 7
    }
}

EXCEL_CONFIG_GLOBAL = {
    'Global Carbon Budget': {
        'skiprows': 20,
        'skipfooter': 1
    },
    'Fossil Emissions by Category': {
        'skiprows': 8
    },
    'Land-Use Change Emissions': {
        'skiprows': 27
    },
    'Ocean Sink': {
        'skiprows': 23
    },
    'Terrestrial Sink': {
        'skiprows': 23
    },
    'Cement Carbonation Sink': {
        'skiprows': 9
    },
    'Historical Budget': {
        'skiprows': 15,
        'skipfooter': 1
    }
}


"""
TODO:

- precision formatting
- for global data: indicator by model?

"""


### functions for datapoints

def global_carbon_budget_datapoints(sheet_data, historical=False):
    df = sheet_data.copy()
    df = df.loc[:, 'Year':]
    df = df.rename(columns={'Year': 'year'})
    df['global'] = 'world'
    # remove 2020 estimates
    df = df[~df['year'].isin(["2020*", "*2020"])]
    try:
        df['year'].dropna().astype(int)
    except ValueError:
        print('the year column contains non integer values')
        raise
    df = df.set_index(['global', 'year'])
    if historical:
        df.columns = df.columns.map(lambda x: 'historical '+x)
    df.columns = df.columns.map(to_concept_id)
    for c in df.columns:
        yield (c, df[c].dropna())


def fix_column_country_data(sheet_data):
    data = sheet_data.iloc[1:, :].copy()
    data = data.rename(columns={data.columns[0]: 'year'})
    data['year'] = data['year'].astype(int)
    return data


def get_data_from_nation_file(data, domain_name, indicator_name, start_col=None, end_col=None):
    df = data.set_index('year').loc[:, slice(start_col, end_col)].copy()
    df = df.stack().reset_index()
    df.columns = ['year', domain_name, indicator_name]
    df = df.set_index([domain_name, 'year']).sort_index()
    return df.dropna()


def country_carbon_emission_datapoints(data, indicator_name):
    return get_data_from_nation_file(data, 'nation', indicator_name, end_col='zimbabwe')


def region_carbon_emission_datapoints(data, indicator_name):
    return get_data_from_nation_file(data, 'region', indicator_name,
                                     start_col='kp_annex_b', end_col='bunkers')


def global_carbon_emission_datapoints(data, indicator_name):
    return get_data_from_nation_file(data, 'global', indicator_name,
                                     start_col='world',
                                     end_col='world')


def statistical_diff_datapoints(data, indicator_name):
    df = data.loc[:, ['year', 'statistical_difference']].copy()
    df['global'] = 'world'
    df = df.set_index(['global', 'year'])
    df.columns = [indicator_name]
    return df.sort_index().dropna()


def statistical_diff_datapoints(data, indicator_name):
    df = data.loc[:, ['year', 'statistical_difference']].copy()
    df['global'] = 'world'
    df = df.set_index(['global', 'year'])
    df.columns = [indicator_name]
    return df.sort_index().dropna()


def bunker_fuel_datapoints(data, indicator_name):
    df = data.loc[:, ['year', 'bunkers']].copy()
    df['global'] = 'world'
    df = df.set_index(['global', 'year'])
    df.columns = [indicator_name]
    return df.sort_index().dropna()


def main():
    # global datapoints
    sheet_name = 'Global Carbon Budget'
    global_budget = pd.read_excel(GLOBAL_FILE,
                                  sheet_name=sheet_name,
                                  **EXCEL_CONFIG_GLOBAL[sheet_name])
    for c, df in global_carbon_budget_datapoints(global_budget):
        concept_id = to_concept_id(c)
        df.to_csv(os.path.join(OUTPUT_DIR, f'ddf--datapoints--{concept_id}--by--global--year.csv'))

    sheet_name = 'Historical Budget'
    global_budget_hist = pd.read_excel(GLOBAL_FILE,
                                       sheet_name=sheet_name,
                                       **EXCEL_CONFIG_GLOBAL[sheet_name])
    for c, df in global_carbon_budget_datapoints(global_budget_hist, historical=True):
        concept_id = to_concept_id(c)
        df.to_csv(os.path.join(OUTPUT_DIR, f'ddf--datapoints--{concept_id}--by--global--year.csv'))

    # national datapoints
    for sheet_name, conf in EXCEL_CONFIG_NATION.items():
        indicator_name = to_concept_id(sheet_name)
        sheet_data = pd.read_excel(NATION_FILE, sheet_name=sheet_name, **conf)
        sheet_data = fix_column_country_data(sheet_data)
        data = sheet_data.copy()
        data.columns = data.columns.map(to_concept_id)
        df = country_carbon_emission_datapoints(data, indicator_name)
        df.to_csv(os.path.join(OUTPUT_DIR, f'ddf--datapoints--{indicator_name}--by--nation--year.csv'))
        df = region_carbon_emission_datapoints(data, indicator_name)
        df.to_csv(os.path.join(OUTPUT_DIR, f'ddf--datapoints--{indicator_name}--by--region--year.csv'))
        df = global_carbon_emission_datapoints(data, indicator_name)
        df.to_csv(os.path.join(OUTPUT_DIR, f'ddf--datapoints--{indicator_name}--by--global--year.csv'))
        stat_diff_name = indicator_name + '_statistical_difference'
        df = statistical_diff_datapoints(data, stat_diff_name)
        df.to_csv(os.path.join(OUTPUT_DIR, f'ddf--datapoints--{stat_diff_name}--by--global--year.csv'))
        bunker_name = indicator_name + '_by_bunkers'
        df = bunker_fuel_datapoints(data, bunker_name)
        df.to_csv(os.path.join(OUTPUT_DIR, f'ddf--datapoints--{bunker_name}--by--global--year.csv'))

    # entities
    entities = sheet_data.columns.tolist()[1:]
    entities.remove('Statistical Difference')
    entities_df = pd.DataFrame(entities, columns=['name'])
    entities_df['geo'] = entities_df['name'].map(to_concept_id)
    entities_df['is--nation'] = 'FALSE'
    entities_df['is--region'] = 'FALSE'
    entities_df['is--global'] = 'FALSE'

    entities_df = entities_df.set_index('geo')

    entities_df.loc[:'zimbabwe', 'is--nation'] = 'TRUE'
    entities_df.loc['kp_annex_b':'bunkers', 'is--region'] = 'TRUE'
    entities_df.loc['world', 'is--global'] = 'TRUE'

    entities_df.to_csv(os.path.join(OUTPUT_DIR, 'ddf--entities--geo.csv'))

    # concepts
    indicator_df = pd.read_excel('./indicators.xlsx', usecols=['concept_name', 'definition', 'unit'])
    indicator_df = indicator_df.rename(columns={'concept_name': 'name'})
    indicator_df['concept'] = indicator_df['name'].map(to_concept_id)
    indicator_df['concept_type'] = 'measure'

    discrete_df = pd.DataFrame.from_dict(
        dict(enumerate(
            [['name', 'Name', 'string', ''],
             ['geo', 'Geo location', 'entity_domain', ''],
             ['nation', 'nation', 'entity_set', 'geo'],
             ['region', 'region', 'entity_set', 'geo'],
             ['global', 'global', 'entity_set', 'geo'],
             ['domain', 'domain', 'string', ''],
             ['definition', 'definition', 'string', ''],
             ['unit', 'unit', 'string', ''],
             ['year', 'year', 'time', '']])),
        columns=['concept', 'name', 'concept_type', 'domain'],
        orient='index')

    concepts_df = pd.concat([indicator_df.set_index('concept'), discrete_df.set_index('concept')])

    concepts_df.to_csv(os.path.join(OUTPUT_DIR, 'ddf--concepts.csv'))


if __name__ == '__main__':
    main()
