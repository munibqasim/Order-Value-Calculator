CREATE OR REPLACE PROCEDURE REVOPS.PRODUCTION.ORDER_VALUE_MONTHLY()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pandas','numpy')
HANDLER = 'run_script'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
def run_script(session: snowpark.Session) -> str:
    def ov_data_pull(session, active_sites_query, ov_job_query, ov_turned_on_date_query, ov_uom_ratios_query,scenario_charges_query):
        # Execute the SQL queries and create DataFrames
        active_sites_df = session.sql(active_sites_query).to_pandas()
        ov_job_df = session.sql(ov_job_query).to_pandas()
        ov_turned_on_date_df = session.sql(ov_turned_on_date_query).to_pandas()
        ov_uom_ratios_df = session.sql(ov_uom_ratios_query).to_pandas()
        scenario_charges_df = session.sql(scenario_charges_query).to_pandas()
        # Create the raw_data dictionary and put all DataFrames inside it
        raw_ov_data = {
            ''ACTIVE_IDS_DATA'': active_sites_df,
            ''OV_JOB_DATA'': ov_job_df,
            ''OV_TURNED_ON_DATA'': ov_turned_on_date_df,
            ''OV_UOM_RATIO_DATA'': ov_uom_ratios_df,
            ''SCENARIO_CHARGES_DATA'': scenario_charges_df
        }
        # Create account string
        account_list = active_sites_df[''ACCOUNT_ID''].unique()
        account_str = tuple([str(x) for x in account_list])
        raw_ov_data[''ACCOUNT_STR''] = account_str
        return raw_ov_data
    def imputed_fill_type(row):
        if (pd.isnull(row[''FINAL_CHARGE_PER_UNIT'']) != True and row[''FINAL_CHARGE_PER_UNIT''] > 0):
            return ''fcpu''
        elif pd.isnull(row[''OVERRIDDEN_CHARGE_PER_UNIT'']) != True and row[''OVERRIDDEN_CHARGE_PER_UNIT''] > 0:
            return ''overridden''
        elif (pd.isnull(
                row[''LABOUR_CHARGE_PER_UNIT''] +
                row[''MATERIALS_CHARGE_PER_UNIT''] +
                row[''OVERHEAD_CHARGE_PER_UNIT''] +
                row[''CUSTOM_SUM_CHARGE_PER_UNIT'']) != True and
              (row[''LABOUR_CHARGE_PER_UNIT''] + row[''MATERIALS_CHARGE_PER_UNIT''] +                       row[''OVERHEAD_CHARGE_PER_UNIT''] +
               row[''CUSTOM_SUM_CHARGE_PER_UNIT'']) > 0):
            return ''sum_scenario''
        elif (pd.isnull(
                row[''LABOUR_CHARGE_PER_UNIT''] +
                row[''MATERIALS_CHARGE_PER_UNIT''] +
                row[''OVERHEAD_CHARGE_PER_UNIT'']) != True and
              (row[''LABOUR_CHARGE_PER_UNIT''] + row[''MATERIALS_CHARGE_PER_UNIT''] + row[''OVERHEAD_CHARGE_PER_UNIT'']) > 0):
            return ''sum_scenario_wo_custom''
        else:
            return ''no_charge''
    def imputed_charge_per_unit_calc(row):
        if pd.isnull(row[''FINAL_CHARGE_PER_UNIT'']) != True and row[''FINAL_CHARGE_PER_UNIT''] > 0:
            return row[''FINAL_CHARGE_PER_UNIT'']
        elif pd.isnull(row[''OVERRIDDEN_CHARGE_PER_UNIT'']) != True and row[''OVERRIDDEN_CHARGE_PER_UNIT''] > 0:
            return row[''OVERRIDDEN_CHARGE_PER_UNIT'']
        elif (pd.isnull(
                row[''LABOUR_CHARGE_PER_UNIT''] +
                row[''MATERIALS_CHARGE_PER_UNIT''] +
                row[''OVERHEAD_CHARGE_PER_UNIT''] +
                row[''CUSTOM_SUM_CHARGE_PER_UNIT'']) != True and
              (row[''LABOUR_CHARGE_PER_UNIT''] + row[''MATERIALS_CHARGE_PER_UNIT''] + row[''OVERHEAD_CHARGE_PER_UNIT''] +
               row[''CUSTOM_SUM_CHARGE_PER_UNIT'']) > 0):
            return (row[''LABOUR_CHARGE_PER_UNIT''] + row[''MATERIALS_CHARGE_PER_UNIT''] +
                    row[''OVERHEAD_CHARGE_PER_UNIT''] + row[''CUSTOM_SUM_CHARGE_PER_UNIT''])
        elif (pd.isnull(
                row[''LABOUR_CHARGE_PER_UNIT''] +
                row[''MATERIALS_CHARGE_PER_UNIT''] +
                row[''OVERHEAD_CHARGE_PER_UNIT'']) != True and
              (row[''LABOUR_CHARGE_PER_UNIT''] + row[''MATERIALS_CHARGE_PER_UNIT''] + row[''OVERHEAD_CHARGE_PER_UNIT'']) > 0):
            return (row[''LABOUR_CHARGE_PER_UNIT''] + row[''MATERIALS_CHARGE_PER_UNIT''] +
                    row[''OVERHEAD_CHARGE_PER_UNIT''])
        else:
            return 0
    def mad(data, c=1.4826, axis=None):
        """
        Calculate the Median Absolute Deviation (MAD) for a dataset.
        Parameters:
        - data: array-like, the dataset to calculate MAD for.
        - c: scaling constant. Default is 1.4826 to make MAD consistent with standard deviation.
        - axis: axis along which to compute the median. Default is None.
        Returns:
        - mad: float, the MAD of the dataset.
        """
        median = np.median(data, axis=axis)
        deviation = np.abs(data - median)
        mad = np.median(deviation, axis=axis)
        return mad * c
    def outlier_removal(ov_df, mad_factor, lower_quantile):
        output_df = pd.DataFrame()
        for site in ov_df[''SITE_ID''].unique():
            # Subset data
            site_ov_data = ov_df[ov_df[''SITE_ID''] == site].copy()
            # Calculate MAD
            mad_values = mad(site_ov_data[''IMPUTED_CHARGE_PER_UNIT'']) / mad_factor
            # Define bounds for outlier removal
            lower_bound = 0.001
            upper_bound = site_ov_data[''IMPUTED_CHARGE_PER_UNIT''].quantile(1 - lower_quantile) + mad_values + 1
            # Initialize columns to zero
            site_ov_data[''IMPUTED_CHARGE_PER_UNIT_WO_OUTLIERS''] = 0
            site_ov_data[''OUTLIER_CHARGE''] = 0
            # Remove outliers
            site_ov_data[''IMPUTED_CHARGE_PER_UNIT_WO_OUTLIERS''] = np.where(
                ((site_ov_data[''IMPUTED_CHARGE_PER_UNIT''] > upper_bound) |
                 (site_ov_data[''IMPUTED_CHARGE_PER_UNIT''] < lower_bound)),
                0,
                site_ov_data[''IMPUTED_CHARGE_PER_UNIT'']
            )
            site_ov_data[''OUTLIER_CHARGE''] = np.where(
                ((site_ov_data[''IMPUTED_CHARGE_PER_UNIT''] > upper_bound) |
                 (site_ov_data[''IMPUTED_CHARGE_PER_UNIT''] < lower_bound)),
                1,
                0
            )
            # Assign total charge per unit columns
            site_ov_data = site_ov_data.assign(
                TOTAL_CHARGE_PER_JOB=site_ov_data[''IMPUTED_CHARGE_PER_UNIT''] * site_ov_data[''MODIFIED_QUANTITY''],
                TOTAL_CHARGE_PER_JOB_IMPUTE=site_ov_data[''IMPUTED_CHARGE_PER_UNIT_WO_OUTLIERS''] * site_ov_data[''MODIFIED_QUANTITY''],
                MATERIALS_CHARGE_PER_JOB=site_ov_data[''MATERIALS_CHARGE_PER_UNIT''] * site_ov_data[''MODIFIED_QUANTITY''],
                FCPU_CHARGE_PER_JOB=site_ov_data[''FINAL_CHARGE_PER_UNIT''] * site_ov_data[''MODIFIED_QUANTITY''],
                OVERHEAD_CHARGE_PER_JOB=site_ov_data[''OVERHEAD_CHARGE_PER_UNIT''] * site_ov_data[''MODIFIED_QUANTITY''],
                LABOR_CHARGE_PER_JOB=site_ov_data[''LABOUR_CHARGE_PER_UNIT''] * site_ov_data[''MODIFIED_QUANTITY''],
                CUSTOM_CHARGE_PER_JOB=site_ov_data[''CUSTOM_SUM_CHARGE_PER_UNIT''] * site_ov_data[''MODIFIED_QUANTITY''],
                OVERRIDDEN_CHARGE_PER_JOB=site_ov_data[''OVERRIDDEN_CHARGE_PER_UNIT''] * site_ov_data[''MODIFIED_QUANTITY'']
            )
            output_df = pd.concat([output_df, site_ov_data], axis=0)
        return output_df
    def format_ov_input_data(ov_job_df, scenario_charges_df, active_ids_df, ov_uom_df):
        # Ensure all column names are in uppercase
        ov_job_df.columns = ov_job_df.columns.str.upper()
        scenario_charges_df.columns = scenario_charges_df.columns.str.upper()
        active_ids_df.columns = active_ids_df.columns.str.upper()
        ov_uom_df.columns = ov_uom_df.columns.str.upper()
        # Merge dataframes
        output_data = ov_job_df.merge(scenario_charges_df, on=[''JOB_ID'', ''SITE_ID'', ''ACCOUNT_ID''], how=''left'')
        output_data = active_ids_df.merge(output_data, on=[''SITE_ID'', ''ACCOUNT_ID''], how=''inner'')
        output_data = output_data.merge(ov_uom_df, on=[''ACCOUNT_ID'', ''CASE_UOM_ID''], how=''left'')
        if output_data.empty:
            return None
        # Converting the produced_quantity, except for situations where we don''t have scenario data
        output_data[''MODIFIED_QUANTITY''] = np.where(
            (output_data[''UOM_PRODUCED''] != output_data[''SCEN_UOM'']) & (~output_data[''SCEN_UOM''].isna()),
            output_data[''PRODUCED_QUANTITY''] / output_data[''RATIO_TO_REFERENCE_UOM''],
            output_data[''PRODUCED_QUANTITY'']
        )
        num_cols = [''FINAL_CHARGE_PER_UNIT'', ''LABOUR_CHARGE_PER_UNIT'', ''MATERIALS_CHARGE_PER_UNIT'',
                    ''OVERHEAD_CHARGE_PER_UNIT'', ''OVERRIDDEN_CHARGE_PER_UNIT'', ''CUSTOM_SUM_CHARGE_PER_UNIT'',
                    ''PRODUCED_QUANTITY'', ''MODIFIED_QUANTITY'']
        for col in num_cols:
            output_data[col] = output_data[col].astype(''float64'')
        output_data[''IMPUTED_CHARGE_PER_UNIT''] = output_data.apply(imputed_charge_per_unit_calc, axis=1)
        # Gets the fill type for each imputed charge
        output_data[''FILL_TYPE''] = output_data.apply(imputed_fill_type, axis=1)
        for fill_type in [''fcpu'', ''overridden'', ''sum_scenario'', ''sum_scenario_wo_custom'', ''no_charge'']:
            output_data[fill_type.upper() + ''_FILL''] = np.where(output_data[''FILL_TYPE''] == fill_type, 1, 0)
        clean_output_data = outlier_removal(ov_df=output_data, mad_factor=5, lower_quantile=0.05)
        clean_output_data = clean_output_data.drop(columns=[''UOM_PRODUCED'', ''SKU_ID'', ''CASE_UOM_ID'', ''PALLET_UOM_ID'',
                                                            ''RATIO_UOM'', ''UOM_RATIOS_ID'', ''SCEN_UOM'', ''SCEN_UOM_LABEL'',
                                                            ''RATIO_TO_REFERENCE_UOM''])
        return clean_output_data
    def job_aggregator(ov_df):
        ''''''
        Aggregates jobs by COMPANY_ID, ACCOUNT_ID, and SITE_ID
        ''''''
        clean_data = (ov_df
                      .groupby([''COMPANY_ID'', ''ACCOUNT_ID'', ''SITE_ID''])
                      .agg({
                          ''TOTAL_CHARGE_PER_JOB'': ''sum'',
                          ''TOTAL_CHARGE_PER_JOB_IMPUTE'': ''sum'',
                          ''CREATED_AT'': ''min'',
                          ''CREATED_AT_V2'': ''max'',
                          ''PROJECT_ID'': pd.Series.nunique,
                          ''IMPUTED_CHARGE_PER_UNIT'': lambda ts: (ts > 0).sum(),
                          ''IMPUTED_CHARGE_PER_UNIT_WO_OUTLIERS'': lambda ts: (ts > 0).sum(),
                          ''MATERIALS_CHARGE_PER_JOB'': ''sum'',
                          ''MATERIALS_CHARGE_PER_UNIT'': lambda ts: (ts > 0).sum(),
                          ''FCPU_CHARGE_PER_JOB'': ''sum'',
                          ''OVERHEAD_CHARGE_PER_JOB'': ''sum'',
                          ''OVERHEAD_CHARGE_PER_UNIT'': lambda ts: (ts > 0).sum(),
                          ''LABOR_CHARGE_PER_JOB'': ''sum'',
                          ''LABOUR_CHARGE_PER_UNIT'': lambda ts: (ts > 0).sum(),
                          ''CUSTOM_CHARGE_PER_JOB'': ''sum'',
                          ''CUSTOM_SUM_CHARGE_PER_UNIT'': lambda ts: (ts > 0).sum(),
                          ''OVERRIDDEN_CHARGE_PER_JOB'': ''sum'',
                          ''OVERRIDDEN_CHARGE_PER_UNIT'': lambda ts: (ts > 0).sum(),
                          ''FCPU_FILL'': ''sum'',
                          ''OVERRIDDEN_FILL'': ''sum'',
                          ''SUM_SCENARIO_FILL'': ''sum'',
                          ''SUM_SCENARIO_WO_CUSTOM_FILL'': ''sum'',
                          ''NO_CHARGE_FILL'': ''sum'',
                          ''OUTLIER_CHARGE'': ''sum'',
                          ''JOB_ID'': ''count''
                      })
                      .reset_index()
                      .rename(columns={
                          ''PROJECT_ID'': ''TOTAL_NUMBER_OF_PROJECTS'',
                          ''CREATED_AT_V2'': ''LAST_JOB_CREATED_DATE'',
                          ''CREATED_AT'': ''FIRST_JOB_CREATED_DATE'',
                          ''IMPUTED_CHARGE_PER_UNIT'': ''CHARGE_FILL_RATE'',
                          ''IMPUTED_CHARGE_PER_UNIT_WO_OUTLIERS'': ''CHARGE_FILL_RATE_WO_OUTLIERS'',
                          ''TOTAL_CHARGE_PER_JOB_IMPUTE'': ''TOTAL_CHARGE_IMPUTE'',
                          ''TOTAL_CHARGE_PER_JOB'': ''TOTAL_CHARGE'',
                          ''MATERIALS_CHARGE_PER_JOB'': ''TOTAL_MATERIAL_CHARGE'',
                          ''MATERIALS_CHARGE_PER_UNIT'': ''MATERIALS_CHARGE_FILL_RATE'',
                          ''FCPU_CHARGE_PER_JOB'': ''TOTAL_FCPU'',
                          ''FCPU_CHARGE_FILL_RATE'': ''FCPU_CHARGE_FILL_RATE'',
                          ''OVERHEAD_CHARGE_PER_JOB'': ''TOTAL_OVERHEAD_CHARGE'',
                          ''OVERHEAD_CHARGE_PER_UNIT'': ''OVERHEAD_CHARGE_FILL_RATE'',
                          ''LABOR_CHARGE_PER_JOB'': ''TOTAL_LABOR_CHARGE'',
                          ''LABOUR_CHARGE_PER_UNIT'': ''LABOR_CHARGE_FILL_RATE'',
                          ''CUSTOM_CHARGE_PER_JOB'': ''TOTAL_CUSTOM_CHARGE'',
                          ''CUSTOM_SUM_CHARGE_PER_UNIT'': ''CUSTOM_CHARGE_FILL_RATE'',
                          ''OVERRIDDEN_CHARGE_PER_JOB'': ''TOTAL_OVERRIDDEN_CHARGE'',
                          ''OVERRIDDEN_CHARGE_PER_UNIT'': ''OVERRIDDEN_CHARGE_FILL_RATE'',
                          ''JOB_ID'': ''TOTAL_NUM_JOBS'',
                          ''OUTLIER_CHARGE'': ''NUM_OUTLIERS''
                      })
                      )
        return clean_data
    def ov_variable_creator(aggregated_df):
        ''''''
        Takes output from job_aggregator and creates variables used in OV calc.
        ''''''
        output_df = aggregated_df.assign(
            PERCENT_FILL_RATE=(aggregated_df.CHARGE_FILL_RATE /
                               aggregated_df.TOTAL_NUM_JOBS),
            OV_EXTRAPOLATED=(aggregated_df.TOTAL_CHARGE /
                             (aggregated_df.CHARGE_FILL_RATE /
                              aggregated_df.TOTAL_NUM_JOBS)),
            OV_IMPUTE_EXTRAPOLATED=(aggregated_df.TOTAL_CHARGE_IMPUTE /
                                    (aggregated_df.CHARGE_FILL_RATE_WO_OUTLIERS /
                                     aggregated_df.TOTAL_NUM_JOBS)),
            MATERIALS_CHARGE_FILL_RATE=(aggregated_df.MATERIALS_CHARGE_FILL_RATE /
                                        aggregated_df.TOTAL_NUM_JOBS),
            OVERHEAD_CHARGE_FILL_RATE=(aggregated_df.OVERHEAD_CHARGE_FILL_RATE /
                                       aggregated_df.TOTAL_NUM_JOBS),
            LABOR_CHARGE_FILL_RATE=(aggregated_df.LABOR_CHARGE_FILL_RATE /
                                    aggregated_df.TOTAL_NUM_JOBS),
            CUSTOM_CHARGE_FILL_RATE=(aggregated_df.CUSTOM_CHARGE_FILL_RATE /
                                     aggregated_df.TOTAL_NUM_JOBS),
            OVERRIDDEN_CHARGE_FILL_RATE=(aggregated_df.OVERRIDDEN_CHARGE_FILL_RATE /
                                         aggregated_df.TOTAL_NUM_JOBS),
            PERCENT_FCPU=(aggregated_df.FCPU_FILL / aggregated_df.TOTAL_NUM_JOBS),
            PERCENT_OVERRIDDEN=(aggregated_df.OVERRIDDEN_FILL / aggregated_df.TOTAL_NUM_JOBS),
            PERCENT_SUM_SCENARIO=(aggregated_df.SUM_SCENARIO_FILL / aggregated_df.TOTAL_NUM_JOBS),
            PERCENT_SUM_SCENARIO_WO_CUSTOM=(aggregated_df.SUM_SCENARIO_WO_CUSTOM_FILL / aggregated_df.TOTAL_NUM_JOBS),
            PERCENT_NO_CHARGES=(aggregated_df.NO_CHARGE_FILL / aggregated_df.TOTAL_NUM_JOBS),
            PERCENT_OUTLIERS=(aggregated_df.NUM_OUTLIERS / aggregated_df.TOTAL_NUM_JOBS)
        )
        output_df = output_df.drop(columns=[''TOTAL_CHARGE'', ''TOTAL_CHARGE_IMPUTE'',
                                            ''FIRST_JOB_CREATED_DATE'',
                                            ''LAST_JOB_CREATED_DATE'',
                                            ''OVERRIDDEN_FILL'', ''SUM_SCENARIO_FILL'',
                                            ''SUM_SCENARIO_WO_CUSTOM_FILL'', ''NO_CHARGE_FILL'',
                                            ''NUM_OUTLIERS''])
        return output_df
    def ov_calculator(eng_df):
        ''''''
        Takes output from ov_variable_creator and applies OV formula:
        # Adjusted Order Value = A x (100% - B) + A x 10% + A x (B - 10%) x 5%
        # A = Order Value
        # B = Materials %
        ''''''
        ov_calc_output = eng_df
        ov_calc_output[''PERCENT_OV_IMPUTE_MATERIALS''] = (ov_calc_output[''TOTAL_MATERIAL_CHARGE'']
                                                         / ov_calc_output[''OV_IMPUTE_EXTRAPOLATED''])
        ov_calc_output[''ADJUSTED_OV_IMPUTE_EXTRAPOLATED''] = np.where((ov_calc_output[''PERCENT_OV_IMPUTE_MATERIALS''] >= 0.1) &
                                                                     (ov_calc_output[''PERCENT_OV_IMPUTE_MATERIALS''] < 1),
                                                                     (ov_calc_output[''OV_IMPUTE_EXTRAPOLATED''] *
                                                                      (1 - ov_calc_output[''PERCENT_OV_IMPUTE_MATERIALS''])
                                                                      + ov_calc_output[''OV_IMPUTE_EXTRAPOLATED''] * 0.1
                                                                      + ov_calc_output[''OV_IMPUTE_EXTRAPOLATED''] *
                                                                      (ov_calc_output[''PERCENT_OV_IMPUTE_MATERIALS''] - 0.1)
                                                                      * 0.05),
                                                                     ov_calc_output[''OV_IMPUTE_EXTRAPOLATED''])
        # Replace nan''s with 0 for sites for which OV is nan due to percent_fill_rate = 0
        affected_cols = [''PERCENT_OV_IMPUTE_MATERIALS'', ''OV_EXTRAPOLATED'', ''OV_IMPUTE_EXTRAPOLATED'', ''ADJUSTED_OV_IMPUTE_EXTRAPOLATED'']
        ov_calc_output[affected_cols] = ov_calc_output[affected_cols].replace({np.nan: 0})
        ov_calc_output = (ov_calc_output
                          .assign(
                              OV_EXTRAPOLATED=(
                                  ov_calc_output[''OV_EXTRAPOLATED''].map(''${:,.2f}''.format)),
                              OV_IMPUTE_EXTRAPOLATED=(
                                  ov_calc_output[''OV_IMPUTE_EXTRAPOLATED''].map(''${:,.2f}''.format)),
                              ADJUSTED_OV_IMPUTE_EXTRAPOLATED=(
                                  ov_calc_output[''ADJUSTED_OV_IMPUTE_EXTRAPOLATED''].map(''${:,.2f}''.format)),
                              TOTAL_FCPU=(
                                  ov_calc_output[''TOTAL_FCPU''].map(''${:,.2f}''.format)),
                              TOTAL_MATERIAL_CHARGE=(
                                  ov_calc_output[''TOTAL_MATERIAL_CHARGE''].map(''${:,.2f}''.format)),
                              PERCENT_FILL_RATE=(
                                  ov_calc_output[''PERCENT_FILL_RATE''].map(''{:,.2%}''.format)),
                              PERCENT_FCPU=(
                                  ov_calc_output[''PERCENT_FCPU''].map(''{:,.2%}''.format)),
                              PERCENT_OVERRIDDEN=(
                                  ov_calc_output[''PERCENT_OVERRIDDEN''].map(''{:,.2%}''.format)),
                              PERCENT_SUM_SCENARIO=(
                                  ov_calc_output[''PERCENT_SUM_SCENARIO''].map(''{:,.2%}''.format)),
                              PERCENT_SUM_SCENARIO_WO_CUSTOM=(
                                  ov_calc_output[''PERCENT_SUM_SCENARIO_WO_CUSTOM''].map(''{:,.2%}''.format)),
                              PERCENT_NO_CHARGES=(
                                  ov_calc_output[''PERCENT_NO_CHARGES''].map(''{:,.2%}''.format)),
                              PERCENT_OV_IMPUTE_MATERIALS=(
                                  ov_calc_output[''PERCENT_OV_IMPUTE_MATERIALS''].map(''{:,.2%}''.format)),
                              PERCENT_OUTLIERS=(
                                  ov_calc_output[''PERCENT_OUTLIERS''].map(''{:,.2%}''.format))
                          ))
        return ov_calc_output
    def final_ov_formatter(raw_ov_df, ov_turned_on_df, active_ids_df, start_date, end_date):
        ''''''
        Performs final formatting and saving of the OV report
        ''''''
        final_ov_df = raw_ov_df.rename(columns={})
        final_ov_df = final_ov_df.merge(ov_turned_on_df, on=[''COMPANY_ID'', ''ACCOUNT_ID''], how=''left'')
        final_ov_df = (active_ids_df[[''COMPANY_ID'', ''ACCOUNT_ID'', ''SITE_ID'']].drop_duplicates()
                       .merge(final_ov_df, on=[''COMPANY_ID'', ''ACCOUNT_ID'', ''SITE_ID''], how=''inner''))
        final_ov_df = final_ov_df.rename(columns={
            ''OV_EXTRAPOLATED'': ''OV_W_OUTLIERS'',
            ''OV_IMPUTE_EXTRAPOLATED'': ''OV_WO_OUTLIERS'',
            ''ADJUSTED_OV_IMPUTE_EXTRAPOLATED'': ''ADJUSTED_OV_WO_OUTLIERS'',
            ''PERCENT_OV_IMPUTE_MATERIALS'': ''PERCENT_OV_MATERIALS''
        })
        final_ov_df[''START_DATE''] = start_date
        final_ov_df[''END_DATE''] = end_date
        # Define target columns
        target_cols = [''START_DATE'', ''END_DATE'', ''COMPANY_ID'', ''ACCOUNT_ID'', ''SITE_ID'', ''DATE_OV_TURNED_ON'',
                       ''TOTAL_NUMBER_OF_PROJECTS'', ''TOTAL_NUM_JOBS'', ''PERCENT_FCPU'', ''TOTAL_FCPU'',
                       ''PERCENT_OVERRIDDEN'', ''PERCENT_SUM_SCENARIO'', ''PERCENT_SUM_SCENARIO_WO_CUSTOM'', ''PERCENT_NO_CHARGES'',
                       ''PERCENT_FILL_RATE'', ''PERCENT_OV_MATERIALS'', ''OV_W_OUTLIERS'', ''PERCENT_OUTLIERS'', ''OV_WO_OUTLIERS'',
                       ''ADJUSTED_OV_WO_OUTLIERS'']
        final_ov_df = final_ov_df[target_cols]
        return final_ov_df
    def ov(raw_ov_data_dict, start_date, end_date):
        ''''''
        Wrapper for OV calculation working on raw data pulled from the DB.
        ''''''
        clean_ov_input_data = format_ov_input_data(
            ov_job_df=raw_ov_data_dict[''OV_JOB_DATA''],
            scenario_charges_df=raw_ov_data_dict[''SCENARIO_CHARGES_DATA''],
            active_ids_df=raw_ov_data_dict[''ACTIVE_IDS_DATA''],
            ov_uom_df=raw_ov_data_dict[''OV_UOM_RATIO_DATA'']
        )
        if clean_ov_input_data is None:
            print("Empty dataframe. Exiting.")
            return None
        # Aggregate jobs to calculate OV
        clean_ov_data_agg = job_aggregator(ov_df=clean_ov_input_data)
        # Create features to calculate OV
        eng_ov_data_agg = ov_variable_creator(aggregated_df=clean_ov_data_agg)
        # Calculate OV
        raw_ov_output = ov_calculator(eng_df=eng_ov_data_agg)
        # Clean OV
        output_df = final_ov_formatter(
            raw_ov_df=raw_ov_output,
            ov_turned_on_df=raw_ov_data_dict[''OV_TURNED_ON_DATA''],
            start_date=start_date,
            end_date=end_date,
            active_ids_df=raw_ov_data_dict[''ACTIVE_IDS_DATA'']
        )
        return output_df
    def main(session: snowpark.Session): 
        today = datetime.today()
        first_day_of_current_month = today.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
         # Create a list of start and end dates for each month from January to the last month
        date_ranges = []
        for month in range(1, today.month):
            start_date = datetime(today.year, month, 1)
            if month == today.month - 1:  # For the last month, set the end date to the last day of the previous month
                end_date = last_day_of_previous_month
            else:
                end_date = (start_date.replace(month=month + 1) - timedelta(days=1))
            date_ranges.append((start_date.strftime(''%Y-%m-%d''), end_date.strftime(''%Y-%m-%d'')))
        for start_date, end_date in date_ranges:
            # Define the SQL queries with placeholders for start_date and end_date
            active_sites_query = """
                SELECT
                    companies.company_id as company_id,
                    accounts.account_id as account_id,
                    accounts.enable_order_value_pricing_model,
                    sites.site_id as site_id
                FROM PM_PROD_EDW.LATEST.sites
                LEFT JOIN PM_PROD_EDW.LATEST.accounts ON accounts.account_id = sites.account_id
                LEFT JOIN PM_PROD_EDW.LATEST.companies ON companies.company_id = accounts.company_id
                WHERE 
                    sites.active = True 
            """
            ov_job_query = f"""
                SELECT
                    sites.account_id,
                    jobs.site_id,
                    WORK_ORDERS.WORK_ORDER_ID as project_id,
                    work_orders.final_charge_per_unit,
                    work_orders.scenario_id,
                    jobs.job_id as job_id,
                    jobs.created_at,
                    jobs.created_at as created_at_v2,
                    jobs.produced_quantity_value_in_reference_uom / default_uom_ratio.ratio_to_reference_uom as produced_quantity,
                    default_uom_ratio.unit_of_measure_id as uom_produced,
                    unit_of_measures.label as uom_produced_label,
                    work_orders.item_id as sku_id,
                    uom_contexts.case_uom_ratio_id as case_uom_id,
                    uom_contexts.full_pallet_uom_ratio_id as pallet_uom_id,
                    default_uom_ratio.unit_of_measure_id as ratio_uom,
                    default_uom_ratio.uom_ratio_id as uom_ratios_id
                FROM PM_PROD_EDW.LATEST.JOBS
                LEFT JOIN PM_PROD_EDW.LATEST.WORK_ORDERS ON work_orders.work_order_id = jobs.WORK_ORDER_ID
                LEFT JOIN PM_PROD_EDW.LATEST.SITES ON jobs.site_id = sites.site_id
                LEFT JOIN PM_PROD_EDW.LATEST.UOM_CONTEXTS ON work_orders.item_id = uom_contexts.item_id
                LEFT JOIN PM_PROD_EDW.LATEST.UOM_RATIOS AS default_uom_ratio ON uom_contexts.default_uom_ratio_id = default_uom_ratio.UOM_RATIO_ID
                LEFT JOIN PM_PROD_EDW.LATEST.UNIT_OF_MEASURES ON unit_of_measures.unit_of_measure_id = default_uom_ratio.unit_of_measure_id
                WHERE
                    jobs.ACTUAL_START_AT >= ''{start_date}''
                    AND jobs.ACTUAL_START_AT <= ''{end_date}''
                    AND jobs.produced_quantity_value_in_reference_uom > 0.99
            """
            ov_turned_on_date_query = f"""
                SELECT
                    companies.company_id as company_id,
                    accounts.account_id as account_id,
                    min(JOBS.actual_start_at) as date_ov_turned_on
                FROM PM_PROD_EDW.LATEST.JOBS
                LEFT JOIN PM_PROD_EDW.LATEST.WORK_ORDERS ON work_orders.work_order_id = jobs.WORK_ORDER_ID
                LEFT JOIN PM_PROD_EDW.LATEST.SITES ON sites.site_id = jobs.site_id
                LEFT JOIN PM_PROD_EDW.LATEST.ACCOUNTS ON accounts.account_id = sites.account_id
                LEFT JOIN PM_PROD_EDW.LATEST.COMPANIES ON companies.company_id = accounts.company_id
                WHERE
                    work_orders.final_charge_per_unit IS NOT NULL
                    AND sites.active = True 
                GROUP BY companies.company_id, accounts.account_id
            """
            ov_uom_ratios_query = """
                SELECT
                    account_id,
                    ratio_to_reference_uom,
                    UOM_RATIO_ID as case_uom_id
                FROM PM_PROD_EDW.LATEST.UOM_RATIOS
            """
            scenario_charges_query = f"""
                WITH
                max_scenario_charges AS (
                    SELECT
                        scenarios.SCENARIO_ID AS scenario_id,
                        MAX(scenario_charges.EFFECTIVE_DATE_AT) AS max_effective_date_at
                    FROM
                        PM_PROD_EDW.LATEST.SCENARIO_CHARGES
                    JOIN PM_PROD_EDW.LATEST.SCENARIOS ON scenarios.SCENARIO_ID = scenario_charges.scenario_id
                    GROUP BY
                        scenarios.SCENARIO_ID
                ),
                min_scenario_charges AS (
                    SELECT
                        scenarios.SCENARIO_ID AS scenario_id,
                        min(scenario_charges.EFFECTIVE_DATE_AT) AS min_effective_date_at
                    FROM
                        PM_PROD_EDW.LATEST.SCENARIO_CHARGES
                    JOIN PM_PROD_EDW.LATEST.SCENARIOS ON scenarios.SCENARIO_ID = scenario_charges.scenario_id
                    GROUP BY
                        scenarios.SCENARIO_ID
                ),
                smaller AS (
                    SELECT DISTINCT (jobs.job_id),
                        JOBS.JOB_ID AS job_id,
                        scenario_charges.scenario_charge_id AS scenario_charges_id
                    FROM
                        PM_PROD_EDW.LATEST.JOBS
                    LEFT JOIN PM_PROD_EDW.LATEST.WORK_ORDERS ON jobs.work_order_id = WORK_ORDERS.work_order_id
                    left join max_scenario_charges on max_scenario_charges.scenario_id = work_orders.scenario_id
                    LEFT JOIN PM_PROD_EDW.LATEST.SCENARIO_CHARGES ON
                        max_scenario_charges.scenario_id = scenario_charges.scenario_id
                        AND scenario_charges.effective_date_at = max_scenario_charges.max_effective_date_at
                    WHERE jobs.produced_quantity_value_in_reference_uom > 0
                        AND jobs.ACTUAL_START_AT >= ''{start_date}''
                        AND jobs.ACTUAL_START_AT <= ''{end_date}''
                ),
                larger AS (
                    SELECT DISTINCT (jobs.job_id),
                        JOBS.JOB_ID AS job_id,
                        scenario_charges.scenario_charge_id AS scenario_charges_id
                    FROM
                        PM_PROD_EDW.LATEST.JOBS
                    LEFT JOIN PM_PROD_EDW.LATEST.WORK_ORDERS ON jobs.work_order_id = WORK_ORDERS.work_order_id
                    left join min_scenario_charges on min_scenario_charges.scenario_id = work_orders.scenario_id
                    LEFT JOIN PM_PROD_EDW.LATEST.SCENARIO_CHARGES ON
                        min_scenario_charges.scenario_id = scenario_charges.scenario_id
                        AND scenario_charges.effective_date_at = min_scenario_charges.min_effective_date_at
                    WHERE jobs.produced_quantity_value_in_reference_uom > 0
                        AND jobs.ACTUAL_START_AT >= ''{start_date}''
                        AND jobs.ACTUAL_START_AT <= ''{end_date}''
                ),
                correct_scenario_charges AS (
                    SELECT
                        job_id,
                        COALESCE(smaller.scenario_charges_id, larger.scenario_charges_id) AS scenario_charges_id
                    FROM
                        smaller
                    FULL OUTER JOIN larger USING (job_id)
                ),
                custom_charges AS (
                    SELECT
                        cpuc.scenario_charge_id,
                        SUM(cpuc.cost_per_unit) AS custom_sum_cost_per_unit,
                        SUM(cpuc.charge_per_unit) AS custom_sum_charge_per_unit,
                        SUM(cpuc.markup_per_unit) AS custom_sum_markup_per_unit,
                        AVG(cpuc.markup_percentage) AS custom_avg_markup_percentage,
                        AVG(cpuc.percentage) AS custom_avg_percentage
                    FROM
                        PM_PROD_EDW.LATEST.CUSTOM_PER_UNIT_CHARGES AS cpuc
                    GROUP BY
                        cpuc.scenario_charge_id
                )
                SELECT
                    accounts.account_id AS account_id,
                    j.site_id,
                    j.job_id AS job_id,
                    scenario_charges.scenario_charge_id AS scenario_charges_id,
                    scenario_charges.effective_date_at,
                    scenario_charges.labour_charge_per_unit,
                    scenario_charges.materials_charge_per_unit,
                    scenario_charges.overhead_charge_per_unit,
                    scenario_charges.overridden_charge_per_unit,
                    custom_charges.custom_sum_charge_per_unit,
                    scenarios.unit_of_measure_id AS scen_uom,
                    unit_of_measures.label AS scen_uom_label
                FROM
                    PM_PROD_EDW.LATEST.JOBS AS j
                LEFT JOIN correct_scenario_charges ON j.job_id = correct_scenario_charges.job_id
                LEFT JOIN PM_PROD_EDW.LATEST.SCENARIO_CHARGES ON scenario_charges.scenario_charge_id = correct_scenario_charges.scenario_charges_id
                LEFT JOIN PM_PROD_EDW.LATEST.SCENARIOS ON scenario_charges.scenario_id = SCENARIOS.scenario_id
                LEFT JOIN PM_PROD_EDW.LATEST.UNIT_OF_MEASURES ON scenarios.unit_of_measure_id = UNIT_OF_MEASURES.unit_of_measure_id
                LEFT JOIN custom_charges ON custom_charges.scenario_charge_id = correct_scenario_charges.scenario_charges_id
                LEFT JOIN PM_PROD_EDW.LATEST.SITES ON sites.site_id = j.site_id
                LEFT JOIN PM_PROD_EDW.LATEST.ACCOUNTS ON ACCOUNTS.account_id = sites.account_id
                WHERE
                    j.ACTUAL_START_AT >= ''{start_date}''
                    AND j.ACTUAL_START_AT <= ''{end_date}''
                    AND j.produced_quantity_value_in_reference_uom > 0
                    AND scenario_charges.scenario_charge_id IS NOT NULL
            """
            # Pull data using the ov_data_pull function
            raw_ov_data = ov_data_pull(session, active_sites_query, ov_job_query, ov_turned_on_date_query, ov_uom_ratios_query, scenario_charges_query)
            # Perform OV calculations
            result_df = ov(
                raw_ov_data_dict=raw_ov_data,
                start_date=start_date,
                end_date=end_date
            )
            def convert_to_datetime(value):
                try:
                    # Try to convert value to datetime, assuming it''s already in a valid format
                    return pd.to_datetime(value, errors=''coerce'')
                except Exception:
                    # If conversion fails, assume it''s an epoch timestamp in nanoseconds
                    try:
                        return pd.to_datetime(value, unit=''ns'', errors=''coerce'')
                    except Exception:
                        return pd.NaT
            date_columns = [''START_DATE'', ''END_DATE'',''DATE_OV_TURNED_ON'']
            for col in date_columns:
                result_df[col] = result_df[col].apply(convert_to_datetime)
            # Add updated_at timestamp
            result_df[''UPDATED_AT''] = pd.to_datetime(datetime.utcnow())
            # Convert percentage columns to numeric
            percentage_columns = [
                ''PERCENT_FCPU'', ''PERCENT_OVERRIDDEN'', ''PERCENT_SUM_SCENARIO'', 
                ''PERCENT_SUM_SCENARIO_WO_CUSTOM'', ''PERCENT_NO_CHARGES'', 
                ''PERCENT_FILL_RATE'', ''PERCENT_OV_MATERIALS'', ''PERCENT_OUTLIERS''
            ]
            for col in percentage_columns:
                result_df[col] = result_df[col].str.rstrip(''%'').astype(''float'')
            # Convert dollar value columns to numeric
            dollar_columns = [''TOTAL_FCPU'', ''OV_W_OUTLIERS'', ''OV_WO_OUTLIERS'', ''ADJUSTED_OV_WO_OUTLIERS'']
            for col in dollar_columns:
                result_df[col] = result_df[col].str.replace(''$'', '''').str.replace('','', '''').astype(''float'')
            # Convert other numeric columns to proper types
            numeric_columns = [''TOTAL_NUMBER_OF_PROJECTS'', ''TOTAL_NUM_JOBS''] + percentage_columns + dollar_columns
            for col in numeric_columns:
                result_df[col] = pd.to_numeric(result_df[col], errors=''coerce'')
            # Ensure all columns are properly typed and handle non-numeric values
            for col in result_df.columns:
                if result_df[col].dtype == ''object'' and col not in [''COMPANY_ID'', ''ACCOUNT_ID'', ''SITE_ID'']:
                    result_df[col] = pd.to_numeric(result_df[col], errors=''coerce'')
            for col in date_columns + [''UPDATED_AT'']:
                result_df[col] = result_df[col].dt.strftime(''%Y-%m-%d %H:%M:%S'')
            # Convert DataFrame to Snowflake DataFrame and append to table
            session.write_pandas(result_df, "ORDER_VALUE", auto_create_table=False, mode="append", schema="PRODUCTION")
        return "Data refresh and insertion complete"
    return main(session)
';