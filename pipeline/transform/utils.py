from typing import Dict

import pandas as pd


def prepare_tables_for_excel(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Split the joined query result into separate tables for Excel output

    Args:
        df: DataFrame output from joining all tables together

    Returns:
        Dictionary mapping sheet names to DataFrames for each table
    """

    if df.empty:
        return {"Empty_Result": pd.DataFrame()}

    # Create separate dataframes for each table
    result = {}

    # Firms table (main table)
    firms_cols = [
        'firm_crd_nb', 'sec_nb', 'business_name', 'full_legal_name',
        'address', 'phone_number', 'employee_count', 'signatory',
        'created_at', 'updated_at'
    ]
    firms_df = df[firms_cols].drop_duplicates(subset=['firm_crd_nb'])
    result['Firms'] = firms_df

    # Compensation arrangements table
    comp_cols = ['firm_crd_nb', 'business_name', 'arrangement']
    comp_df = df[comp_cols].dropna(subset=['arrangement'])
    comp_df = comp_df[comp_df['arrangement'] != 'None'].drop_duplicates()
    if not comp_df.empty:
        result['Compensation_Arrangements'] = comp_df

    # Client types table
    client_cols = ['firm_crd_nb', 'business_name', 'client_type', 'aum_value']
    client_df = df[client_cols].dropna(subset=['client_type'])
    client_df = client_df[client_df['client_type'] != 'None'].drop_duplicates()
    if not client_df.empty:
        result['Client_Types'] = client_df

    # Private funds table
    fund_cols = ['firm_crd_nb', 'business_name', 'fund_name', 'fund_id']
    fund_df = df[fund_cols].dropna(subset=['fund_name'])
    fund_df = fund_df[fund_df['fund_name'] != 'None'].drop_duplicates()
    if not fund_df.empty:
        result['Private_Funds'] = fund_df

    # Generate summary table
    summary_data = []
    for firm_crd in firms_df['firm_crd_nb'].unique():
        firm_name = firms_df[firms_df['firm_crd_nb'] == firm_crd]['business_name'].iloc[0]

        # Count metrics for this firm
        comp_count = len(comp_df[comp_df['firm_crd_nb'] == firm_crd]) if 'Compensation_Arrangements' in result else 0
        client_count = len(client_df[client_df['firm_crd_nb'] == firm_crd]) if 'Client_Types' in result else 0
        fund_count = len(fund_df[fund_df['firm_crd_nb'] == firm_crd]) if 'Private_Funds' in result else 0

        # Calculate total AUM if available
        total_aum = 0
        if 'Client_Types' in result:
            firm_clients = client_df[client_df['firm_crd_nb'] == firm_crd]
            if not firm_clients.empty:
                # Convert to numeric, coercing errors to NaN, then sum
                total_aum = pd.to_numeric(firm_clients['aum_value'], errors='coerce').fillna(0).sum()

        summary_data.append({
            'firm_crd_nb': firm_crd,
            'business_name': firm_name,
            'total_arrangements': comp_count,
            'total_client_types': client_count,
            'total_funds': fund_count,
            'total_aum': total_aum
        })

    if summary_data:
        result['Summary'] = pd.DataFrame(summary_data)

    return result
