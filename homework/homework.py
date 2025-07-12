"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerles un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaign_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_date: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - cons_price_idx
    - euribor_three_months



    """

    import os
    import pandas as pd
    import zipfile

    # directories routes
    input_dir = 'files/input'
    output_dir = 'files/output'
    os.makedirs(output_dir, exist_ok=True)

    # read all csv files from zip files
    all_dfs = []

    for filename in os.listdir(input_dir):
        if filename.endswith('.csv.zip'):
            zip_path = os.path.join(input_dir, filename)
            with zipfile.ZipFile(zip_path) as z:
                for csv_file in z.namelist():
                    with z.open(csv_file) as f:
                        df = pd.read_csv(f)
                        all_dfs.append(df)

    # Join all dataframes into one
    data = pd.concat(all_dfs, ignore_index=True)

    # client data cleaning
    client_df = data[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()

    client_df['job'] = client_df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    client_df['education'] = client_df['education'].str.replace('.', '_', regex=False)
    client_df['education'] = client_df['education'].replace('unknown', pd.NA)
    client_df['credit_default'] = client_df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    client_df['mortgage'] = client_df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)

    # campaign data cleaning
    campaign_df = data[['client_id', 'number_contacts', 'contact_duration',
                        'previous_campaign_contacts', 'previous_outcome',
                        'campaign_outcome', 'day', 'month']].copy()

    campaign_df['previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)

    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }

    campaign_df['month'] = campaign_df['month'].str.lower().map(month_map)
    campaign_df['day'] = campaign_df['day'].astype(str).str.zfill(2)
    campaign_df['last_contact_date'] = '2022-' + campaign_df['month'] + '-' + campaign_df['day']

    campaign_df = campaign_df.drop(columns=['day', 'month'])

    # economics data cleaning
    economics_df = data[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()

    # save the cleaned dataframes to csv files
    client_df.to_csv(os.path.join(output_dir, 'client.csv'), index=False)
    campaign_df.to_csv(os.path.join(output_dir, 'campaign.csv'), index=False)
    economics_df.to_csv(os.path.join(output_dir, 'economics.csv'), index=False)

    return

if __name__ == "__main__":
    clean_campaign_data()
