import polars as pl
import requests

cur_dir = r"C:\Users\steven.hosper\Desktop\bro_state_dashboard\data"

def get_gmw_ids(kvk_number: str) -> list[str]:
    """Get GMW (Grondwatermonitoring Well) IDs for a given KVK number."""
    try:
        r = requests.get(f'https://publiek.broservices.nl/gm/gmw/v1/bro-ids?bronhouder={kvk_number}')
        r.raise_for_status()
        return r.json().get('broIds', [])
    except requests.RequestException:
        return []

def get_gld_ids(kvk_number: str) -> list[str]:
    """Get GLD (Grondwaterstandonderzoek) IDs for a given KVK number."""
    try:
        r = requests.get(f'https://publiek.broservices.nl/gm/gld/v1/bro-ids?bronhouder={kvk_number}')
        r.raise_for_status()
        return r.json().get('broIds', [])
    except requests.RequestException:
        return []

def get_gmn_ids(kvk_number: str) -> list[str]:
    """Get GMN (Grondwatermeetnet) IDs for a given KVK number."""
    try:
        r = requests.get(f'https://publiek.broservices.nl/gm/gmn/v1/bro-ids?bronhouder={kvk_number}')
        r.raise_for_status()
        return r.json().get('broIds', [])
    except requests.RequestException:
        return []

def get_frd_ids(kvk_number: str) -> list[str]:
    """Get FRD (Formatieweerstandonderzoek) IDs for a given KVK number."""
    try:
        r = requests.get(f'https://publiek.broservices.nl/gm/frd/v1/bro-ids?bronhouder={kvk_number}')
        r.raise_for_status()
        return r.json().get('broIds', [])
    except requests.RequestException:
        return []

def get_gar_ids(kvk_number: str) -> list[str]:
    """Get GAR (Grondwateranalyseresultaat) IDs for a given KVK number."""
    try:
        r = requests.get(f'https://publiek.broservices.nl/gm/gar/v1/bro-ids?bronhouder={kvk_number}')
        r.raise_for_status()
        return r.json().get('broIds', [])
    except requests.RequestException:
        return []

def write_to_db(df: pl.DataFrame, current_time: str = None):
    from db_utils import get_db_connection, ORGANISATION_TABLE, RECORD_TABLE
    conn = get_db_connection()
    cursor = conn.cursor()

    # Insert organisations if they don't exist
    for row in df.iter_rows(named=True):
        # Check if organisation already exists
        cursor.execute(f"""
            SELECT id FROM {ORGANISATION_TABLE}
            WHERE kvk_number = %s
            LIMIT 1
        """, (row['kvk'],))
        existing = cursor.fetchone()

        if not existing:
            # Insert organisation since it doesn't exist
            cursor.execute(f"""
                INSERT INTO {ORGANISATION_TABLE} (name, kvk_number)
                VALUES (%s, %s)
            """, (row['name'], row['kvk']))

    # Insert records
    for row in df.iter_rows(named=True):
        cursor.execute(f"""
            SELECT id FROM {ORGANISATION_TABLE}
            WHERE kvk_number = %s
        """, (row['kvk'],))
        organisation_id = cursor.fetchone()[0]

        cursor.execute(f"""
            INSERT INTO {RECORD_TABLE} (organisation_id, time, gmw, gld, gmn, gar, frd)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (organisation_id, time) DO NOTHING
        """, (
            organisation_id,
            current_time,
            row['GMW'],
            row['GLD'],
            row['GMN'],
            row['GAR'],
            row['FRD'],
        ))

    conn.commit()
    cursor.close()
    conn.close()

def main():
    # Read the Excel file
    df = pl.read_excel(
        r"C:\Users\steven.hosper\Documents\bronhouders.xlsx",
        has_header=True,
        sheet_id=1,
    )
    
    # Assuming there's a column with KVK numbers - adjust column name as needed
    # Replace 'kvk_number' with the actual column name in your DataFrame
    kvk_column = 'kvk'  # Change this to match your actual column name
    
    # Add new columns by applying the functions to each KVK number
    df = df.with_columns([
        pl.col(kvk_column).map_elements(
            lambda kvk: len(get_gmw_ids(str(kvk))), 
            return_dtype=pl.Int64
        ).alias("GMW"),
        pl.col(kvk_column).map_elements(
            lambda kvk: len(get_gld_ids(str(kvk))), 
            return_dtype=pl.Int64
        ).alias("GLD"),
        pl.col(kvk_column).map_elements(
            lambda kvk: len(get_gmn_ids(str(kvk))), 
            return_dtype=pl.Int64
        ).alias("GMN"),
        pl.col(kvk_column).map_elements(
            lambda kvk: len(get_gar_ids(str(kvk))), 
            return_dtype=pl.Int64
        ).alias("GAR"),
        pl.col(kvk_column).map_elements(
            lambda kvk: len(get_frd_ids(str(kvk))), 
            return_dtype=pl.Int64
        ).alias("FRD")
    ])
    
    # Write the updated DataFrame back to Excel
    import datetime
    now = datetime.datetime.now()
    now_str = now.strftime("%Y%m%d")
    df.write_excel(
        f"{cur_dir}\{now_str}_bronhouders.xlsx",
        include_header=True
    )
    
    print("Updated DataFrame:")
    print(df)

    write_to_db(df, current_time=now)

if __name__ == "__main__":
    main()