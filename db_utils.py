import psycopg2

# Database setup
DB_PATH = 'data.db'
ORGANISATION_TABLE = 'public.organisations'
RECORD_TABLE = 'public.records'

# Replace DB_PATH with connection params
DB_CONFIG = {
    'dbname': 'bro_state_dashboard',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

DB_USER = DB_CONFIG['user']
DB_PASSWORD = DB_CONFIG['password']
DB_HOST = DB_CONFIG['host']
DB_PORT = DB_CONFIG['port']
DB_NAME = DB_CONFIG['dbname']

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


# Helper function to get most recent record per organisation
def get_latest_record():
    query = """
    SELECT o.id, o.name, o.kvk_number, r.GMW, r.GMN, r.GLD, r.GAR, r.FRD, r.time
    FROM public.organisations o
    LEFT JOIN public.record r ON o.id = r.organisation_id
    WHERE r.id IN (
        SELECT id FROM public.record r2
        WHERE r2.organisation_id = o.id
        ORDER BY r2.time DESC
        LIMIT 1
    ) OR r.id IS NULL
    ORDER BY COALESCE(r.GMW, 0) DESC
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# Helper function to get time series data for an organisation
def get_org_time_series(org_id):
    query = """
    SELECT time, GMW, GMN, GLD, GAR, FRD
    FROM public.records
    WHERE organisation_id = %s
    ORDER BY time ASC
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, (org_id,))
    results = cursor.fetchall()
    conn.close()
    return results

# Helper function to get all organisations
def get_all_organisations():
    query = "SELECT name, kvk_number FROM public.organisations ORDER BY name"
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return [{"name": r[0], "kvk": r[1]} for r in results]