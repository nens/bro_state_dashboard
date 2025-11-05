from fasthtml.common import *
from fastsql import Database as SQLDatabase
import sqlalchemy as sa

from db_utils import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
from db_utils import get_db_connection, get_latest_record, get_all_organisations, get_org_time_series
# The FastHTML app object and shortcut to `app.route`


db = SQLDatabase(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
app, rt = fast_app(static_path='.', templates_path='./templates', debug=True)

# Enums constrain the values accepted for a route parameter
names = str_enum('names', 'Alice', 'Bev', 'Charlie')


@rt
def index():
    return (
        Title("BROSTAR - De staat van de BRO"),
        Link(rel="stylesheet", href="/style.css"),  # reference your CSS file
        Div(
            H1("De staat van de Basisregistratie Ondergrond", cls="title"),
            Button("Grafiek", id="show-graph-btn", cls="btn", onclick="location.href='/graph'"),
            Button("Tabellen", cls="btn", onclick="location.href='/tables'"),
            Table(id="main-table"),
            cls="container",
        )
    )

@rt
def graph():
    orgs = get_all_organisations()
    return (
        Title("BROSTAR - Organisatie Tijdreeks"),
        Link(rel="stylesheet", href="/style.css"),
        Div(
            H1("Tijdreeks per Organisatie", cls="title"),
            Div(
                Input(id="org-select", list="org-list", placeholder="Selecteer een organisatie...", cls="select"),
                Datalist(
                    *[Option(label=o["name"], value=o["kvk"]) for o in orgs],
                    id="org-list"
                ),
                cls="select-container"
            ),
            Div(id="graph-container", cls="graph-container"),
            cls="container"
        )
    )

# ✅ List all tables in the database
@rt("/tables")
def list_tables():
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """
    result = db.execute(sa.text(query))
    tables = [row[0] for row in result.fetchall()]  # row[0] is the actual name

    return (
        Title("Database Tables"),
        Link(rel="stylesheet", href="/style.css"),
        Div(
            H1("Database Tables", cls="title"),
            Button("← Terug naar overzicht", cls="btn", onclick="location.href='/'"),
            Ul(*[
                Li(A(t, href=f"/table/{t}"))
                for t in tables
            ]),
            cls="container",
        )
    )


@rt("/table/{table_name}")
def show_table(table_name: str):
    table_url = list_tables.to()

    try:
        # 1️⃣ Fetch the table’s data
        result = db.execute(sa.text(f'SELECT * FROM "{table_name}";'))
        rows = result.fetchall()
    except Exception as e:
        return Div(f"Error reading table '{table_name}': {e}")

    if not rows:
        return Div(f"No rows found in {table_name}.")

    # 2️⃣ Extract headers
    headers = list(rows[0]._mapping.keys())

    # 3️⃣ Filter out *_id columns
    visible_headers = [h for h in headers if not h.endswith("_id") and h != "id"]

    # 4️⃣ Prepare rows with organization name resolution
    display_rows = []
    for row in rows:
        row_dict = dict(row._mapping)
        display_row = {}

        for h in visible_headers:
            display_row[h] = row_dict[h]

        # Special case: show organisation name instead of organisation_id
        if "organisation_id" in headers:
            org_id = row_dict.get("organisation_id")
            if org_id:
                try:
                    org_name = db.execute(
                        sa.text("SELECT name FROM public.organisations WHERE id = :id"),
                        {"id": org_id}
                    ).scalar()
                    display_row["organisation__name"] = org_name or f"ID {org_id}"
                except Exception:
                    display_row["organisation__name"] = f"ID {org_id}"

        display_rows.append(display_row)

    # Add 'organisation__name' header if it exists
    if any("organisation__name" in r for r in display_rows):
        visible_headers.append("organisation__name")

    # 5️⃣ Render FastHTML table
    return (
        Title(f"Tabel: {table_name}"),
        Link(rel="stylesheet", href="/style.css"),
        Div(
            H1(f"Tabel: {table_name}", cls="title"),
            Button("← Terug naar Tabellen", cls="btn", onclick=f"location.href='{table_url}'"),
            Table(
                Thead(
                    Tr(*[Th(h.replace("_", " ").title()) for h in visible_headers])
                ),
                Tbody(
                    *[
                        Tr(*[Td(str(row.get(h, ""))) for h in visible_headers])
                        for row in display_rows
                    ]
                ),
                cls="data-table",
            ),
            cls="container",
        )
    )



# Run the app
serve()