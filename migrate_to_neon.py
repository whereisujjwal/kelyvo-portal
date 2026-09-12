import os
import json

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine,
    MetaData,
    select,
    text,
    Boolean,
)
from sqlalchemy.dialects.postgresql import JSON, JSONB


# Load DATABASE_URL from .env
load_dotenv()

SQLITE_URL = "sqlite:///./kelyvo.db"
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is missing. Make sure your .env file contains your Neon DATABASE_URL."
    )

if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgresql://",
        "postgresql+psycopg2://",
        1,
    )


# Connections
sqlite_engine = create_engine(SQLITE_URL)
postgres_engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)


# Reflect both databases
sqlite_meta = MetaData()
sqlite_meta.reflect(bind=sqlite_engine)

postgres_meta = MetaData()
postgres_meta.reflect(bind=postgres_engine)


sqlite_tables = list(sqlite_meta.sorted_tables)

print("=" * 60)
print("KELYVO — SQLite → Neon Migration")
print("=" * 60)
print(f"SQLite tables found:   {len(sqlite_tables)}")
print(f"Neon tables found:     {len(postgres_meta.tables)}")
print()


# ------------------------------------------------------------------
# 1. Check that all SQLite tables exist in Neon
# ------------------------------------------------------------------

missing_tables = [
    table.name
    for table in sqlite_tables
    if table.name not in postgres_meta.tables
]

if missing_tables:
    print("ERROR: These SQLite tables are missing from Neon:")
    for table_name in missing_tables:
        print(f"  - {table_name}")

    raise RuntimeError(
        "Migration stopped. Neon schema does not contain all SQLite tables."
    )


# ------------------------------------------------------------------
# 2. Read all SQLite data BEFORE touching Neon
# ------------------------------------------------------------------

print("Reading local SQLite data...")
print()

all_data = {}

with sqlite_engine.connect() as sqlite_conn:
    for sqlite_table in sqlite_tables:
        rows = sqlite_conn.execute(
            select(sqlite_table)
        ).mappings().all()

        all_data[sqlite_table.name] = rows

        print(f"{sqlite_table.name}: {len(rows)} rows")


print()
print("Local SQLite data loaded successfully.")
print()


# ------------------------------------------------------------------
# 3. Replace Neon data inside ONE transaction
# ------------------------------------------------------------------

print("Preparing Neon database...")
print("Existing Neon rows will be replaced by the local SQLite data.")
print()

with postgres_engine.begin() as pg_conn:

    # Disable FK enforcement for the transaction where supported,
    # then truncate everything using CASCADE.
    #
    # TRUNCATE ... CASCADE is much safer than deleting tables one by one
    # because the database contains many foreign-key relationships.

    neon_table_names = [
        table.name
        for table in postgres_meta.sorted_tables
    ]

    if neon_table_names:
        quoted_tables = ", ".join(
            f'"{name}"'
            for name in neon_table_names
        )

        pg_conn.execute(
            text(
                f"TRUNCATE TABLE {quoted_tables} "
                "RESTART IDENTITY CASCADE"
            )
        )

        print("Existing Neon data cleared safely.")
        print()

    # --------------------------------------------------------------
    # Insert tables in dependency order
    # --------------------------------------------------------------

    for sqlite_table in sqlite_tables:
        table_name = sqlite_table.name
        pg_table = postgres_meta.tables[table_name]

        rows = all_data[table_name]

        if not rows:
            print(f"{table_name}: 0 rows")
            continue

        # Only copy columns that exist in both databases.
        columns = [
            column.name
            for column in sqlite_table.columns
            if column.name in pg_table.columns
        ]

        data = []

        for row in rows:
            item = {}

            for column_name in columns:
                value = row[column_name]
                pg_column = pg_table.columns[column_name]

                # --------------------------------------------------
                # Boolean conversion
                # --------------------------------------------------

                if isinstance(pg_column.type, Boolean):
                    if value is not None:
                        value = bool(value)

                # --------------------------------------------------
                # JSON / JSONB conversion
                # --------------------------------------------------

                elif isinstance(pg_column.type, (JSON, JSONB)):
                    if isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except (ValueError, TypeError):
                            # Keep plain string if it is not valid JSON.
                            pass

                item[column_name] = value

            data.append(item)

        pg_conn.execute(
            pg_table.insert(),
            data,
        )

        print(f"{table_name}: {len(data)} rows migrated")

print()
print("Data migration completed successfully.")
print()


# ------------------------------------------------------------------
# 4. Reset PostgreSQL sequences
# ------------------------------------------------------------------

print("Updating PostgreSQL sequences...")

with postgres_engine.begin() as pg_conn:

    for table in postgres_meta.sorted_tables:

        for column in table.primary_key.columns:

            # Only integer auto-increment primary keys need sequence reset.
            if not column.autoincrement:
                continue

            if "INTEGER" not in str(column.type).upper():
                continue

            try:
                max_id = pg_conn.execute(
                    text(
                        f'SELECT MAX("{column.name}") '
                        f'FROM "{table.name}"'
                    )
                ).scalar()

                if max_id is None:
                    continue

                sequence_name = pg_conn.execute(
                    text(
                        "SELECT pg_get_serial_sequence("
                        ":table_name, :column_name)"
                    ),
                    {
                        "table_name": table.name,
                        "column_name": column.name,
                    },
                ).scalar()

                if sequence_name:
                    pg_conn.execute(
                        text(
                            "SELECT setval("
                            ":sequence_name, "
                            ":max_id, "
                            "true)"
                        ),
                        {
                            "sequence_name": sequence_name,
                            "max_id": max_id,
                        },
                    )

            except Exception as exc:
                print(
                    f"Sequence warning for "
                    f"{table.name}.{column.name}: {exc}"
                )


# ------------------------------------------------------------------
# 5. Verification
# ------------------------------------------------------------------

print()
print("=" * 60)
print("Migration verification")
print("=" * 60)

with postgres_engine.connect() as pg_conn:

    total_source_rows = 0
    total_neon_rows = 0

    for sqlite_table in sqlite_tables:
        table_name = sqlite_table.name

        source_count = len(all_data[table_name])

        neon_count = pg_conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{table_name}"'
            )
        ).scalar()

        total_source_rows += source_count
        total_neon_rows += neon_count

        status = "OK" if source_count == neon_count else "MISMATCH"

        print(
            f"{status}: {table_name} "
            f"(SQLite={source_count}, Neon={neon_count})"
        )

print()
print(
    f"Total SQLite rows: {total_source_rows}"
)
print(
    f"Total Neon rows:   {total_neon_rows}"
)

if total_source_rows != total_neon_rows:
    raise RuntimeError(
        "VERIFICATION FAILED: SQLite and Neon row totals do not match."
    )

print()
print("=" * 60)
print("MIGRATION SUCCESSFUL")
print("=" * 60)
print()
print("Your local kelyvo.db was NOT modified.")
print("Neon now contains the migrated SQLite data.")