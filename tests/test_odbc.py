from __future__ import annotations

import pytest

from duckplus import (
    AccessStrategy,
    CustomODBCStrategy,
    DuckDBDsnStrategy,
    ExcelStrategy,
    IBMiAccessStrategy,
    MySQLStrategy,
    PostgresStrategy,
    SQLServerStrategy,
    SecretManager,
    SecretRegistry,
    connect,
)


@pytest.fixture()
def registry() -> SecretRegistry:
    return SecretRegistry()


@pytest.fixture()
def manager(registry: SecretRegistry):
    with connect() as conn:
        yield SecretManager(conn, registry=registry, auto_load=False)


def test_sql_server_strategy_registers_and_renders(manager: SecretManager) -> None:
    strategy = SQLServerStrategy(
        secret_name="erp_sql",
        version=18,
        trust_server_certificate=True,
    )

    strategy.register(
        manager,
        SERVER="tcp:sql.example.com,1433",
        DATABASE="erp",
        UID="svc_user",
        PWD="pa;ss",
        PORT=1433,
    )

    connection_string = strategy.connection_string(manager)
    assert (
        connection_string
        == "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=tcp:sql.example.com,1433;"
        "DATABASE=erp;"
        "UID=svc_user;"
        "PWD={pa;ss};"
        "PORT=1433;"
        "ENCRYPT=yes;"
        "TRUSTSERVERCERTIFICATE=yes"
    )


def test_postgres_strategy_supports_sslmode(manager: SecretManager) -> None:
    strategy = PostgresStrategy(secret_name="analytics_pg", sslmode="require")

    strategy.register(
        manager,
        SERVER="pg.example.com",
        DATABASE="analytics",
        UID="svc_user",
        PWD="pg-pass",
        PORT=5432,
    )

    connection_string = strategy.connection_string(manager)
    assert (
        connection_string
        == "DRIVER={PostgreSQL Unicode};"
        "SERVER=pg.example.com;"
        "DATABASE=analytics;"
        "UID=svc_user;"
        "PWD=pg-pass;"
        "PORT=5432;"
        "SSLMODE=require"
    )


def test_mysql_strategy_handles_defaults(manager: SecretManager) -> None:
    strategy = MySQLStrategy(
        secret_name="mysql_reporting",
        ssl_mode="VERIFY_IDENTITY",
        charset="utf8mb4",
    )

    strategy.register(
        manager,
        SERVER="mysql.example.com",
        DATABASE="reporting",
        UID="svc_user",
        PWD="pa;ss",
        PORT=3306,
    )

    connection_string = strategy.connection_string(manager)
    assert (
        connection_string
        == "DRIVER={MySQL ODBC 8.0 Unicode Driver};"
        "SERVER=mysql.example.com;"
        "DATABASE=reporting;"
        "UID=svc_user;"
        "PWD={pa;ss};"
        "PORT=3306;"
        "CHARSET=utf8mb4;"
        "SSLMODE=VERIFY_IDENTITY"
    )


def test_ibmi_strategy_supports_library_and_naming(manager: SecretManager) -> None:
    strategy = IBMiAccessStrategy(
        secret_name="ibmi",
        library_list=["QGPL", "MYLIB"],
        naming="system",
    )

    strategy.register(
        manager,
        SYSTEM="myhost",
        UID="odbc",
        PWD="secret",
    )

    connection_string = strategy.connection_string(manager)
    assert connection_string.startswith(
        "DRIVER={IBM i Access ODBC Driver};SYSTEM=myhost;UID=odbc;PWD=secret;"
    )
    assert "DBQ=QGPL,MYLIB" in connection_string
    assert connection_string.endswith("NAM=1")


def test_excel_strategy_handles_read_only(manager: SecretManager) -> None:
    strategy = ExcelStrategy(secret_name="excel_reports", read_only=True)
    strategy.register(manager, DBQ=r"C:\Reports.xlsx")

    connection_string = strategy.connection_string(manager)
    assert (
        connection_string
        == "DRIVER={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};"
        r"DBQ=C:\Reports.xlsx;"
        "READONLY=1"
    )


def test_access_strategy_handles_password(manager: SecretManager) -> None:
    strategy = AccessStrategy(secret_name="access_db", read_only=False)
    strategy.register(manager, DBQ=r"C:\Data\Northwind.accdb", PWD="password")

    connection_string = strategy.connection_string(manager)
    assert connection_string.startswith(
        "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        r"DBQ=C:\Data\Northwind.accdb;"
        "PWD=password;"
    )
    assert connection_string.endswith("READONLY=0")


def test_duckdb_dsn_strategy(manager: SecretManager) -> None:
    strategy = DuckDBDsnStrategy(secret_name="duckdb_dsn")
    strategy.register(manager, DATABASE="analytics.duckdb")

    connection_string = strategy.connection_string(manager)
    assert connection_string == "DSN=DuckDB;DATABASE=analytics.duckdb"


def test_custom_strategy_supports_generic_driver(manager: SecretManager) -> None:
    strategy = CustomODBCStrategy(
        secret_name="sqlite",
        driver="{SQLite3 ODBC Driver}",
        required_keys=("Database",),
    )

    strategy.register(manager, DATABASE=":memory:")
    connection_string = strategy.connection_string(manager)
    assert connection_string == "DRIVER={SQLite3 ODBC Driver};DATABASE=:memory:"


def test_definition_requires_all_parameters(manager: SecretManager) -> None:
    strategy = SQLServerStrategy(secret_name="missing", version=17)
    with pytest.raises(KeyError):
        strategy.definition(SERVER="host", DATABASE="db", UID="user")

    with pytest.raises(KeyError):
        strategy.connection_string(manager)
