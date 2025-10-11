"""ODBC connection string strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, Mapping, Sequence

from .secrets import SecretDefinition, SecretManager, SecretRecord


def _stringify(value: object) -> str:
    """Return *value* converted to ``str``."""

    if isinstance(value, str):
        return value
    return str(value)


def _normalize_key(key: str) -> str:
    """Return an uppercase representation of *key* suitable for ODBC."""

    return key.upper()


def _format_value(value: str) -> str:
    """Return *value* formatted for inclusion in a connection string."""

    if not value:
        return value

    if value.startswith("{") and value.endswith("}"):
        return value

    requires_braces = value != value.strip() or any(
        sep in value for sep in (";", "{", "}")
    )
    if requires_braces:
        escaped = value.replace("}", "}}")
        return f"{{{escaped}}}"
    return value


class BaseODBCStrategy(ABC):
    """Base class for ODBC connection string strategies."""

    secret_engine: ClassVar[str] = "ODBC"

    def __init__(
        self,
        *,
        secret_name: str,
        options: Mapping[str, object] | None = None,
    ) -> None:
        self.secret_name = secret_name
        processed: dict[str, str] = {}
        if options is not None:
            for key, value in options.items():
                if not isinstance(key, str):
                    raise TypeError(
                        "Option keys must be strings; "
                        f"received {type(key).__name__}."
                    )
                normalized_key = _normalize_key(key)
                processed[normalized_key] = _stringify(value)
        self._options = processed

    @property
    @abstractmethod
    def driver_fragment(self) -> tuple[str, str]:
        """Return the leading key/value pair for the connection string."""

    @property
    def required_keys(self) -> Sequence[str]:
        """Return the required secret keys for the strategy."""

        return ()

    @property
    def optional_keys(self) -> Sequence[str]:
        """Return optional secret keys that retain ordering in the output."""

        return ()

    @property
    def default_options(self) -> Mapping[str, str]:
        """Return static connection string options supplied by the strategy."""

        return {}

    def definition(
        self,
        *,
        parameters: Mapping[str, object] | None = None,
        **overrides: object,
    ) -> SecretDefinition:
        """Return a :class:`~duckplus.secrets.SecretDefinition` for this strategy."""

        combined: dict[str, object] = {}
        if parameters is not None:
            for key, value in parameters.items():
                combined[key] = value
        for key, value in overrides.items():
            combined[key] = value

        normalized = self._normalize_parameters(combined)
        return SecretDefinition(
            name=self.secret_name,
            engine=self.secret_engine,
            parameters=normalized,
        )

    def register(
        self,
        manager: SecretManager,
        *,
        parameters: Mapping[str, object] | None = None,
        replace: bool = False,
        **overrides: object,
    ) -> SecretRecord:
        """Store the strategy parameters within *manager* and return the record."""

        definition = self.definition(parameters=parameters, **overrides)
        return manager.create_secret(definition, replace=replace)

    def connection_string(self, manager: SecretManager) -> str:
        """Return the ODBC connection string resolved via *manager*."""

        record = manager.get_secret(self.secret_name)
        if record is None:
            raise KeyError(f"Secret not found: {self.secret_name}")

        values = {_normalize_key(key): value for key, value in record.parameters}
        return self._build_connection_string(values)

    def _normalize_parameters(self, provided: Mapping[str, object]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for key, value in provided.items():
            if not isinstance(key, str):
                raise TypeError(
                    "Secret parameters must use string keys; "
                    f"received {type(key).__name__}."
                )
            normalized[_normalize_key(key)] = _stringify(value)

        required = tuple(_normalize_key(key) for key in self.required_keys)
        for key in required:
            if key not in normalized:
                raise KeyError(
                    f"Missing required parameter '{key}' for secret '{self.secret_name}'."
                )

        return normalized

    def _build_connection_string(self, parameters: Mapping[str, str]) -> str:
        values = dict(parameters)
        required_keys = tuple(_normalize_key(key) for key in self.required_keys)
        optional_keys = tuple(_normalize_key(key) for key in self.optional_keys)

        fragments: list[tuple[str, str]] = [self.driver_fragment]

        for key in required_keys:
            try:
                value = values.pop(key)
            except KeyError as exc:
                raise KeyError(
                    f"Secret '{self.secret_name}' is missing required value '{key}'."
                ) from exc
            fragments.append((key, value))

        for key in optional_keys:
            if key in values:
                value = values.pop(key)
                fragments.append((key, value))

        defaults: dict[str, str] = {}
        for key, value in self.default_options.items():
            normalized_key = _normalize_key(key)
            defaults[normalized_key] = _stringify(value)

        for key in defaults:
            if key in required_keys:
                raise ValueError(
                    "Default option conflicts with required key: "
                    f"{key} in {type(self).__name__}."
                )
            values.setdefault(key, defaults[key])

        for key, value in self._options.items():
            if key in required_keys:
                raise ValueError(
                    "Option conflicts with required key: "
                    f"{key} in {type(self).__name__}."
                )
            values[key] = value

        for key in sorted(values):
            fragments.append((key, values[key]))

        return ";".join(f"{key}={_format_value(value)}" for key, value in fragments)


class DriverBasedStrategy(BaseODBCStrategy):
    """Strategy helper that fixes a ``DRIVER`` fragment and key expectations."""

    def __init__(
        self,
        *,
        secret_name: str,
        driver: str,
        required_keys: Sequence[str] = (),
        optional_keys: Sequence[str] = (),
        default_options: Mapping[str, object] | None = None,
        options: Mapping[str, object] | None = None,
    ) -> None:
        self._driver = driver
        self._required = tuple(_normalize_key(key) for key in required_keys)
        self._optional = tuple(_normalize_key(key) for key in optional_keys)
        defaults: dict[str, str] = {}
        if default_options is not None:
            for key, value in default_options.items():
                defaults[_normalize_key(key)] = _stringify(value)
        self._defaults = defaults
        super().__init__(secret_name=secret_name, options=options)

    @property
    def driver_fragment(self) -> tuple[str, str]:
        return ("DRIVER", self._driver)

    @property
    def required_keys(self) -> Sequence[str]:
        return self._required

    @property
    def optional_keys(self) -> Sequence[str]:
        return self._optional

    @property
    def default_options(self) -> Mapping[str, str]:
        return dict(self._defaults)


class IBMiAccessStrategy(BaseODBCStrategy):
    """Strategy for the IBM i Access ODBC driver (AS/400)."""

    def __init__(
        self,
        *,
        secret_name: str,
        library_list: Sequence[str] | None = None,
        naming: str | None = None,
        options: Mapping[str, object] | None = None,
    ) -> None:
        combined: dict[str, object] = {}
        if options is not None:
            combined.update(options)
        if library_list is not None:
            combined["DBQ"] = ",".join(library_list)
        if naming is not None:
            normalized = naming.lower()
            if normalized not in {"sql", "system"}:
                raise ValueError(
                    "naming must be 'sql' or 'system'; "
                    f"received {naming!r}."
                )
            combined["NAM"] = "0" if normalized == "sql" else "1"
        super().__init__(secret_name=secret_name, options=combined)

    @property
    def driver_fragment(self) -> tuple[str, str]:
        return ("DRIVER", "{IBM i Access ODBC Driver}")

    @property
    def required_keys(self) -> Sequence[str]:
        return ("SYSTEM", "UID", "PWD")

    @property
    def optional_keys(self) -> Sequence[str]:
        return ("DATABASE", "DBQ", "LIBL", "CMT", "TRANSLATE", "SIGNON", "SSL")


class SQLServerStrategy(DriverBasedStrategy):
    """Strategy for Microsoft SQL Server ODBC drivers."""

    def __init__(
        self,
        *,
        secret_name: str,
        version: int = 17,
        encrypt: bool | None = None,
        trust_server_certificate: bool | None = None,
        options: Mapping[str, object] | None = None,
    ) -> None:
        if version not in (17, 18):
            raise ValueError("SQL Server driver version must be 17 or 18.")

        defaults: dict[str, object] = {}
        if encrypt is None:
            if version == 18:
                defaults["Encrypt"] = "yes"
        else:
            defaults["Encrypt"] = "yes" if encrypt else "no"

        if trust_server_certificate is not None:
            defaults["TrustServerCertificate"] = (
                "yes" if trust_server_certificate else "no"
            )

        driver = f"{{ODBC Driver {version} for SQL Server}}"
        super().__init__(
            secret_name=secret_name,
            driver=driver,
            required_keys=("SERVER", "DATABASE", "UID", "PWD"),
            optional_keys=("PORT", "APP", "WSID"),
            default_options=defaults,
            options=options,
        )
        self.version = version
        self._encrypt = encrypt
        self._trust_server_certificate = trust_server_certificate


class PostgresStrategy(DriverBasedStrategy):
    """Strategy for PostgreSQL ODBC drivers."""

    def __init__(
        self,
        *,
        secret_name: str,
        driver: str | None = None,
        sslmode: str | None = None,
        options: Mapping[str, object] | None = None,
    ) -> None:
        resolved_driver = driver or "{PostgreSQL Unicode}"
        defaults: dict[str, object] = {}
        if sslmode is not None:
            defaults["SSLMode"] = sslmode

        super().__init__(
            secret_name=secret_name,
            driver=resolved_driver,
            required_keys=("SERVER", "DATABASE", "UID", "PWD"),
            optional_keys=("PORT", "SSLMODE", "APPLICATIONNAME", "CLIENTENCODING"),
            default_options=defaults,
            options=options,
        )
        self.driver = resolved_driver
        self.sslmode = sslmode


class MySQLStrategy(DriverBasedStrategy):
    """Strategy for MySQL ODBC drivers."""

    def __init__(
        self,
        *,
        secret_name: str,
        version: str = "8.0",
        ansi: bool = False,
        ssl_mode: str | None = None,
        charset: str | None = None,
        options: Mapping[str, object] | None = None,
    ) -> None:
        suffix = "ANSI" if ansi else "Unicode"
        driver = f"{{MySQL ODBC {version} {suffix} Driver}}"

        defaults: dict[str, object] = {}
        if ssl_mode is not None:
            defaults["SSLMode"] = ssl_mode
        if charset is not None:
            defaults["Charset"] = charset

        super().__init__(
            secret_name=secret_name,
            driver=driver,
            required_keys=("SERVER", "DATABASE", "UID", "PWD"),
            optional_keys=("PORT", "SSLMODE", "CHARSET", "OPTION"),
            default_options=defaults,
            options=options,
        )
        self.version = version
        self.ansi = ansi
        self.ssl_mode = ssl_mode
        self.charset = charset


class ExcelStrategy(BaseODBCStrategy):
    """Strategy for the Microsoft Excel ODBC driver."""

    def __init__(
        self,
        *,
        secret_name: str,
        read_only: bool | None = None,
        options: Mapping[str, object] | None = None,
    ) -> None:
        combined: dict[str, object] = {}
        if options is not None:
            combined.update(options)
        if read_only is not None:
            combined["ReadOnly"] = "1" if read_only else "0"
        super().__init__(secret_name=secret_name, options=combined)

    @property
    def driver_fragment(self) -> tuple[str, str]:
        return (
            "DRIVER",
            "{Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)}",
        )

    @property
    def required_keys(self) -> Sequence[str]:
        return ("DBQ",)

    @property
    def optional_keys(self) -> Sequence[str]:
        return ("READONLY", "HDR", "IMEX")


class AccessStrategy(BaseODBCStrategy):
    """Strategy for the Microsoft Access ODBC driver."""

    def __init__(
        self,
        *,
        secret_name: str,
        read_only: bool | None = None,
        options: Mapping[str, object] | None = None,
    ) -> None:
        combined: dict[str, object] = {}
        if options is not None:
            combined.update(options)
        if read_only is not None:
            combined["ReadOnly"] = "1" if read_only else "0"
        super().__init__(secret_name=secret_name, options=combined)

    @property
    def driver_fragment(self) -> tuple[str, str]:
        return (
            "DRIVER",
            "{Microsoft Access Driver (*.mdb, *.accdb)}",
        )

    @property
    def required_keys(self) -> Sequence[str]:
        return ("DBQ",)

    @property
    def optional_keys(self) -> Sequence[str]:
        return ("PWD", "READONLY")


class DuckDBDsnStrategy(BaseODBCStrategy):
    """Strategy for the DuckDB ODBC driver using a DSN."""

    def __init__(
        self,
        *,
        secret_name: str,
        dsn: str = "DuckDB",
        options: Mapping[str, object] | None = None,
    ) -> None:
        self._dsn = dsn
        super().__init__(secret_name=secret_name, options=options)

    @property
    def driver_fragment(self) -> tuple[str, str]:
        return ("DSN", self._dsn)

    @property
    def optional_keys(self) -> Sequence[str]:
        return ("DATABASE", "READONLY")


class CustomODBCStrategy(DriverBasedStrategy):
    """Generic strategy for arbitrary ODBC drivers."""

    def __init__(
        self,
        *,
        secret_name: str,
        driver: str,
        required_keys: Sequence[str] = (),
        optional_keys: Sequence[str] = (),
        default_options: Mapping[str, object] | None = None,
        options: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(
            secret_name=secret_name,
            driver=driver,
            required_keys=required_keys,
            optional_keys=optional_keys,
            default_options=default_options,
            options=options,
        )
