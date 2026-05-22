from __future__ import annotations

import os
from pathlib import Path
from typing import Final

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import Session, sessionmaker

from robot_dh.registry.schema import Base

DEFAULT_DB_PATH: Final[Path] = Path(".robot_dh/robot_dh.db")
SUPPORTED_DB_DRIVERS: Final[set[str]] = {"sqlite", "postgresql+psycopg"}

_ENGINE_CACHE: dict[str, Engine] = {}
_SESSION_FACTORY_CACHE: dict[str, sessionmaker[Session]] = {}


def resolve_db_uri(db_uri: str | None = None) -> str:
    uri = db_uri or os.environ.get("ROBOT_DH_DB_URI")
    if uri is None or uri.strip() == "":
        return f"sqlite:///{DEFAULT_DB_PATH.as_posix()}"

    url = make_url(uri)
    if url.drivername not in SUPPORTED_DB_DRIVERS:
        raise ValueError(f"Unsupported database URI scheme: {uri}")
    return uri


def get_db_backend(db_uri: str | None = None) -> str:
    return make_url(resolve_db_uri(db_uri)).get_backend_name()


def resolve_db_path(db_uri: str | None = None) -> Path:
    url = make_url(resolve_db_uri(db_uri))
    if url.get_backend_name() != "sqlite":
        raise ValueError("Database path is only available for SQLite backends")

    database = url.database or DEFAULT_DB_PATH.as_posix()
    if database == ":memory:":
        return Path(database)

    path = Path(database).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _normalized_engine_uri(db_uri: str | None = None) -> str:
    url = make_url(resolve_db_uri(db_uri))
    if url.get_backend_name() != "sqlite":
        return url.render_as_string(hide_password=False)

    database = url.database or DEFAULT_DB_PATH.as_posix()
    if database == ":memory:":
        return "sqlite:///:memory:"

    path = resolve_db_path(str(url))
    return str(URL.create(drivername="sqlite", database=path.as_posix()))


def _configure_sqlite(engine: Engine) -> None:
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def get_engine(db_uri: str | None = None) -> Engine:
    engine_uri = _normalized_engine_uri(db_uri)
    cached = _ENGINE_CACHE.get(engine_uri)
    if cached is not None:
        return cached

    backend = make_url(engine_uri).get_backend_name()
    engine = create_engine(
        engine_uri,
        future=True,
        pool_pre_ping=backend == "postgresql",
        connect_args={"check_same_thread": False} if backend == "sqlite" else {},
    )
    if backend == "sqlite":
        _configure_sqlite(engine)
    _ENGINE_CACHE[engine_uri] = engine
    return engine


def init_db(db_uri: str | None = None) -> None:
    Base.metadata.create_all(get_engine(db_uri))


def get_session(db_uri: str | None = None) -> Session:
    engine_uri = _normalized_engine_uri(db_uri)
    init_db(engine_uri)
    factory = _SESSION_FACTORY_CACHE.get(engine_uri)
    if factory is None:
        factory = sessionmaker(bind=get_engine(engine_uri), expire_on_commit=False, future=True)
        _SESSION_FACTORY_CACHE[engine_uri] = factory
    return factory()
