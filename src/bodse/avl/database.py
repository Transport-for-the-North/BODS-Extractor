# -*- coding: utf-8 -*-
"""Functionality for storing / retrieving AVL data from a database."""

##### IMPORTS #####
# Standard imports
import logging
import pathlib
import re
from typing import Any, Protocol

# Third party imports
from pydantic import fields
import sqlalchemy
from sqlalchemy import types

# Local imports
from bodse.avl import raw

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
SQLALCHEMY_TYPE_LOOKUP = {
    "str": types.String,
    "int": types.Integer,
    "float": types.Float,
    "dt.datetime": types.DateTime,
}


##### CLASSES #####
class RawAVLDatabase:
    """Manages interactions with an SQlite database to store raw AVL data."""

    _metadata_table_name = "avl_metadata"
    _vehicle_activity_table_name = "vehicle_activity"

    def __init__(self, path: pathlib.Path, overwrite: bool = False) -> None:
        """Initialise database.

        Parameters
        ----------
        path : pathlib.Path
            Path to the database file.
        overwrite : bool, default False
            Whether to delete `path` if it already exists and
            re-create it or just append to existing database.
        """
        self._engine = sqlalchemy.create_engine(f"sqlite:///{path}")
        self._metadata = sqlalchemy.MetaData()

        self._metadata_table = sqlalchemy.Table(
            self._metadata_table_name,
            self._metadata,
            sqlalchemy.Column(
                "id", types.Integer, primary_key=True, autoincrement=True
            ),
            *columns_from_class(raw.AVLMetadata),
        )

        self._vehicle_activity_table = sqlalchemy.Table(
            self._vehicle_activity_table_name,
            self._metadata,
            sqlalchemy.Column("id", types.Integer, primary_key=True),
            sqlalchemy.Column(
                "metadata_id",
                types.Integer,
                sqlalchemy.ForeignKey(f"{self._metadata_table_name}.id"),
                nullable=False,
            ),
            *columns_from_class(raw.VehicleActivity),
        )

        if path.is_file() and overwrite:
            path.unlink()

        if not path.is_file():
            self._metadata.create_all(self._engine)

    @property
    def engine(self) -> sqlalchemy.Engine:
        """Database engine object."""
        return self._engine

    def connect(self) -> sqlalchemy.Connection:
        """Connect to database and return connection."""
        return self._engine.connect()

    def insert_avl_metadata(
        self, conn: sqlalchemy.Connection, metadata: raw.AVLMetadata
    ) -> int:
        """Insert metadata into database.

        Parameters
        ----------
        conn : sqlalchemy.Connection
            Database connection, see `RawAVLDatabase.connect`.
        metadata : raw.AVLMetadata
            Metadata for current run.

        Returns
        -------
        int
            Unique run ID, calculated by adding 1 to the maximum
            ID already in the database.
        """
        stmt = (
            self._metadata_table.select()
            .with_only_columns(self._metadata_table.c.id)
            .order_by(self._metadata_table.c.id.desc())
            .limit(1)
        )
        result = conn.execute(stmt).fetchone()

        if result is None:
            id_ = 0
        else:
            id_ = result.id + 1

        stmt = self._metadata_table.insert().values(id=id_, **metadata.data_dict)
        conn.execute(stmt)
        return id_

    def insert_vehicle_activity(
        self,
        conn: sqlalchemy.Connection,
        activity: raw.VehicleActivity,
        metadata_id: int,
    ) -> None:
        """Insert vehicle activity data into database.

        Parameters
        ----------
        conn : sqlalchemy.Connection
            Connection to database, see `RawAVLDatabase.connect`.
        activity : raw.VehicleActivity
            Vehicle activity data.
        metadata_id : int
            Run ID, see `RawAVLDatabase.insert_avl_metadata`.
        """
        stmt = self._vehicle_activity_table.insert().values(
            metadata_id=metadata_id, **activity.data_dict
        )
        conn.execute(stmt)


class _DataStorage(Protocol):
    @classmethod
    def get_fields(cls) -> list[fields.ModelField]:
        """List of data fields."""


##### FUNCTIONS #####
def columns_from_class(dataclass: _DataStorage) -> list[sqlalchemy.Column]:
    """Determine columns for database table based on dataclass.

    Returns
    -------
    list[sqlalchemy.Column]
        Columns for table with name and field type from `dataclass.get_fields`.
    """
    columns = []

    for field in dataclass.get_fields():
        type_, nullable = type_lookup(field.type)
        col = sqlalchemy.Column(field.name, type_=type_, nullable=nullable)
        columns.append(col)

    return columns


def type_lookup(type_name: str) -> tuple[type[Any], bool]:
    """Convert string name of Python type into sqlalchemy type.

    Parameters
    ----------
    type_name : str
        Name of Python type e.g. 'str'.

    Returns
    -------
    types.TypeEngine[Any]
        SQLAlchemy field type.
    bool
        Whether, or not, the field is nullable (optional).

    Raises
    ------
    KeyError
        If name of Python type isn't in `SQLALCHEMY_TYPE_LOOKUP`.
    """
    match = re.match(r"^(?:(\w+)?\[)?([\w.]+)\]?$", type_name.strip())

    if match is None:
        raise ValueError(f"unexpected type format: '{type_name}'")

    prefix = match.group(1)
    type_ = match.group(2).lower()

    if prefix is not None and prefix.lower() == "optional":
        nullable = True
    else:
        nullable = False

    return SQLALCHEMY_TYPE_LOOKUP[type_], nullable
