# -*- coding: utf-8 -*-
"""Functionality for storing / retrieving AVL data from a database."""

##### IMPORTS #####
# Standard imports
import abc
import collections
import logging
import pathlib
import re
from typing import Any, Optional, Protocol

# Third party imports
from pydantic import fields
import sqlalchemy
from sqlalchemy import types

# Local imports
from bodse.avl import raw, gtfs

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
_SQLALCHEMY_TYPE_LOOKUP = {
    # Built-in types
    "str": types.String,
    "int": types.Integer,
    "conint": types.Integer,
    "float": types.Float,
    "datetime": types.DateTime,
    "date": types.DateTime,
    "time": types.Time,
    "bool": types.Boolean,
    # Custom enum types
    "incrementality": types.Enum(gtfs.Incrementality),
    "schedulerelationship": types.Enum(gtfs.ScheduleRelationship),
    "vehiclestopstatus": types.Enum(gtfs.VehicleStopStatus),
    "congestionlevel": types.Enum(gtfs.CongestionLevel),
    "occupancystatus": types.Enum(gtfs.OccupancyStatus),
    "wheelchairaccessible": types.Enum(gtfs.WheelchairAccessible),
}


##### CLASSES #####
class _Database(abc.ABC):
    """Base class for managing AVL local SQLite database with sqlalchemy."""

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
        self._path = pathlib.Path(path)
        self._engine = sqlalchemy.create_engine(f"sqlite:///{path}")
        self._metadata = sqlalchemy.MetaData()

        self._init_tables()
        self._create(overwrite)

    @abc.abstractmethod
    def _init_tables(self) -> None:
        """Define database tables, instance variables.

        This method is called before `_create` but after the
        `_engine` and `_metadata` are initialised, in `__init__`.
        """
        raise NotImplementedError("Implemented by child class")

    def _create(self, overwrite: bool = False) -> None:
        """Create database if it doesn't already exist.

        Parameters
        ----------
        overwrite : bool, default False
            Whether to delete database if it already exists.
        """
        if self._path.is_file() and overwrite:
            self._path.unlink()

        if not self._path.is_file():
            self._metadata.create_all(self._engine)

    @property
    def engine(self) -> sqlalchemy.Engine:
        """Database engine object."""
        return self._engine

    def connect(self) -> sqlalchemy.Connection:
        """Connect to database and return connection."""
        return self._engine.connect()


class RawAVLDatabase(_Database):
    """Manages interactions with an SQlite database to store raw AVL data."""

    _metadata_table_name = "avl_metadata"
    _vehicle_activity_table_name = "vehicle_activity"

    def _init_tables(self) -> None:
        # inherited docstring
        self._metadata_table = sqlalchemy.Table(
            self._metadata_table_name,
            self._metadata,
            sqlalchemy.Column("id", types.Integer, primary_key=True, autoincrement=True),
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


class GTFSRTDatabase(_Database):
    """Creates and accesses a database for storing GTFS-rt feed messages."""

    _metadata_table_name = "gtfs_rt_meta"
    _trip_table_name = "gtfs_rt_trip_updates"
    _positions_table_name = "gtfs_rt_vehicle_positions"

    _prefixes = collections.defaultdict(
        lambda: "",
        vehicle_descriptor="vehicle_",
        stop_arrival="arrival_",
        stop_departure="departure_",
    )
    _excludes = {
        "stop_time_update": {"arrival", "departure"},
        "trip_update": {"feed_id", "trip", "vehicle", "stop_time_update"},
        "vehicle_position": {"feed_id", "trip", "vehicle", "position"},
    }

    def _init_tables(self) -> None:
        # inherited docstring
        id_column = sqlalchemy.Column("id", types.Integer, primary_key=True)

        self._metadata_table = sqlalchemy.Table(
            self._metadata_table_name,
            self._metadata,
            id_column,
            *columns_from_class(gtfs.FeedHeader),
        )

        # Define columns with same names and types across both tables
        metadata_column = sqlalchemy.Column(
            "metadata_id",
            types.Integer,
            sqlalchemy.ForeignKey(f"{self._metadata_table_name}.id"),
            nullable=False,
            index=True,
        )
        feed_id_column = sqlalchemy.Column("feed_id", types.String, nullable=False)

        # Not yet implemented functionality to store trip updates in the database
        if False:
            self._trip_table = sqlalchemy.Table(
                self._trip_table_name,
                self._metadata,
                id_column.copy(),
                metadata_column.copy(),
                feed_id_column.copy(),
                *columns_from_class(gtfs.TripDescriptor),
                *columns_from_class(
                    gtfs.VehicleDescriptor, prefix=self._prefixes["vehicle_descriptor"]
                ),
                *columns_from_class(
                    gtfs.StopTimeUpdate, exclude_fields=self._excludes.get("stop_time_update")
                ),
                *columns_from_class(gtfs.StopTimeEvent, prefix=self._prefixes["stop_arrival"]),
                *columns_from_class(
                    gtfs.StopTimeEvent, prefix=self._prefixes["stop_departure"]
                ),
                *columns_from_class(
                    gtfs.TripUpdate,
                    exclude_fields=self._excludes.get("trip_update"),
                ),
            )

        self._positions_table = sqlalchemy.Table(
            self._positions_table_name,
            self._metadata,
            id_column.copy(),
            metadata_column.copy(),
            feed_id_column.copy(),
            *columns_from_class(gtfs.TripDescriptor),
            *columns_from_class(
                gtfs.VehicleDescriptor, prefix=self._prefixes["vehicle_descriptor"]
            ),
            *columns_from_class(gtfs.Position),
            *columns_from_class(
                gtfs.VehiclePosition, exclude_fields=self._excludes.get("vehicle_position")
            ),
            sqlalchemy.Column("identifier_hash", types.String, nullable=False, unique=True),
        )
        self._position_hashes: set[str] = set()

        self._alerts_table = NotImplemented

    def _get_max_metadata_id(self, conn: sqlalchemy.Connection) -> int:
        stmt = (
            self._metadata_table.select()
            .with_only_columns(self._metadata_table.c.id)
            .order_by(self._metadata_table.c.id.desc())
            .limit(1)
        )
        result = conn.execute(stmt).fetchone()

        if result is None:
            return 0
        return result.id + 1

    def _insert_positions(
        self, conn: sqlalchemy.Connection, metadata_id: int, position: gtfs.VehiclePosition
    ) -> int:
        """Insert vehicle position data into `_positions_table`.

        This executes the query but does not commit.
        """
        position_hash_fields = [
            "trip",
            "vehicle",
            "position",
            "current_stop_sequence",
            "stop_id",
            "current_status",
            "timestamp",
            "position_delay_seconds",
            "congestion_level",
            "occupancy_status",
            "is_deleted",
        ]
        identifier_hash = position.sha256(position_hash_fields)

        # Don't add identifiers which have already been added
        if identifier_hash in self._position_hashes:
            return 0

        stmt = (
            self._positions_table.insert()
            .values(
                metadata_id=metadata_id,
                feed_id=position.feed_id,
                **values_from_class(position.trip),
                **values_from_class(
                    position.vehicle, prefix=self._prefixes["vehicle_descriptor"]
                ),
                **values_from_class(position.position),
                **values_from_class(
                    position, exclude_fields=self._excludes.get("vehicle_position")
                ),
                identifier_hash=identifier_hash,
            )
            .prefix_with("OR IGNORE")
        )

        res = conn.execute(stmt)
        self._position_hashes.add(identifier_hash)
        return res.rowcount

    def insert_feed(self, conn: sqlalchemy.Connection, feed: gtfs.FeedMessage) -> None:
        """Insert data from GTFS-rt feed into database.

        This executes the queries but does not commit.

        Parameters
        ----------
        conn : sqlalchemy.Connection
            Connection to SQLite database.
        feed : gtfs.FeedMessage
            GTFS-rt feed message data.

        Raises
        ------
        NotImplementedError
            If any trip updates or alerts are provided as
            functionality for storing these in the database
            hasn't been implemented.
        """
        meta_id = self._get_max_metadata_id(conn)
        LOG.info("Inserting feed into database with ID: %s", meta_id)

        stmt = self._metadata_table.insert().values(
            id=meta_id,
            timestamp=feed.header.timestamp,
            gtfs_realtime_version=feed.header.gtfs_realtime_version,
            incrementality=feed.header.incrementality,
        )
        conn.execute(stmt)

        for _ in feed.updates:
            raise NotImplementedError("functionality for storing trip updates in the database")

        LOG.info("Inserting %s positions into database", len(feed.positions))
        row_count = 0
        for position in feed.positions:
            row_count += self._insert_positions(conn, meta_id, position)
        LOG.debug("Inserted %s new rows into the database", row_count)

        for _ in feed.alerts:
            raise NotImplementedError("functionality for storing alerts in the database")

    def create_indices(self) -> None:
        # TODO(MB) Add indices to database to speed up future queries
        raise NotImplementedError("WIP!")


class _DataStorage(Protocol):
    @classmethod
    def get_fields(cls) -> list[fields.ModelField]:
        """List of data fields."""


##### FUNCTIONS #####
def columns_from_class(
    dataclass: _DataStorage, prefix: str = "", exclude_fields: Optional[set[str]] = None
) -> list[sqlalchemy.Column]:
    """Determine columns for database table based on dataclass.

    Parameters
    ----------
    dataclass : _DataStorage
        Class to get data from, should have `get_fields` method.
    prefix : str, default ""
        Prefix to prepend to the attributes to get column names.
    exclude_fields : set[str], optional
        Names of any attributes in `dataclass` to exclude from output.

    Returns
    -------
    list[sqlalchemy.Column]
        Columns for table with name and field type from `dataclass.get_fields`.
    """
    columns = []

    for field in dataclass.get_fields():
        if exclude_fields is not None and field.name in exclude_fields:
            continue

        type_, nullable = type_lookup(field.type)
        col = sqlalchemy.Column(prefix + field.name, type_=type_, nullable=nullable)
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
    match = re.match(
        r"^(?:(\w+)?\[)?"  # Collection / typing e.g. list, dict, Union, Optional
        r"(\w+\.)?"  # Module name, can't handle multiple modules
        r"(\w+)"  # Type name
        r"(\(.*\))?"  # Optional brackets for pydantic's callable types e.g. conint(ge=0)
        r"\]?$",  # Optional closing bracket ']'
        type_name.strip(),
    )

    if match is None:
        raise ValueError(f"unexpected type format: '{type_name}'")

    prefix = match.group(1)
    type_ = match.group(3).lower()

    nullable = prefix is not None and prefix.lower() == "optional"

    return _SQLALCHEMY_TYPE_LOOKUP[type_], nullable


def values_from_class(
    dataclass: Optional[_DataStorage],
    prefix: str = "",
    exclude_fields: Optional[set[str]] = None,
) -> dict[str, Any]:
    """Get attributes and values from `dataclass`.

    Parameters
    ----------
    dataclass : _DataStorage
        Class to get data from, should have `get_fields` method.
        If None, an empty dictionary is returned.
    prefix : str, default ""
        Prefix to prepend to the attribute names in the dictionary.
    exclude_fields : set[str], optional
        Names of any attributes in `dataclass` to exclude from output.

    Returns
    -------
    dict[str, Any]
        Attribute value pairs, with possible prefix on the keys.

    Raises
    ------
    TypeError
        If `dataclass` is a type rather than an instance.
    """
    data: dict[str, Any] = {}
    if dataclass is None:
        return data
    if isinstance(dataclass, type):
        raise TypeError(f"expected instance of _DataStorage not {type(dataclass)}")

    for field in dataclass.get_fields():
        if exclude_fields is not None and field.name in exclude_fields:
            continue

        data[prefix + field.name] = getattr(dataclass, field.name)

    return data
