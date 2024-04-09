# -*- coding: utf-8 -*-
"""Functionality for storing / retrieving AVL data from a database."""

##### IMPORTS #####

# Built-Ins
import abc
import collections
import logging
import os
import pathlib
import re
import sys
import textwrap
import time
import warnings
from typing import Any, Optional, Protocol

# Third Party
import pandas as pd
import sqlalchemy
from pydantic import fields
from sqlalchemy import sql, types

# Local Imports
from bodse import utils
from bodse.avl import gtfs, raw

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
_SQLALCHEMY_TYPE_LOOKUP: dict[str, type] = {
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
    "incrementality": types.Enum(gtfs.Incrementality),  # type: ignore
    "schedulerelationship": types.Enum(gtfs.ScheduleRelationship),  # type: ignore
    "vehiclestopstatus": types.Enum(gtfs.VehicleStopStatus),  # type: ignore
    "congestionlevel": types.Enum(gtfs.CongestionLevel),  # type: ignore
    "occupancystatus": types.Enum(gtfs.OccupancyStatus),  # type: ignore
    "wheelchairaccessible": types.Enum(gtfs.WheelchairAccessible),  # type: ignore
}

MAX_TO_SQL_CHUNKSIZE = 10_000_000
try:
    MAX_TO_SQL_CHUNKSIZE = int(
        os.getenv(
            "BODSE_MAX_TO_SQL_CHUNKSIZE",
            str(MAX_TO_SQL_CHUNKSIZE),
        )
    )
except ValueError as exc:
    warnings.warn(
        "error with 'BODSE_MAX_TO_SQL_CHUNKSIZE' environment variable:"
        f" {exc}\nusing default value: {MAX_TO_SQL_CHUNKSIZE:,}",
        RuntimeWarning,
    )


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
        self.path = pathlib.Path(path)
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
        if self.path.is_file() and overwrite:
            self.path.unlink()

        if not self.path.is_file():
            self._metadata.create_all(self._engine)

    @property
    def engine(self) -> sqlalchemy.Engine:
        """Database engine object."""
        return self._engine

    def connect(self) -> sqlalchemy.Connection:
        """Connect to database and return connection."""
        return self._engine.connect()

    def table_exists(self, name: str) -> bool:
        """Check if given table exists in database, doesn't check if it contains data."""
        stmt = sql.text(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=:name;"
        )
        with self.connect() as conn:
            result = conn.execute(stmt, {"name": name})
            count: int = result.fetchone()[0]

        if count == 1:
            return True
        if count == 0:
            return False

        raise ValueError(f"count is {count}, which shouldn't be possible")


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
    _speeds_table_name = "gtfs_rt_vehicle_speed_estimates"
    _stop_times_table_name = "gtfs_stop_times"
    _stop_delays_table_name = "gtfs_stop_delays"

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

    # Maximum size of the position hashes set, set is cleared when above this
    _MAX_POSITION_HASHES_SET_SIZE = 10_000_000

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
        # self._trip_table = sqlalchemy.Table(
        #     self._trip_table_name,
        #     self._metadata,
        #     id_column.copy(),
        #     metadata_column.copy(),
        #     feed_id_column.copy(),
        #     *columns_from_class(gtfs.TripDescriptor),
        #     *columns_from_class(
        #         gtfs.VehicleDescriptor, prefix=self._prefixes["vehicle_descriptor"]
        #     ),
        #     *columns_from_class(
        #         gtfs.StopTimeUpdate, exclude_fields=self._excludes.get("stop_time_update")
        #     ),
        #     *columns_from_class(gtfs.StopTimeEvent, prefix=self._prefixes["stop_arrival"]),
        #     *columns_from_class(
        #         gtfs.StopTimeEvent, prefix=self._prefixes["stop_departure"]
        #     ),
        #     *columns_from_class(
        #         gtfs.TripUpdate,
        #         exclude_fields=self._excludes.get("trip_update"),
        #     ),
        # )

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
            sqlalchemy.Column("identifier_hash", types.VARCHAR(64), nullable=False),
        )
        self._position_hashes: set[str] = set()

        self._speeds_table = sqlalchemy.Table(
            self._speeds_table_name,
            self._metadata,
            sqlalchemy.Column(
                "position_id",
                types.Integer,
                sqlalchemy.ForeignKey(f"{self._positions_table_name}.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
                unique=True,
            ),
            sqlalchemy.Column("trip_id", types.String, nullable=False, index=True),
            sqlalchemy.Column("current_stop_sequence", types.Integer, nullable=False),
            sqlalchemy.Column("timestamp", types.DateTime, nullable=False),
            sqlalchemy.Column("current_status", _SQLALCHEMY_TYPE_LOOKUP["vehiclestopstatus"]),
            sqlalchemy.Column("easting", types.Float, nullable=False),
            sqlalchemy.Column("northing", types.Float, nullable=False),
            sqlalchemy.Column("prev_easting", types.Float),
            sqlalchemy.Column("prev_northing", types.Float),
            sqlalchemy.Column("prev_time", types.DateTime),
            sqlalchemy.Column("distance_metres", types.Float),
            sqlalchemy.Column("delta_time_seconds", types.Float),
            sqlalchemy.Column("speed_ms", types.Float),
        )

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
            Connection to database.
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

        if len(feed.updates) > 0:
            warnings.warn(
                f"AVL feed has {len(feed.updates):,} trip updates"
                " which won't be stored in the database and are ignored",
                UserWarning,
            )

        LOG.info("Inserting %s positions into database", len(feed.positions))
        row_count = 0
        for position in feed.positions:
            row_count += self._insert_positions(conn, meta_id, position)
        LOG.debug("Inserted %s new rows into the database", row_count)

        if sys.getsizeof(self._position_hashes) > self._MAX_POSITION_HASHES_SET_SIZE:
            LOG.debug(
                "Clearing position hashes set, current length %s and size %sKB",
                f"{len(self._position_hashes):,}",
                f"{sys.getsizeof(self._position_hashes) / 1000:,.0f}",
            )
            self._position_hashes.clear()

        if len(feed.alerts) > 0:
            warnings.warn(
                f"AVL feed has {len(feed.alerts):,} alerts which"
                " won't be stored in the database and are ignored",
                UserWarning,
            )

    def delete_duplicate_positions(self, conn: sqlalchemy.Connection) -> None:
        """Delete duplicate rows from positions table based on 'identifier_hash' column."""
        table = self._positions_table_name
        LOG.info("Deleting duplicate rows from '%s' table, this may take a few minutes", table)
        start = time.perf_counter()

        stmt = sql.text(
            f"""
            DELETE FROM {table} WHERE ROWID NOT IN
                (
                SELECT min(ROWID) FROM {table}
                GROUP BY identifier_hash
                );
            """
        )
        res = conn.execute(stmt)

        LOG.info(
            "Deleted %s duplicate rows from '%s' table, query took %s",
            res.rowcount,
            table,
            utils.readable_seconds(round(time.perf_counter() - start)),
        )

    def fill_vehicle_speeds_table(self, conn: sqlalchemy.Connection) -> None:
        """Calculate vehicle speeds based and fill in new database table.

        Speeds are calculated based on crow-fly distance from previous
        position and the position timestamps.
        """
        LOG.info(
            "Creating index on '%s' for trip_id, current_stop_sequence, timestamp",
            self._positions_table,
        )
        start = time.perf_counter()

        conn.execute(
            sql.text(
                "CREATE INDEX IF NOT EXISTS"
                f" ix_{self._positions_table_name}_trip_id_current_stop"
                f" ON {self._positions_table_name}"
                " (trip_id, current_stop_sequence, timestamp)"
            )
        )
        conn.execute(
            sql.text(
                "CREATE INDEX IF NOT EXISTS"
                f" ix_{self._positions_table_name}_trip_id_date_timestamp"
                f" ON {self._positions_table_name} (trip_id, date(timestamp), timestamp)"
            )
        )
        LOG.info(
            "Index created in %s", utils.readable_seconds(int(time.perf_counter() - start))
        )

        LOG.info(
            "Calculating vehicle speeds and filling '%s', this may take some time",
            self._speeds_table,
        )
        start = time.perf_counter()
        conn.execute(sql.text("DROP VIEW IF EXISTS gtfs_previous_vehicle_positions"))
        previous_position_stmt = sql.text(
            f"""
            -- Get easting / northing from the previous vehicle position
            CREATE VIEW gtfs_previous_vehicle_positions AS

            SELECT
                id,
                trip_id,
                current_stop_sequence,
                timestamp,
                current_status,
                easting,
                northing,
                lag (easting) OVER ordered_trip prev_easting,
                lag (northing) OVER ordered_trip prev_northing,
                lag (timestamp) OVER ordered_trip prev_time

            FROM {self._positions_table_name}

            WHERE trip_id IS NOT NULL

            WINDOW ordered_trip AS (
                PARTITION BY trip_id, date(timestamp)
                ORDER BY timestamp
            )
            """
        )
        conn.execute(previous_position_stmt)

        select_stmt = """
            SELECT *, distance_metres / delta_time_seconds AS speed_ms

            FROM (
                -- Calculate distance and time from previous row
                SELECT
                    *,
                    sqrt(
                        pow(easting - prev_easting, 2) + pow(northing - prev_northing, 2)
                    ) AS distance_metres,
                    unixepoch (timestamp) - unixepoch (prev_time) AS delta_time_seconds

                FROM gtfs_previous_vehicle_positions
            )

            -- Get the most recent position for each stop in the sequence, per day
            GROUP BY trip_id, current_stop_sequence, date(timestamp)
            ORDER BY trip_id, current_stop_sequence, timestamp DESC;
        """

        result = conn.execute(
            sql.text(f"INSERT OR REPLACE INTO {self._speeds_table_name}\n{select_stmt}")
        )
        LOG.info(
            "Filled %s rows in speeds table in %s",
            f"{result.rowcount:,}",
            utils.readable_seconds(int(time.perf_counter() - start)),
        )

    def _create_stop_times_table(self, conn: sqlalchemy.Connection) -> sqlalchemy.Table:
        """Create table to store GTFS stop times information."""
        name = self._stop_times_table_name
        stops_table = sqlalchemy.Table(
            name,
            self._metadata,
            sqlalchemy.Column("trip_id", types.String, nullable=False),
            sqlalchemy.Column("arrival_time", types.Time, nullable=False),
            sqlalchemy.Column("departure_time", types.Time, nullable=False),
            sqlalchemy.Column("stop_id", types.String, nullable=False),
            sqlalchemy.Column("stop_sequence", types.Integer, nullable=False),
            sqlalchemy.Column("stop_headsign", types.String),
            sqlalchemy.Column("pickup_type", types.Integer),
            sqlalchemy.Column("drop_off_type", types.Integer),
            sqlalchemy.Column("shape_dist_traveled", types.Float),
            sqlalchemy.Column("timepoint", types.Integer),
            sqlalchemy.Column("stop_direction_name", types.String),
            sqlalchemy.Column("stop_east", types.Float),
            sqlalchemy.Column("stop_north", types.Float),
            sqlalchemy.Column("arrival_secs", types.Integer, nullable=False),
            sqlalchemy.Column("departure_secs", types.Integer, nullable=False),
            sqlalchemy.Column("stop_distance_metres", types.Float),
        )

        stops_table.create(bind=conn, checkfirst=True)

        index_stmt = sql.text(
            f"CREATE INDEX IF NOT EXISTS ix_{name}_trip_id_stop_sequence"
            f" ON {name} (trip_id, stop_sequence)"
        )
        conn.execute(index_stmt)

        return stops_table

    def insert_stop_times(
        self, conn: sqlalchemy.Connection, stop_times: pd.DataFrame, overwrite: bool = False
    ) -> None:
        """Insert GTFS stop times data into database.

        Parameters
        ----------
        stop_times : pd.DataFrame
            Stop times data as defined in
            [GTFS spec](https://gtfs.org/schedule/reference/#stop_timestxt)
            with 'stop_east' and 'stop_north' columns appended containing
            the BNG easting and northing values respectively.
        overwrite : bool, default False
            If True, clears the stop times table and inserts new values.
            If False, checks if the stop times table already exists with same
            number of rows as `stop_times` and doesn't overwrite it if that's
            the case.
        """
        if not overwrite and self.table_exists(self._stop_times_table_name):
            result = conn.execute(
                sql.text(f"SELECT count(*) FROM {self._stop_times_table_name}")
            )
            n_rows: int = result.fetchone()[0]  # type: ignore
            if len(stop_times) == n_rows:
                LOG.info(
                    "Stop times table already exists with same number of rows"
                    " (%s) as given stop times data so it is not overwritten.",
                    f"{n_rows:,}",
                )
                return

        table = self._create_stop_times_table(conn)

        # Clear rows before inserting new ones
        stmt = table.delete()
        result = conn.execute(stmt)
        LOG.info("Cleared %s table, removed %s rows", table.name, f"{result.rowcount:,}")

        LOG.info(
            "Inserting stop times (%s rows) into AVL database table %s",
            f"{len(stop_times):,}",
            table.name,
        )
        start = time.perf_counter()

        if MAX_TO_SQL_CHUNKSIZE is not None and len(stop_times) > MAX_TO_SQL_CHUNKSIZE:
            chunksize = MAX_TO_SQL_CHUNKSIZE
        else:
            chunksize = None

        rowcount = stop_times.to_sql(
            table.name, conn, index=False, if_exists="append", chunksize=chunksize
        )
        LOG.info(
            "Inserted %s rows in %s",
            f"{rowcount:,}",
            utils.readable_seconds(int(time.perf_counter() - start)),
        )

    def filter_vehicle_positions(self, conn: sqlalchemy.Connection) -> None:
        """Remove AVL points where the GPS data may be incorrect.

        The AVL data is removed if it falls into any of the following:
        - High speed (from last AVL position) and far from next stop; or
        - Large absolute delay; or
        - Very large distance from next stop.

        The removed data will be stored in table "gtfs_position_filter".
        """
        # TODO(MB) Add parameters for speed, delay and distance filters
        LOG.info("Removing GTFS-rt vehicle positions with incorrect GPS data")
        filter_table_name = "gtfs_position_filter"
        stmt = sql.text(f"DROP TABLE IF EXISTS {filter_table_name}")
        conn.execute(stmt)

        stmt = sql.text(
            f"""
            -- Table containing positions to be dropped
            CREATE TABLE {filter_table_name} AS
            SELECT
                *,
                estimated_arrival_seconds - arrival_secs AS estimated_delay_secs
            FROM (
                -- Calculate delays and distance from position to next stop for filtering
                SELECT *,
                    datetime(unixepoch(timestamp) + round(dist_to_stop / speed_ms), 'unixepoch')
                        AS estimated_arrival_time,
                    (unixepoch(timestamp) + round(dist_to_stop / speed_ms)) - unixepoch(timestamp, 'start of day')
                        AS estimated_arrival_seconds
                FROM (
                    SELECT
                        spd.position_id, spd.trip_id, spd.current_stop_sequence, spd.timestamp,
                        spd.current_status, spd.easting, spd.northing, spd.speed_ms,
                        stp.arrival_time, stp.departure_time, stp.stop_id, stp.stop_east, stp.stop_north,
                        stp.timepoint, stp.arrival_secs, stp.departure_secs, stp.stop_distance_metres,
                        sqrt(pow(easting - stop_east, 2) + pow(northing - stop_north, 2)) AS dist_to_stop
                    FROM gtfs_rt_vehicle_speed_estimates spd
                        JOIN gtfs_stop_times stp ON spd.trip_id = stp.trip_id AND spd.current_stop_sequence = stp.stop_sequence
                )
            )
            -- Unrealistic speeds and far from stops
            WHERE
                (speed_ms > 35 AND dist_to_stop > 1000)
                OR abs(estimated_delay_secs) > 7200
                OR dist_to_stop > (stop_distance_metres * 10)
            """
        )
        conn.execute(stmt)
        result = conn.execute(sql.text(f"SELECT count() FROM {filter_table_name}"))
        LOG.info(
            "Found %s rows in GTFS-rt vehicle positions table for"
            " deletion due to unrealistically large speeds, delays or"
            " distance from stops suggesting errors in the GPS position.",
            # Mypy false positive flagging not indexable
            f"{result.fetchone()[0]:,}",  # type: ignore
        )

        stmt = sql.text(
            "CREATE INDEX IF NOT EXISTS"
            f" ix_{filter_table_name}_position_id"
            f" ON {filter_table_name} (position_id)"
        )
        conn.execute(stmt)
        LOG.debug("Created index on %s for position_id", filter_table_name)

        # Clearing old calculated speeds
        stmt = self._speeds_table.delete()
        result = conn.execute(stmt)
        LOG.info(
            "Cleared %s table, removed %s rows",
            self._speeds_table.name,
            f"{result.rowcount:,}",
        )

        # Deleting incorrect positions
        stmt = sql.text(
            f"""
            DELETE FROM {self._positions_table_name}
            WHERE id IN (SELECT position_id FROM {filter_table_name})
            """
        )
        result = conn.execute(stmt)
        LOG.info(
            "Removed %s rows from %s table with incorrect GPS data",
            f"{result.rowcount:,}",
            self._positions_table_name,
        )

    def calculate_stop_delays(self, conn: sqlalchemy.Connection) -> None:
        """Calculate estimated delay to stops where AVL data is available.

        Delay estimates are saved to table with name
        `GTFSRTDatabase._stop_delays_table_name`.
        """
        table = self._stop_delays_table_name
        LOG.info("Calculating stop time delays and saving to database table '%s'", table)
        stmt = sql.text(f"DROP TABLE IF EXISTS {table};")
        conn.execute(stmt)
        LOG.debug("Clearing %s table", table)

        stmt = sql.text(
            f"""
            CREATE TABLE {table} AS
                SELECT
                    *,
                    estimated_arrival_seconds - arrival_secs AS estimated_delay_secs
                FROM (
                    SELECT *,
                        datetime(unixepoch(timestamp) + round(stop_dist_metres / speed_ms), 'unixepoch')
                            AS estimated_arrival_time,
                        (unixepoch(timestamp) + round(stop_dist_metres / speed_ms)) - unixepoch(timestamp, 'start of day')
                            AS estimated_arrival_seconds
                    FROM (
                        SELECT
                            spd.position_id, spd.trip_id, spd.current_stop_sequence, spd.timestamp,
                            spd.current_status, spd.easting, spd.northing, spd.speed_ms,
                            stp.arrival_time, stp.departure_time, stp.stop_id, stp.stop_east, stp.stop_north,
                            stp.timepoint, stp.arrival_secs, stp.departure_secs,
                            sqrt(pow(easting - stop_east, 2) + pow(northing - stop_north, 2)) AS stop_dist_metres
                        FROM gtfs_rt_vehicle_speed_estimates spd
                            JOIN gtfs_stop_times stp
                                ON spd.trip_id = stp.trip_id
                                    AND spd.current_stop_sequence = stp.stop_sequence
                    )
                );
            """
        )
        result = conn.execute(stmt)
        LOG.debug(
            "Calculated initial stop delay estimates for %s rows", f"{result.rowcount:,}"
        )

        stmt = sql.text(
            f"""
            UPDATE {table}
            SET estimated_delay_secs = estimated_arrival_seconds - mod(arrival_secs, 86400)
            WHERE abs(estimated_arrival_seconds - mod(arrival_secs, 86400)) < abs(estimated_delay_secs);
            """
        )
        result = conn.execute(stmt)
        LOG.debug(
            "Updated stop delay estimates which cross midnight (%s rows)",
            f"{result.rowcount:,}",
        )

        stmt = sql.text(
            f"""
            UPDATE {table}
            SET
                estimated_arrival_time = timestamp,
                estimated_arrival_seconds = unixepoch(timestamp) - unixepoch(timestamp, 'start of day'),
                estimated_delay_secs = unixepoch(timestamp) - unixepoch(timestamp, 'start of day') - arrival_secs
            WHERE current_status = 'STOPPED_AT';
            """
        )
        result = conn.execute(stmt)
        LOG.debug(
            "Calculated initial stop delay estimates for %s rows", f"{result.rowcount:,}"
        )

    def get_average_stop_delays(
        self, conn: sqlalchemy.Connection
    ) -> tuple[pd.DataFrame, dict[str, str]]:
        """Extract average stop delays for all stops with AVL data.

        Returns
        -------
        pd.DataFrame
            Average stop delays with columns: 'trip_id',
            'current_stop_sequence', 'sample_size' and
            delay columns.
        dict[str, str]
            Lookup from average type (min, max, mean) to
            the delay column name.
        """
        LOG.info("Extracting average stop delays")
        delay_columns = {i: f"{i}_estimated_delay_secs" for i in ("mean", "min", "max")}
        stmt = sql.text(
            f"""
            SELECT 
                trip_id,
                current_stop_sequence,
                count(estimated_delay_secs) AS sample_size,
                min(estimated_delay_secs) AS min_estimated_delay_secs,
                max(estimated_delay_secs) AS max_estimated_delay_secs,
                sum(estimated_delay_secs) / count(estimated_delay_secs)
                    AS mean_estimated_delay_secs
            FROM {self._stop_delays_table_name}
            WHERE estimated_delay_secs IS NOT NULL
            GROUP BY trip_id, current_stop_sequence"""
        )
        result = conn.execute(stmt)
        data = pd.DataFrame(result.fetchall(), columns=list(result.keys()))

        LOG.info("Calculate min, max and mean delays for %s trip stop times", f"{len(data):,}")
        return data, delay_columns

    def get_stop_delays_summary(self, conn: sqlalchemy.Connection) -> pd.DataFrame:
        """Extract a summary of the stop delays at different aggregations.

        Returns
        -------
        pd.DataFrame
            Summary of stop delays with columns: "group",
            "min_estimated_delay_secs", "max_estimated_delay_secs",
            "mean_estimated_delay_secs" and "sample_size".
        """
        LOG.info("Extracting stop delays summary from %s", self._stop_delays_table_name)
        groups = [
            ("all", None),
            ("positive", "estimated_delay_secs > 0"),
            ("negative", "estimated_delay_secs < 0"),
            ("zero", "estimated_delay_secs = 0"),
        ]
        statements = []

        for name, condition in groups:
            stmt = textwrap.dedent(
                f"""
                SELECT
                    '{name}' AS "group",
                    min(estimated_delay_secs) AS min_estimated_delay_secs,
                    max(estimated_delay_secs) AS max_estimated_delay_secs,
                    sum(estimated_delay_secs) / count(estimated_delay_secs)
                        AS mean_estimated_delay_secs,
                    count(estimated_delay_secs) AS sample_size
                FROM {self._stop_delays_table_name}
                WHERE estimated_delay_secs IS NOT NULL
                """
            ).strip()
            if condition is not None:
                stmt += f" AND {condition}"

            statements.append(stmt)

        result = conn.execute(sql.text("\n\nUNION\n\n".join(statements)))
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


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

        type_, nullable = type_lookup(field.type)  # type: ignore
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
