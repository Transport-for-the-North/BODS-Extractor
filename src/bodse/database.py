# -*- coding: utf-8 -*-
"""Functionality for accessing the PostGreSQL data for storing the timetable data."""

##### IMPORTS #####

# Built-Ins
import dataclasses
import datetime
import enum
import logging
import pathlib
from typing import Optional

# Third Party
import sqlalchemy
from sqlalchemy import orm

##### CONSTANTS #####

LOG = logging.getLogger(__name__)


##### CLASSES & FUNCTIONS #####


@dataclasses.dataclass
class DatabaseConfig:
    """Connection parameters for PostGreSQL database."""

    username: str
    password: str
    host: str
    database: str
    port: int = 5432

    def create_url(self) -> sqlalchemy.URL:
        """Create PostgreSQL database URL from config parameters."""
        return sqlalchemy.URL.create(
            "postgresql",
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query={"application_name": "bodse"},
        )


class _TableBase(orm.DeclarativeBase):
    """Base table for BODSE database."""


class ZoningSystem(_TableBase):
    """Database table containing zoning system definitions."""

    __tablename__ = "zone_type_list"
    __table_args__ = {"schema": "foreign_keys"}

    ID: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    name: orm.Mapped[Optional[str]] = orm.mapped_column(nullable=True)
    description: orm.Mapped[Optional[str]] = orm.mapped_column(nullable=True)
    source: orm.Mapped[Optional[str]] = orm.mapped_column(nullable=True)
    min_longitude: orm.Mapped[Optional[float]] = orm.mapped_column(nullable=True)
    max_longitude: orm.Mapped[Optional[float]] = orm.mapped_column(nullable=True)
    min_latitude: orm.Mapped[Optional[float]] = orm.mapped_column(nullable=True)
    max_latitude: orm.Mapped[Optional[float]] = orm.mapped_column(nullable=True)


class RunMetadata(_TableBase):
    """Database table containing run metadata."""

    __tablename__ = "run_metadata"
    __table_args__ = {"schema": "bus_data"}

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    zone_type_id: orm.Mapped[Optional[int]] = orm.mapped_column(
        sqlalchemy.ForeignKey("foreign_keys.zone_type_list.id", ondelete="CASCADE"),
        nullable=True,
    )
    time_period: orm.Mapped[Optional[str]] = orm.mapped_column(nullable=True)
    mode: orm.Mapped[Optional[str]] = orm.mapped_column(nullable=True)
    modelled_date: orm.Mapped[Optional[datetime.date]] = orm.mapped_column(nullable=True)
    timetable_id: orm.Mapped[Optional[int]] = orm.mapped_column(
        sqlalchemy.ForeignKey("bus_data.timetables.id"), nullable=True
    )
    created_date: orm.Mapped[datetime.datetime] = orm.mapped_column()
    modified: orm.Mapped[datetime.datetime] = orm.mapped_column()
    created_by: orm.Mapped[str] = orm.mapped_column()
    modified_by: orm.Mapped[str] = orm.mapped_column()


class Timetable(_TableBase):
    """Database table storing GTFS timetables."""

    __tablename__ = "timetables"
    __table_args__ = {"schema": "bus_data"}

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    feed_update_time: orm.Mapped[datetime.datetime] = orm.mapped_column(nullable=True)
    timetable_path: orm.Mapped[str] = orm.mapped_column(nullable=False)
    adjusted: orm.Mapped[bool] = orm.mapped_column(nullable=False)
    base_timetable_id: orm.Mapped[Optional[int]] = orm.mapped_column(
        sqlalchemy.ForeignKey("bus_data.timetables.id", ondelete="NO ACTION"), nullable=True
    )
    delay_calculation: orm.Mapped[Optional[str]] = orm.mapped_column(nullable=True)
    created_date: orm.Mapped[datetime.datetime] = orm.mapped_column()
    modified: orm.Mapped[datetime.datetime] = orm.mapped_column()
    created_by: orm.Mapped[str] = orm.mapped_column()
    modified_by: orm.Mapped[str] = orm.mapped_column()

    @property
    def actual_timetable_path(self) -> pathlib.Path:
        """Full resolved Path to the timetable."""
        return pathlib.Path(self.timetable_path).resolve()


class Database:
    """Class for reading / writing to the PostGreSQL database."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.engine = sqlalchemy.create_engine(
            config.create_url(),
        )

        _TableBase.metadata.create_all(self.engine)

    def insert_run_metadata(
        self,
        zoning_systems_id: Optional[int] = None,
        time_period: Optional[str] = None,
        mode: Optional[str] = None,
        modelled_date: Optional[datetime.date] = None,
        timetable_id: Optional[int] = None,
    ) -> int:
        """Insert run metadata into the database table.

        Parameters
        ----------
        zoning_systems_id : int, optional
            ID for the zone system, if relevant.
        time_period : str, optional
            Time period for the model run, if relevant.
        mode : str, optional
            Mode for the model run, if relevant.
        modelled_date : datetime.date, optional
            Date for the model run, e.g. date being modelled.
        timetable_id : int, optional
            Database ID for the timetable used.

        Returns
        -------
        int
            ID from the run metadata table.
        """
        stmt = (
            sqlalchemy.insert(RunMetadata)
            .values(
                zoning_systems_id=zoning_systems_id,
                time_period=time_period,
                mode=mode,
                modelled_date=modelled_date,
                timetable_id=timetable_id,
            )
            .returning(RunMetadata.id)
        )

        with orm.Session(self.engine) as session:
            result = session.execute(stmt)
            id_ = result.one().tuple()[0]
            session.commit()

        LOG.info("Inserted run metadata into database with ID: %s", id_)
        return id_

    def insert_timetable(
        self,
        feed_update_time: Optional[datetime.datetime],
        timetable_path: str,
        adjusted: bool = False,
        base_timetable_id: Optional[int] = None,
        delay_calculation: Optional[str] = None,
    ) -> Timetable:
        """Insert timetable data into database.

        Parameters
        ----------
        feed_update_time : datetime.datetime | None
            Last time the GTFS feed was updated on BODS.
        timetable_path : str
            Path to the timetable GTFS file.
        adjusted : bool, default False
            False if the timetable has been downloaded from BODS.
            True if this timetable has been adjusted, usually
            with the AVL adjustment process.
        base_timetable_id : int, required if `adjusted` is True
            ID (in this database table) of the unadjusted timetable
            used as an input to produce this adjusted timetable.
        delay_calculation : str, optional
            Name of the delay calculation, if timetable is adjusted.

        Returns
        -------
        Timetable
            Read-only view of timetable data stored in the database.
        """
        stmt = (
            sqlalchemy.insert(Timetable)
            .values(
                feed_update_time=feed_update_time,
                timetable_path=timetable_path,
                adjusted=adjusted,
                base_timetable_id=base_timetable_id,
                delay_calculation=delay_calculation,
            )
            .returning(Timetable)
        )

        with orm.Session(self.engine) as session:
            result = session.execute(stmt)
            timetable_data = result.one().tuple()[0]
            session.expunge_all()
            session.commit()

        LOG.info("Inserted GTFS timetable into database with ID: %s", timetable_data.id)
        return timetable_data

    def find_recent_timetable(self, adjusted: bool = False) -> Optional[Timetable]:
        """Find the most recent timetable uploaded to the database.

        Parameters
        ----------
        adjusted : bool, default False
            Whether to look for adjusted or unadjusted timetables.

        Returns
        -------
        Timetable | None
            Read-only view of database row for most recent timetable found,
            or None if no timetables are found.
        """
        stmt = (
            sqlalchemy.select(Timetable)
            .where(Timetable.adjusted == adjusted)
            .order_by(Timetable.created_date.desc())
            .limit(1)
        )

        with orm.Session(self.engine) as session:
            result = session.execute(stmt)
            try:
                timetable = result.one().tuple()[0]
            except sqlalchemy.exc.NoResultFound:
                return None
            session.expunge_all()

        return timetable


def init_sqlalchemy_logging(logger: logging.Logger) -> None:
    """Sets sqlalchemy engine logger to INFO and adds filter to StreamHandlers on `logger`."""
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    for handler in logger.handlers:
        if type(handler) == logging.StreamHandler:  # pylint: disable=unidiomatic-typecheck
            handler.addFilter(lambda x: not x.name.startswith("sqlalchemy"))
