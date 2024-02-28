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
        return sqlalchemy.URL.create(
            "postgresql",
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query={"application_name": "bodse"},
        )


class ModelName(enum.Enum):
    """Name of the possible model results stored in database."""

    BODSE_SCHEDULED = "bodse_scheduled"
    BODSE_AVL = "bodse_avl"
    BODSE_ADJUSTED = "bodse_adjusted"
    OTP4GB = "otp4gb"
    BUS_ANALYTICS = "bus_analytics"


class _TableBase(orm.DeclarativeBase):
    """Base table for BODSE database."""


class ZoningSystem(_TableBase):
    """Database table containing zoning system definitions."""

    __tablename__ = "zoning_systems"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
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

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    zone_system_id: orm.Mapped[Optional[int]] = orm.mapped_column(
        sqlalchemy.ForeignKey("zoning_systems.id"), nullable=True
    )
    model_name: orm.Mapped[ModelName] = orm.mapped_column(
        sqlalchemy.Enum(ModelName), nullable=False
    )
    start_datetime: orm.Mapped[datetime.datetime] = orm.mapped_column(nullable=False)
    end_datetime: orm.Mapped[datetime.datetime] = orm.mapped_column(nullable=False)
    parameters: orm.Mapped[str] = orm.mapped_column(sqlalchemy.JSON, nullable=False)
    successful: orm.Mapped[bool] = orm.mapped_column(nullable=False)
    error: orm.Mapped[Optional[str]] = orm.mapped_column(nullable=True)
    output: orm.Mapped[Optional[str]] = orm.mapped_column(nullable=True)


class Timetable(_TableBase):
    """Database table storing GTFS timetables."""

    __tablename__ = "timetables"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    run_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey("run_metadata.id"))
    feed_update_time: orm.Mapped[datetime.datetime] = orm.mapped_column(nullable=True)
    upload_date: orm.Mapped[datetime.date] = orm.mapped_column(nullable=False)
    timetable_path: orm.Mapped[str] = orm.mapped_column(nullable=False)
    adjusted: orm.Mapped[bool] = orm.mapped_column(nullable=False)
    base_timetable_id: orm.Mapped[Optional[int]] = orm.mapped_column(
        sqlalchemy.ForeignKey("timetables.id"), nullable=True
    )
    delay_calculation: orm.Mapped[Optional[str]] = orm.mapped_column(nullable=True)

    @property
    def actual_timetable_path(self) -> pathlib.Path:
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
        model_name: ModelName,
        start_datetime: datetime.datetime,
        parameters: str,
        sucessful: bool,
        zone_system_id: Optional[int] = None,
        error: Optional[str] = None,
        output: Optional[str] = None,
    ) -> int:
        stmt = (
            sqlalchemy.insert(RunMetadata)
            .values(
                zone_system_id=zone_system_id,
                model_name=model_name,
                start_datetime=start_datetime,
                end_datetime=datetime.datetime.now(),
                parameters=parameters,
                sucessful=sucessful,
                error=error,
                output=output,
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
        run_id: int,
        feed_update_time: Optional[datetime.datetime],
        timetable_path: str,
        adjusted: bool = False,
        base_timetable_id: Optional[int] = None,
    ) -> Timetable:
        stmt = (
            sqlalchemy.insert(Timetable)
            .values(
                run_id=run_id,
                feed_update_time=feed_update_time,
                upload_date=datetime.date.today(),
                timetable_path=timetable_path,
                adjusted=adjusted,
                base_timetable_id=base_timetable_id,
            )
            .returning(Timetable)
        )

        with orm.Session(self.engine) as session:
            result = session.execute(stmt)
            id_ = result.one().tuple()[0]
            session.commit()

        LOG.info("Inserted GTFS timetable into database with ID: %s", id_)
        return id_

    def find_recent_timetable(self, adjusted: bool = False) -> Timetable:
        stmt = (
            sqlalchemy.select(Timetable)
            .where(Timetable.adjusted == adjusted)
            .order_by(Timetable.upload_date.desc())
            .limit(1)
        )

        with orm.Session(self.engine) as session:
            result = session.execute(stmt)
            timetable = result.one().tuple()[0]

        return timetable
