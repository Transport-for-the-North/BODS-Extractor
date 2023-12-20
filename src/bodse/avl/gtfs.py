# -*- coding: utf-8 -*-
"""WIP Functionality for parsing the GTFS-RT downloads."""

from __future__ import annotations

import abc
import datetime as dt
import enum
import hashlib
import logging
from typing import Any, Optional
from urllib import parse
import warnings

import numpy as np
import pydantic
from google.transit import gtfs_realtime_pb2
from pydantic import dataclasses, fields

from bodse import request

##### CONSTANTS #####

LOG = logging.getLogger(__name__)
API_ENDPOINT = "gtfsrtdatafeed/"

_GTFS_COORDINATES_CRS = "EPSG:4326"  # longitude / latitude
_EASTING_NORTHING_CRS = "EPSG:27700"
_MINIMUM_TRANSFORM_ACCURACY = 5

# Pyproj is an optional dependancy only required for calculating
# easting / northing, if not given NaN's will be returned for those
# values
try:
    import pyproj
    from pyproj import transformer
except ModuleNotFoundError:
    _COORD_TRANSFORMER = None
else:
    _COORD_TRANSFORMER = transformer.Transformer.from_crs(
        _GTFS_COORDINATES_CRS,
        _EASTING_NORTHING_CRS,
        accuracy=_MINIMUM_TRANSFORM_ACCURACY,
        allow_ballpark=False,
    )


##### CLASSES #####


class _GTFSDataclass(abc.ABC):
    "Base class for GTFS dataclasses."

    @staticmethod
    @abc.abstractmethod
    def from_gtfs(data) -> _GTFSDataclass:
        """Extract GTFS-rt data from gtfs_realtime_pb2 object.

        Parameters
        ----------
        data : gtfs_realtime_pb2 feed object
            GTFS-rt object defined in protobuf.
        """
        raise NotImplementedError("Abtract method not implemented in ABC")

    @classmethod
    def get_fields(cls) -> list[fields.ModelField]:
        """List of data fields."""
        # Added by dataclass decorator pylint: disable=no-member
        return list(cls.__dataclass_fields__.values())


class _GTFSEntity(abc.ABC):
    "Base class for GTFS dataclasses containing feed entities."

    @staticmethod
    @abc.abstractmethod
    def from_gtfs(id_: str, data: Any, is_deleted: bool = False) -> _GTFSEntity:
        """Extract GTFS-rt data from gtfs_realtime_pb2 entity object.

        Parameters
        ----------
        id : str
            GTFS feed entity ID (unique within a single feed message).
        data : Any
            GTFS feed entity data.
        is_deleted : bool, default False
            GTFS feed entity flag.
        """
        raise NotImplementedError("Abtract method not implemented in ABC")

    @classmethod
    def get_fields(cls) -> list[fields.ModelField]:
        """List of data fields."""
        # Added by dataclass decorator pylint: disable=no-member
        return list(cls.__dataclass_fields__.values())

    def sha256(self, field_names: Optional[list[str]] = None) -> str:
        """Generate hexadecimal SHA256 hash for the object."""
        if field_names is None:
            field_names = [i.name for i in self.get_fields()]

        encoding = "utf-8"
        m = hashlib.sha256()

        for name in field_names:
            value = getattr(self, name)
            if isinstance(value, _GTFSEntity):
                m.update(bytes(value.sha256(), encoding))
            else:
                m.update(bytes(str(value), encoding))

        return m.hexdigest()


class Incrementality(enum.IntEnum):
    """Determines whether the current fetch is incremental.

    According to GTFS-rt definition "Currently, DIFFERENTIAL mode is
    unsupported and behavior is unspecified for feeds that use this mode."
    """

    FULL_DATASET = 0
    DIFFERENTIAL = 1


@dataclasses.dataclass
class FeedHeader(_GTFSDataclass):
    """Metadata about the GTFS feed message."""

    timestamp: dt.datetime
    gtfs_realtime_version: str
    incrementality: Incrementality = Incrementality.FULL_DATASET

    @pydantic.validator("gtfs_realtime_version")
    def _check_version(cls, value: str) -> str:
        # pylint: disable=no-self-argument
        if value != "2.0":
            raise ValueError(f"expected GTFS-rt version 2.0 not {value}")

        return value

    @pydantic.validator("incrementality")
    def _check_incrementality(cls, value: Incrementality) -> Incrementality:
        # pylint: disable=no-self-argument
        if value != Incrementality.FULL_DATASET:
            raise ValueError(
                "according to the GTFS-rt v2.0 specification "
                '"DIFFERENTIAL mode is unsupported and behavior is unspecified"'
            )

        return value

    @staticmethod
    def from_gtfs(data: gtfs_realtime_pb2.FeedHeader) -> FeedHeader:
        """Extract from gtfs_realtime_pb2 FeedHeader object.

        Raises
        ------
        ValueError: if any expected fields are missing.
        """
        return FeedHeader(
            **_get_fields(
                data,
                "timestamp",
                "gtfs_realtime_version",
                "incrementality",
                raise_missing=True,
            )
        )


class ScheduleRelationship(enum.IntEnum):
    "Relation between this trip and the static schedule."

    SCHEDULED = 0
    "Trip that is running in accordance with its GTFS schedule"
    ADDED = 1
    "An extra trip that was added in addition to a running schedule"
    UNSCHEDULED = 2
    "A trip that is running with no schedule associated to it"
    CANCELED = 3
    "A trip that existed in the schedule but was removed."


@dataclasses.dataclass
class TripDescriptor(_GTFSDataclass):
    "A descriptor that identifies an instance of a GTFS trip."

    trip_id: Optional[str] = None
    "The trip_id from the GTFS feed that this selector refers to"
    route_id: Optional[str] = None
    "The route_id from the GTFS that this selector refers to."
    direction_id: Optional[int] = None
    """The direction_id from the GTFS feed trips.txt file, indicating the
    direction of travel for trips this selector refers to."""
    start_time: Optional[dt.time] = None
    "The initially scheduled start time of this trip instance."
    start_date: Optional[dt.date] = None
    "The scheduled start date of this trip instance."
    schedule_relationship: Optional[ScheduleRelationship] = None
    "The relation between this trip and the static schedule."

    @pydantic.validator("start_date", pre=True)
    def _parse_date(cls, value: Optional[str]) -> Optional[dt.date]:
        # pylint: disable=no-self-argument
        if value is None:
            return None
        return dt.date.fromisoformat(value)

    @pydantic.validator("start_time", pre=True)
    def _parse_time(cls, value: Optional[str]) -> Optional[dt.time]:
        # pylint: disable=no-self-argument
        if value is None:
            return None
        return dt.time.fromisoformat(value)

    @pydantic.validator("trip_id")
    def _empty_strings(cls, value: Optional[str]) -> Optional[str]:
        """Check if string is empty and return None if so."""
        # pylint: disable=no-self-argument
        if value is None:
            return value

        value = value.strip()
        if len(value) == 0:
            return None

        return value

    @staticmethod
    def from_gtfs(data) -> TripDescriptor:
        """Extract from gtfs_realtime_pb2 TripDescriptor object."""
        scalars = _get_fields(
            data,
            "trip_id",
            "route_id",
            "direction_id",
            "start_time",
            "start_date",
            "schedule_relationship",
        )
        return TripDescriptor(**scalars)


class WheelchairAccessible(enum.IntEnum):
    """Whether, or not, a vehicle is wheelchair accessible."""

    NO_VALUE = 0
    """The trip doesn't have information about wheelchair accessibility
    (default behaviour don't overwrite GTFS schedule)."""
    UNKNOWN = 1
    """The trip has no accessibility value present 
    (should overwrite GTFS according to specification)."""
    WHEELCHAIR_ACCESSIBLE = 2
    "The trip is wheelchair accessible."
    WHEELCHAIR_INACCESSIBLE = 3
    "The trip is not wheelchair accessible."


@dataclasses.dataclass
class VehicleDescriptor(_GTFSDataclass):
    "Identification information for the vehicle performing the trip."

    id: Optional[str] = None
    "Internal system identification of the vehicle."
    label: Optional[str] = None
    "User visible label"
    license_plate: Optional[str] = None
    "The license plate of the vehicle."
    wheelchair_accessible: WheelchairAccessible = WheelchairAccessible.NO_VALUE
    """wheelchair_accessible isn't implemented in gtfs-realtime-bindings v1.0.0,
    but is defined in the GTFS-rt specification."""

    @staticmethod
    def from_gtfs(data) -> VehicleDescriptor:
        """Extract from gtfs_realtime_pb2 VehicleDescriptor object."""
        return VehicleDescriptor(
            **_get_fields(data, "id", "label", "license_plate", "wheelchair_accessible")
        )


@dataclasses.dataclass
class StopTimeEvent(_GTFSDataclass):
    "Timing information for a single predicted event."
    delay: Optional[int] = None
    time: Optional[dt.datetime] = None
    uncertainty: Optional[int] = None

    @staticmethod
    def from_gtfs(data) -> StopTimeEvent:
        """Extract from gtfs_realtime_pb2 StopTimeEvent object."""
        return StopTimeEvent(**_get_fields(data, "delay", "time", "uncertainty"))


class StopScheduleRelationship(enum.IntEnum):
    "The relation between this StopTime and the static schedule."

    SCHEDULED = 0
    SKIPPED = 1
    NO_DATA = 2


@dataclasses.dataclass
class StopTimeUpdate(_GTFSDataclass):
    "Realtime update for events for a given stop on a trip."

    stop_sequence: Optional[int] = None
    "Same as in stop_times.txt in the corresponding GTFS feed."
    stop_id: Optional[str] = None
    "Must be the same as in stops.txt in the corresponding GTFS feed."
    arrival: Optional[StopTimeEvent] = None
    departure: Optional[StopTimeEvent] = None
    schedule_relationship: StopScheduleRelationship = StopScheduleRelationship.SCHEDULED
    "The relation between this StopTime and the static schedule."

    @staticmethod
    def from_gtfs(data) -> StopTimeUpdate:
        """Extract from gtfs_realtime_pb2 StopTimeUpdate object."""
        scalars = _get_fields(data, "stop_sequence", "stop_id", "schedule_relationship")
        if scalars["schedule_relationship"] is None:
            scalars.pop("schedule_relationship")

        classes = _dataclass_fields(
            data, dict.fromkeys(("arrival", "departure"), StopTimeEvent)
        )
        return StopTimeUpdate(**scalars, **classes)


@dataclasses.dataclass
class TripUpdate(_GTFSEntity):
    """GTFS-rt trip update entity data."""

    feed_id: str
    trip: TripDescriptor
    vehicle: Optional[VehicleDescriptor]
    stop_time_update: list[StopTimeUpdate]
    timestamp: Optional[dt.datetime]
    delay: Optional[int]
    is_deleted: bool = False

    @staticmethod
    def from_gtfs(
        id_: str, data: gtfs_realtime_pb2.TripUpdate, is_deleted: bool = False
    ) -> TripUpdate:
        """Extract GTFS-rt data from gtfs_realtime_pb2 TripUpdate entity object.

        Parameters
        ----------
        id : str
            GTFS feed entity ID (unique within a single feed message).
        data : Any
            GTFS feed entity data.
        is_deleted : bool, default False
            GTFS feed entity flag.
        """
        for name in ("trip", "stop_time_update"):
            if not data.HasField(name):
                raise ValueError(f"missing mandatory field '{name}'")

        if data.HasField("vehicle"):
            vehicle = VehicleDescriptor.from_gtfs(data)
        else:
            vehicle = None

        scalars = _get_fields(data, "timestamp", "delay")

        return TripUpdate(
            feed_id=id_,
            trip=TripDescriptor.from_gtfs(data.trip),
            vehicle=vehicle,
            stop_time_update=[StopTimeUpdate.from_gtfs(i) for i in data.stop_time_update],
            **scalars,
            is_deleted=is_deleted,
        )


@dataclasses.dataclass
class Position(_GTFSDataclass):
    """Position of vehicle."""

    latitude: float
    "Degrees North, in the WGS-84 coordinate system."
    longitude: float
    "Degrees East, in the WGS-84 coordinate system."
    easting: float
    "British National Grid (EPSG:27700) easting."
    northing: float
    "British National Grid (EPSG:27700) northing."
    bearing: Optional[float] = None
    "Bearing, in degrees, clockwise from North."
    odometer: Optional[float] = None
    "Odometer value, in meters"
    speed: Optional[float] = None
    "Momentary speed measured by the vehicle, in meters per second."

    @staticmethod
    def from_gtfs(data) -> Position:
        """Extract from gtfs_realtime_pb2 Position object."""
        mandatory = _get_fields(data, "latitude", "longitude", raise_missing=True)
        optional = _get_fields(data, "bearing", "odometer", "speed")

        mandatory["easting"], mandatory["northing"] = _lat_lon_to_bng(
            mandatory["latitude"], mandatory["longitude"]
        )

        return Position(**mandatory, **optional)


class VehicleStopStatus(enum.IntEnum):
    """Status of next stop in journey."""

    INCOMING_AT = 0
    "The vehicle is just about to arrive at the stop."
    STOPPED_AT = 1
    "The vehicle is standing at the stop."
    IN_TRANSIT_TO = 2
    "The vehicle has departed and is in transit to the next stop."


class CongestionLevel(enum.IntEnum):
    "Congestion level that is affecting this vehicle."

    UNKNOWN_CONGESTION_LEVEL = 0
    RUNNING_SMOOTHLY = 1
    STOP_AND_GO = 2
    CONGESTION = 3
    SEVERE_CONGESTION = 4


class OccupancyStatus(enum.IntEnum):
    """The degree of passenger occupancy of the vehicle.

    According to the GTFS-rt standard "This field is still experimental,
    and subject to change. It may be formally adopted in the future."
    """

    EMPTY = 0
    MANY_SEATS_AVAILABLE = 1
    FEW_SEATS_AVAILABLE = 2
    STANDING_ROOM_ONLY = 3
    CRUSHED_STANDING_ROOM_ONLY = 4
    FULL = 5
    NOT_ACCEPTING_PASSENGERS = 6


@dataclasses.dataclass
class VehiclePosition(_GTFSEntity):
    """GTFS-rt trip vehicle position (AVL) data."""

    feed_id: str
    trip: Optional[TripDescriptor] = None
    vehicle: Optional[VehicleDescriptor] = None
    position: Optional[Position] = None
    current_stop_sequence: Optional[pydantic.conint(ge=0)] = None
    stop_id: Optional[str] = None
    current_status: VehicleStopStatus = VehicleStopStatus.IN_TRANSIT_TO
    timestamp: Optional[dt.datetime] = None
    position_delay_seconds: Optional[int] = None
    congestion_level: Optional[CongestionLevel] = None
    occupancy_status: Optional[OccupancyStatus] = None
    is_deleted: bool = False

    @staticmethod
    def from_gtfs(
        id_: str,
        data: gtfs_realtime_pb2.VehiclePosition,
        is_deleted: bool = False,
        response_timestamp: Optional[dt.datetime] = None,
    ) -> VehiclePosition:
        """Extract GTFS-rt data from gtfs_realtime_pb2 VehiclePosition entity object.

        Parameters
        ----------
        id : str
            GTFS feed entity ID (unique within a single feed message).
        data : Any
            GTFS feed entity data.
        is_deleted : bool, default False
            GTFS feed entity flag.
        response_timestamp : dt.datetime, optional
            Timestamp for the AVL request response, used to
            calculate `position_delay_seconds`.
        """
        optional_fields = _get_fields(
            data,
            "current_stop_sequence",
            "stop_id",
            "current_status",
            "timestamp",
            "congestion_level",
            "occupancy_status",
        )

        classes = _dataclass_fields(
            data, {"trip": TripDescriptor, "vehicle": VehicleDescriptor, "position": Position}
        )

        if response_timestamp is not None and "timestamp" in optional_fields:
            timestamp: dt.datetime = dt.datetime.fromtimestamp(
                optional_fields["timestamp"], dt.timezone.utc
            )
            delay = (response_timestamp - timestamp).total_seconds()
        else:
            delay = None

        return VehiclePosition(
            feed_id=id_,
            **classes,
            **optional_fields,
            is_deleted=is_deleted,
            position_delay_seconds=delay,
        )


@dataclasses.dataclass
class FeedMessage:
    """GTFS-rt feed data."""

    header: FeedHeader
    updates: list[TripUpdate]
    positions: list[VehiclePosition]
    alerts: list[str]
    "List of entity IDs containing alerts, not used."

    # GTFS-rt feed entity fields
    _update_field = "trip_update"
    _position_field = "vehicle"
    _alert_field = "alert"

    @classmethod
    def parse(cls, data: bytes) -> FeedMessage:
        """Parse GTFS-rt feed data from binary protobuf representation.

        Parameters
        ----------
        data : bytes
            Protocol buffer binary representation of the GTFS-rt feed data.
        """
        feed = gtfs_realtime_pb2.FeedMessage()  # pylint: disable=no-member
        feed.ParseFromString(data)
        header = FeedHeader.from_gtfs(feed.header)
        updates = []
        positions = []
        alerts = []

        entity = feed.entity[0]
        for entity in feed.entity:
            id_ = entity.id
            is_deleted = entity.is_deleted

            if entity.HasField(cls._update_field):
                updates.append(
                    TripUpdate.from_gtfs(id_, getattr(entity, cls._update_field), is_deleted)
                )

            if entity.HasField(cls._position_field):
                positions.append(
                    VehiclePosition.from_gtfs(
                        id_, getattr(entity, cls._position_field), is_deleted, header.timestamp
                    )
                )

            if entity.HasField(cls._alert_field):
                alerts.append(id_)

        return FeedMessage(
            header=header,
            updates=updates,
            positions=positions,
            alerts=alerts,
        )


##### FUNCTIONS #####


def _get_fields(data: Any, *field_names: str, raise_missing: bool = False) -> dict[str, Any]:
    """Get field values from gtfs_realtime_pb2 object.

    Parameters
    ----------
    data : Any
        gtfs_realtime_pb2 object, expected to have `HasField` method.
    field_names : str
        Name(s) of attributes to get from `data`.
    raise_missing : bool, default False
        If True, raises `ValueError` if any `fields` are missing.

    Returns
    -------
    dict[str, Any]
        Field names and values.
    """
    values = {}
    missing = []
    for name in field_names:
        try:
            has_field = data.HasField(name)
        except ValueError:
            warnings.warn(f"'{name}' field not in GTFS-rt object {type(data)}", RuntimeWarning)
            has_field = False

        if has_field:
            values[name] = getattr(data, name)
        elif raise_missing:
            missing.append(name)

    if len(missing) > 0:
        raise ValueError(f"missing fields: {missing}")
    return values


def _dataclass_fields(
    data: Any, field_classes: dict[str, _GTFSDataclass], raise_missing: bool = False
) -> dict[str, _GTFSDataclass]:
    """Produce `_GTFSDataclass` for fields in gtfs_realtime_pb2 object.

    Parameters
    ----------
    data : Any
        gtfs_realtime_pb2 object, expected to have `HasField` method.
    field_classes : dict[str, _GTFSDataclass]
        Name(s) of attributes to get from `data` and the `_GTFSDataclass`
        to contain that fields data.
    raise_missing : bool, default False
        If True, raises `ValueError` if any `fields` are missing.

    Returns
    -------
    dict[str, Any]
        Field names and values.
    """
    values = _get_fields(data, *list(field_classes.keys()), raise_missing=raise_missing)
    classes = {}

    for name, value in values.items():
        classes[name] = field_classes[name].from_gtfs(value)

    return classes


def download(
    auth: request.APIAuth, bounds: Optional[request.BoundingBox] = None
) -> FeedMessage:
    """Download and parse GTFS-rt feed from BODS AVL API.

    Parameters
    ----------
    auth : request.APIAuth
        BODS API authentification.
    bounds : request.BoundingBox, optional
        Bounds for filtering the AVL response.

    Returns
    -------
    FeedMessage
        AVL feed data.

    Raises
    ------
    HTTPError: if the get request fails.
    """
    params = {}
    if bounds is not None:
        params["boundingBox"] = bounds.as_str()

    url = parse.urljoin(request.BODS_API_BASE_URL, API_ENDPOINT)
    gtfs_response, _ = request.get(url, auth=auth, params=params)

    feed = FeedMessage.parse(gtfs_response)

    LOG.info(
        "Downloaded AVL GTFS-rt feed, found %s trip updates,"
        " %s vehicle positions and %s alerts (ignored)",
        len(feed.updates),
        len(feed.positions),
        len(feed.alerts),
    )

    return feed


def _lat_lon_to_bng(latitude: float, longitude: float) -> tuple[float, float]:
    """Convert latitude and longitude to easting and northing.

    Depenancy pyproj is required for this to work, if it isn't
    available (NaN, NaN) is returned.
    """
    if _COORD_TRANSFORMER is None:
        return (np.nan, np.nan)

    try:
        return _COORD_TRANSFORMER.transform(latitude, longitude, errcheck=True)
    except pyproj.ProjError:
        LOG.error(
            "Error transforming coordinates (%s, %s)", longitude, latitude, exc_info=True
        )
        return (np.nan, np.nan)
