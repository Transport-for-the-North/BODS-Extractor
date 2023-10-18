# -*- coding: utf-8 -*-
"""WIP Functionality for parsing the GTFS-RT downloads."""

from __future__ import annotations

import abc
import datetime as dt
import enum
import logging
from typing import Any, Optional
from urllib import parse

from google.transit import gtfs_realtime_pb2
from pydantic import dataclasses
import pydantic

from bodse import request


##### CONSTANTS #####
LOG = logging.getLogger(__name__)
API_ENDPOINT = "gtfsrtdatafeed/"


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


class _GTFSEntity(abc.ABC):
    "Base class for GTFS dataclasses containing feed entities."

    @staticmethod
    @abc.abstractmethod
    def from_gtfs(id_: str, data: Any, is_deleted: bool = False) -> _GTFSDataclass:
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


@dataclasses.dataclass
class VehicleDescriptor(_GTFSDataclass):
    "Identification information for the vehicle performing the trip."

    id: Optional[str] = None
    "Internal system identification of the vehicle."
    label: Optional[str] = None
    "User visible label"
    license_plate: Optional[str] = None
    "The license plate of the vehicle."

    @staticmethod
    def from_gtfs(data) -> VehicleDescriptor:
        """Extract from gtfs_realtime_pb2 VehicleDescriptor object."""
        return VehicleDescriptor(**_get_fields(data, "id", "label", "license_plate"))


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

    id: str
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
            id=id_,
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

    id: str
    trip: Optional[TripDescriptor] = None
    vehicle: Optional[VehicleDescriptor] = None
    position: Optional[Position] = None
    current_stop_sequence: Optional[pydantic.conint(ge=0)] = None
    stop_id: Optional[str] = None
    current_status: VehicleStopStatus = VehicleStopStatus.IN_TRANSIT_TO
    timestamp: Optional[dt.datetime] = None
    congestion_level: Optional[CongestionLevel] = None
    occupancy_status: Optional[OccupancyStatus] = None
    is_deleted: bool = False

    @staticmethod
    def from_gtfs(
        id_: str, data: gtfs_realtime_pb2.VehiclePosition, is_deleted: bool = False
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

        return VehiclePosition(id=id_, **classes, **optional_fields, is_deleted=is_deleted)


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
                        id_, getattr(entity, cls._position_field), is_deleted
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
def _get_fields(data: Any, *fields: str, raise_missing: bool = False) -> dict[str, Any]:
    """Get field values from gtfs_realtime_pb2 object.

    Parameters
    ----------
    data : Any
        gtfs_realtime_pb2 object, expected to have `HasField` method.
    fields : str
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
    for name in fields:
        if data.HasField(name):
            values[name] = getattr(data, name)
        elif raise_missing:
            missing.append(name)

    if len(missing) > 0:
        raise ValueError(f"missing fields: {missing}")
    return values


def _dataclass_fields(
    data: Any, fields: dict[str, _GTFSDataclass], raise_missing: bool = False
) -> dict[str, _GTFSDataclass]:
    """Produce `_GTFSDataclass` for fields in gtfs_realtime_pb2 object.

    Parameters
    ----------
    data : Any
        gtfs_realtime_pb2 object, expected to have `HasField` method.
    fields : dict[str, _GTFSDataclass]
        Name(s) of attributes to get from `data` and the `_GTFSDataclass`
        to contain that fields data.
    raise_missing : bool, default False
        If True, raises `ValueError` if any `fields` are missing.

    Returns
    -------
    dict[str, Any]
        Field names and values.
    """
    values = _get_fields(data, *list(fields.keys()), raise_missing=raise_missing)
    classes = {}

    for name, value in values.items():
        classes[name] = fields[name].from_gtfs(value)

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
