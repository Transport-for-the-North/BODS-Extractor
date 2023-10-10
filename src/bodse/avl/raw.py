# -*- coding: utf-8 -*-
"""Functionality for parsing the AVL raw XML format."""

##### IMPORTS #####
from __future__ import annotations

# Standard imports
import copy
import datetime as dt
import logging
from typing import Any, Iterator, Literal, Optional, TypeVar
import warnings
import xml.etree.ElementTree as ET

# Third party imports
from pydantic import dataclasses, fields
from tqdm import tqdm

# Local imports


##### CONSTANTS #####
LOG = logging.getLogger(__name__)
API_ENDPOINT = "datafeed/"
SIRI_NAMESPACES = {"siri": "http://www.siri.org.uk/siri"}
VEHICLE_MONITORING_TAG = "siri:ServiceDelivery/siri:VehicleMonitoringDelivery"
T = TypeVar("T")

##### CLASSES #####
class AVLParseWarning(RuntimeWarning):
    """Warning when parsing AVL Siri XML data."""


class AVLParseError(Exception):
    """Error when parsing AVL Siri XML data."""


@dataclasses.dataclass
class AVLMetadata:
    """Metadata from the BODS AVL Siri XML data."""

    response_time: dt.datetime
    producer_ref: str
    valid_until: dt.datetime
    shortest_cycle: str

    _producer_ref_tag = "siri:ServiceDelivery/siri:ProducerRef"
    _vehicle_monitoring_tags = {
        "response_time": "siri:ResponseTimestamp",
        "valid_until": "siri:ValidUntil",
        "shortest_cycle": "siri:ShortestPossibleCycle",
    }

    @classmethod
    def parse_xml(cls, root: ET.Element) -> AVLMetadata:
        """Parse and extract metadata from Siri XML data.

        Parameters
        ----------
        root: ET.Element
            Root XML element from Siri data.
        """
        parent = "AVLMetadata"

        producer_ref = _findtext(root, cls._producer_ref_tag, parent, default="unknown")
        assert isinstance(producer_ref, str)

        vehicle_monitoring = _find(
            root, VEHICLE_MONITORING_TAG, parent, if_missing="error"
        )
        assert vehicle_monitoring is not None

        data = {}
        for name, tag in cls._vehicle_monitoring_tags.items():
            data[name] = _findtext(vehicle_monitoring, tag, parent, if_missing="error")

        return AVLMetadata(producer_ref=producer_ref, **data)

    @classmethod
    def get_fields(cls) -> list[fields.ModelField]:
        """List of data fields."""
        return list(cls.__dataclass_fields__.values())

    @property
    def data_dict(self) -> dict[str, Any]:
        """Convert class to dictionary."""
        return {i.name: getattr(self, i.name) for i in self.get_fields()}


@dataclasses.dataclass
class VehicleActivity:
    """AVL vehicle activity data."""

    xml_item_id: str
    recorded: dt.datetime
    valid_until: dt.datetime
    longitude: float
    latitude: float

    bearing: Optional[float] = None
    line_ref: Optional[str] = None
    direction: Optional[str] = None
    line_name: Optional[str] = None
    operator: Optional[str] = None
    vehicle_ref: Optional[str] = None
    origin_ref: Optional[str] = None
    destination_ref: Optional[str] = None
    block_ref: Optional[str] = None
    origin_name: Optional[str] = None
    destination_name: Optional[str] = None
    occupancy: Optional[str] = None
    origin_scheduled_time: Optional[dt.datetime] = None
    destination_scheduled_time: Optional[dt.datetime] = None
    # Optional framed vehicle journey info
    data_frame_ref: Optional[str] = None
    vehicle_journey_ref: Optional[str] = None

    # Optional tags found in extensions
    vehicle_id: Optional[str] = None
    seated_occupancy: Optional[int] = None
    seated_capacity: Optional[int] = None
    wheelchair_occupancy: Optional[int] = None
    wheelchair_capacity: Optional[int] = None
    occupancy_threshold: Optional[str] = None
    ticket_machine_service_code: Optional[str] = None
    journey_code: Optional[str] = None

    # XML tag lookups
    _expected_tags = {
        "recorded": "siri:RecordedAtTime",
        "xml_item_id": "siri:ItemIdentifier",
        "valid_until": "siri:ValidUntilTime",
    }
    _warning_tags = {
        "line_ref": "siri:LineRef",
        "direction": "siri:DirectionRef",
        "operator": "siri:OperatorRef",
        "vehicle_ref": "siri:VehicleRef",
        "longitude": "siri:VehicleLocation/siri:Longitude",
        "latitude": "siri:VehicleLocation/siri:Latitude",
    }
    _optional_tags = {
        "line_name": "siri:PublishedLineName",
        "origin_ref": "siri:OriginRef",
        "destination_ref": "siri:DestinationRef",
        "block_ref": "siri:BlockRef",
        "origin_name": "siri:OriginName",
        "destination_name": "siri:DestinationName",
        "occupancy": "siri:Occupancy",
        "origin_scheduled_time": "siri:OriginAimedDepartureTime",
        "destination_scheduled_time": "siri:DestinationAimedArrivalTime",
        "bearing": "siri:Bearing",
        "data_frame_ref": "siri:FramedVehicleJourneyRef/siri:DataFrameRef",
        "vehicle_journey_ref": "siri:FramedVehicleJourneyRef"
        "/siri:DatedVehicleJourneyRef",
    }
    _vehicle_journey_tags = {
        "vehicle_id": "siri:VehicleUniqueId",
        "seated_occupancy": "siri:SeatedOccupancy",
        "seated_capacity": "siri:SeatedCapacity",
        "wheelchair_occupancy": "siri:WheelchairOccupancy",
        "wheelchair_capacity": "siri:WheelchairCapacity",
        "occupancy_threshold": "siri:OccupancyThresholds",
        "ticket_machine_service_code": "siri:Operational/siri:TicketMachine"
        "/siri:TicketMachineServiceCode",
        "journey_code": "siri:Operational/siri:TicketMachine/siri:JourneyCode",
    }

    @classmethod
    def parse_xml(cls, activity: ET.Element, name: str) -> VehicleActivity:
        """Parse and extract from VehicleActivity Siri element.

        Parameters
        ----------
        root: ET.Element
            Root XML element from Siri data.
        """
        parent = f"VehicleActivity - {name}"

        data = {}

        for name, tag in cls._expected_tags.items():
            data[name] = _findtext(activity, tag, parent)

        vehicle_journey = _find(
            activity, "siri:MonitoredVehicleJourney", parent=parent, if_missing="error"
        )
        assert vehicle_journey is not None

        for name, tag in cls._warning_tags.items():
            data[name] = _findtext(vehicle_journey, tag, parent, if_missing="ignore")

        for name, tag in cls._optional_tags.items():
            data[name] = _findtext(vehicle_journey, tag, parent, if_missing="ignore")

        vehicle_journey = _find(
            activity, "siri:Extensions/siri:VehicleJourney", parent, if_missing="ignore"
        )
        if vehicle_journey is not None:
            for name, tag in cls._vehicle_journey_tags.items():
                data[name] = _findtext(
                    vehicle_journey, tag, parent, if_missing="ignore"
                )

        return VehicleActivity(**data)

    @classmethod
    def get_fields(cls) -> list[fields.ModelField]:
        """List of data fields."""
        return list(cls.__dataclass_fields__.values())

    @property
    def data_dict(self) -> dict[str, Any]:
        """Convert class to dictionary."""
        return {i.name: getattr(self, i.name) for i in self.get_fields()}


##### FUNCTIONS #####
def _check_xml_find(
    value: T,
    tag: str,
    parent: str,
    if_missing: Literal["ignore", "warn", "error"],
    default: Optional[T] = None,
) -> T:
    if value is not None:
        return value

    if if_missing == "ignore":
        pass
    elif if_missing == "warn":
        warnings.warn(f"'{tag}' not present in {parent}", AVLParseWarning)
    elif if_missing == "error":
        raise AVLParseError(f"'{tag}' not present in {parent}")
    else:
        raise ValueError(f"unexpected value ({if_missing}) for missing")

    return default


def _findtext(
    element: ET.Element,
    tag: str,
    parent: str,
    if_missing: Literal["ignore", "warn", "error"] = "warn",
    default: Optional[str] = None,
) -> Optional[str]:
    if element is None:
        raise ValueError("undefined element")

    text = element.findtext(tag, namespaces=SIRI_NAMESPACES)
    return _check_xml_find(text, tag, parent, if_missing, default)


def _find(
    element: ET.Element,
    tag: str,
    parent: str,
    if_missing: Literal["ignore", "warn", "error"] = "warn",
    default: Optional[ET.Element] = None,
) -> Optional[ET.Element]:
    if element is None:
        raise ValueError("undefined element")

    sub_element = element.find(tag, namespaces=SIRI_NAMESPACES)
    return _check_xml_find(sub_element, tag, parent, if_missing, default)


def parse_metadata(text: str) -> tuple[ET.Element, AVLMetadata]:
    """Parse metadata from `text` in Siri XML format.

    Returns
    -------
    ET.Element
        Root element from Siri XML data.
    AVLMetadata
        Metadata from Siri XML.
    """
    root = ET.XML(text)
    meta = AVLMetadata.parse_xml(root)

    return root, meta


def iterate_activities(siri: ET.Element) -> Iterator[VehicleActivity]:
    """Iterate through 'VehicleActivity' tags and parse.

    Parameters
    ---------
    siri: ET.Element
        Root element of Siri XML data.

    See Also
    --------
    parse_metadata
    """
    vehicle_monitoring = _find(
        siri,
        VEHICLE_MONITORING_TAG,
        "Siri",
        if_missing="error",
    )
    assert vehicle_monitoring is not None

    vehicle_activities = vehicle_monitoring.findall(
        "siri:VehicleActivity", namespaces=SIRI_NAMESPACES
    )
    pbar = tqdm(
        enumerate(vehicle_activities),
        total=len(vehicle_activities),
        desc="Parsing AVL",
        dynamic_ncols=True,
        unit_scale=True,
    )

    for i, activity in pbar:
        yield VehicleActivity.parse_xml(activity, str(i))


def pretty_print(element: ET.Element) -> None:
    """Print `element` with indentations for XML levels."""
    element = copy.copy(element)
    ET.indent(element)
    print(ET.tostring(element, encoding="unicode"))
