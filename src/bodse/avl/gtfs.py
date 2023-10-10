# -*- coding: utf-8 -*-
"""WIP Functionality for parsing the GTFS-RT downloads."""

##### IMPORTS #####
from __future__ import annotations
import dataclasses

# Standard imports
import datetime as dt
import enum
import logging
import pathlib
from typing import Iterator
from urllib import parse


# Third party imports
from google.transit import gtfs_realtime_pb2

# Local imports
from bodse import request


##### CONSTANTS #####
LOG = logging.getLogger(__name__)
API_ENDPOINT = "gtfsrtdatafeed/"


##### CLASSES #####
class Incrementality(enum.IntEnum):
    FULL_DATASET = 0
    DIFFERENTIAL = 1


@dataclasses.dataclass
class FeedHeader:
    timestamp: dt.datetime
    incrementality: Incrementality = Incrementality.FULL_DATASET


@dataclasses.dataclass
class FeedEntity:
    ...


@dataclasses.dataclass
class FeedMessage:
    header: FeedHeader
    extensions = None

    @classmethod
    def parse(cls, feed: gtfs_realtime_pb2.FeedMessage) -> FeedMessage:
        header = FeedHeader.parse(feed.header)

    def entities(self) -> Iterator[FeedEntity]:
        for entity in self.entities:
            ...


##### FUNCTIONS #####
def write_feed(
    path: pathlib.Path, auth: request.APIAuth, entity_limit: int = 100
) -> None:
    gtfs_repsonse, _ = request.get(
        parse.urljoin(request.BODS_API_BASE_URL, API_ENDPOINT),
        # params={"boundingBox": "51.401,51.509,0.01,0.201"},
        auth=auth,
    )

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(gtfs_repsonse)

    with open(path, "wt", encoding="utf-8") as output:
        output.write(
            f"Found {len(feed.entity)} entities, writing first {entity_limit}\n"
        )

        for i, entity in enumerate(feed.entity):
            output.write("\n".join(("-" * 20, f"{i!s:^20.20}", "-" * 20, str(entity))))

            if i >= entity_limit:
                break

    LOG.info("Written: %s", path.name)


def main():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response)
