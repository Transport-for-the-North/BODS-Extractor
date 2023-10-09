# BODS-Extractor

Repository for the BODS (Bus Open Data Service) Extractor Python package (BODSE). The BODSE package
contains functionality for accessing the BODS API to download and does simple processing /
aggregation of the data. BODSE is split into multiple sub-packages for handling the different
data available from BODS:

- timetable: download the scheduled timetable data
- avl: download and aggregate the automatic vehicle location (AVL) data

In future more functionality may be added to handle the other BODS data.

## Timetable - WIP

The timetable sub-package is work in progress but will contain functionality for downloading
the timetable data in GTFS format initially.

## AVL - WIP

The avl sub-package is work in progress but will contain functionality for downloading the AVL
data in GTFS-rt format initially.

***Note:** the AVL package contains some WIP modules for downloading the data in Siri XML format.*
