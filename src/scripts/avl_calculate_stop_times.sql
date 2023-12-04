SELECT
	*,
	sqrt(pow(easting - prev_easting, 2) + pow(northing - prev_northing, 2)) AS distance,
	unixepoch (timestamp) - unixepoch (prev_time) AS delta_time

FROM gtfs_rt_vehicle_positions_previous pos

ORDER BY trip_id, current_stop_sequence

LIMIT 100;
