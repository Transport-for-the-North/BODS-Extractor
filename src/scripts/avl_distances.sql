SELECT
	*,
	sqrt(pow(easting - prev_easting, 2) + pow(northing - prev_northing, 2))
		AS distance_metres,
	unixepoch (timestamp) - unixepoch (prev_time) AS delta_time_seconds

FROM (
	-- Get easting / northing from the previous vehicle position
	SELECT
		*,
		lag (easting) OVER ordered_trip prev_easting,
		lag (northing) OVER ordered_trip prev_northing,
		lag (timestamp) OVER ordered_trip prev_time

	FROM gtfs_rt_vehicle_positions

	WHERE trip_id IS NOT NULL AND length (trip_id) > 0

	WINDOW ordered_trip AS (PARTITION BY trip_id ORDER BY current_stop_sequence, timestamp)

	ORDER BY trip_id, current_stop_sequence, timestamp
)

ORDER BY trip_id, current_stop_sequence;
