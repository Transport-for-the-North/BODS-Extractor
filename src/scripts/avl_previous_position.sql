SELECT
	trip_id, current_stop_sequence, current_status, timestamp,
	easting, northing, lag (easting) OVER ordered_trip prev_easting,
	lag (northing) OVER ordered_trip prev_northing,
	lag (timestamp) OVER ordered_trip prev_time

FROM gtfs_rt_vehicle_positions pos

WHERE trip_id IS NOT NULL AND length (trip_id) > 0

WINDOW ordered_trip AS (PARTITION BY trip_id ORDER BY current_stop_sequence)

ORDER BY trip_id, current_stop_sequence
