SELECT
	metadata_id, ceil(position_delay_seconds / 300) AS position_delay_ceil_10mins,
	count(*) AS n_rows, count(trip_id) AS trip_id, count(*) - count(trip_id) AS null_trip_id

FROM gtfs_rt_vehicle_positions

GROUP BY metadata_id, position_delay_10mins;
