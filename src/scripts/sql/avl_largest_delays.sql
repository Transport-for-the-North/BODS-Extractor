SELECT
	stp.position_id,
	stp.trip_id,
	stp.current_stop_sequence,
	stp.timestamp,
	stp.arrival_time,
	stp.estimated_arrival_time,
	round(stp.estimated_delay_secs / 60) AS "estimated_delay_mins",
	stp.speed_ms,
	round(stp.stop_dist_metres / 1000) AS "stop_dist_km",
	spd.prev_time,
	spd.distance_metres AS distance_to_prev_metres,
	spd.delta_time_seconds AS time_to_prev_seconds
FROM gtfs_stop_time_delays stp
	JOIN gtfs_rt_vehicle_speed_estimates spd ON stp.position_id = spd.position_id
ORDER BY abs(estimated_delay_secs) DESC
LIMIT 1000
