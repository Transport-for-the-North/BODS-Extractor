-- Table containing positions to be dropped
CREATE TABLE gtfs_position_filter AS

SELECT *
FROM (
	-- Calculate delays and distance from position to next stop for filtering
	SELECT *,
		datetime(unixepoch(timestamp) + round(dist_to_stop / speed_ms), 'unixepoch')
			AS estimated_arrival_time,
		(unixepoch(timestamp) + round(dist_to_stop / speed_ms)) - unixepoch(timestamp, 'start of day')
			AS estimated_arrival_seconds
	FROM (
		SELECT
			spd.position_id, spd.trip_id, spd.current_stop_sequence, spd.timestamp,
			spd.current_status, spd.easting, spd.northing, spd.speed_ms,
			stp.arrival_time, stp.departure_time, stp.stop_id, stp.stop_east, stp.stop_north,
			stp.timepoint, stp.arrival_secs, stp.departure_secs, stp.stop_distance_metres,
			sqrt(pow(easting - stop_east, 2) + pow(northing - stop_north, 2)) AS dist_to_stop
		FROM gtfs_rt_vehicle_speed_estimates spd
			JOIN gtfs_stop_times stp ON spd.trip_id = stp.trip_id AND spd.current_stop_sequence = stp.stop_sequence
	)
)
-- Unrealistic speeds and far from stops
WHERE
	(speed_ms > 35 AND dist_to_stop > 1000)
	OR abs(estimated_arrival_seconds - arrival_secs) > 7200
	OR dist_to_stop > (stop_distance_metres * 10)
