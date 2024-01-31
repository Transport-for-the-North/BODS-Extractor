DROP TABLE IF EXISTS gtfs_stop_time_delays;

CREATE TABLE gtfs_stop_time_delays AS
	SELECT
		*,
		estimated_arrival_seconds - arrival_secs AS estimated_delay_secs
	FROM (	
		SELECT *,
			datetime(unixepoch(timestamp) + round(stop_dist_metres / speed_ms), 'unixepoch') AS estimated_arrival_time,
			(unixepoch(timestamp) + round(stop_dist_metres / speed_ms)) - unixepoch(timestamp, 'start of day') AS estimated_arrival_seconds
		FROM (
			SELECT
				spd.position_id, spd.trip_id, spd.current_stop_sequence, spd.timestamp,
				spd.current_status, spd.easting, spd.northing, spd.speed_ms,
				stp.arrival_time, stp.departure_time, stp.stop_id, stp.stop_east, stp.stop_north,
				stp.timepoint, stp.arrival_secs, stp.departure_secs,
				sqrt(pow(easting - stop_east, 2) + pow(northing - stop_north, 2)) AS stop_dist_metres
			FROM gtfs_rt_vehicle_speed_estimates spd
				JOIN gtfs_stop_times stp ON spd.trip_id = stp.trip_id AND spd.current_stop_sequence = stp.stop_sequence
		)
	);

UPDATE gtfs_stop_time_delays
SET estimated_delay_secs = estimated_arrival_seconds - mod(arrival_secs, 86400)
WHERE abs(estimated_arrival_seconds - mod(arrival_secs, 86400)) < abs(estimated_delay_secs);

UPDATE gtfs_stop_time_delays
SET
	estimated_arrival_time = timestamp,
	estimated_arrival_seconds = unixepoch(timestamp) - unixepoch(timestamp, 'start of day'),
	estimated_delay_secs = unixepoch(timestamp) - unixepoch(timestamp, 'start of day') - arrival_secs
WHERE current_status = 'STOPPED_AT';
