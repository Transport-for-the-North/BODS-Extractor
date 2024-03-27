-- Calculate distance from previous stop
UPDATE gtfs_stop_times
	SET prev_stop_distance_metres = sqrt(
		pow(stop_east - d.prev_easting, 2) + pow(stop_north - d.prev_northing, 2)
	)

FROM (
	SELECT
		ROWID,
		lag (stop_east) OVER ordered_trip prev_easting,
		lag (stop_north) OVER ordered_trip prev_northing
	FROM gtfs_stop_times
	WINDOW ordered_trip AS (PARTITION BY trip_id ORDER BY stop_sequence)
) d
WHERE ROWID = d.ROWID
