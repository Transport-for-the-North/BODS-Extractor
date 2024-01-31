UPDATE OR ABORT gtfs_rt_vehicle_positions
	SET position_delay_seconds = new.position_delay_seconds
	
	FROM (
		SELECT pos.id, unixepoch(meta.timestamp) - unixepoch(pos.timestamp) AS position_delay_seconds

		FROM gtfs_rt_vehicle_positions pos
			LEFT JOIN gtfs_rt_meta meta ON pos.metadata_id = meta.id
	) new
	WHERE gtfs_rt_vehicle_positions.id = new.id
	
	
