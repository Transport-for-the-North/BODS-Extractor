SELECT 
	trip_id,
	current_stop_sequence,
	count(estimated_delay_secs),
	round(min(estimated_delay_secs) / 60, 1) AS "Min Delay (mins)",
	round(max(estimated_delay_secs) / 60, 1) AS "Max Delay (mins)",
	round((sum(estimated_delay_secs) / count(estimated_delay_secs)) / 60, 1) AS "Mean Delay (mins)"

FROM gtfs_stop_time_delays
WHERE estimated_delay_secs IS NOT NULL

GROUP BY trip_id, current_stop_sequence
