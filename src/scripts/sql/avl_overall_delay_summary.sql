SELECT
	'all' AS "group",
	min(estimated_delay_secs),
	max(estimated_delay_secs),
	count(estimated_delay_secs),
	sum(estimated_delay_secs) / count(estimated_delay_secs) AS "mean(estimated_delay_secs)"
FROM gtfs_stop_time_delays
WHERE estimated_delay_secs IS NOT NULL

UNION

SELECT
	'positive' AS "group",
	min(estimated_delay_secs),
	max(estimated_delay_secs),
	count(estimated_delay_secs),
	sum(estimated_delay_secs) / count(estimated_delay_secs) AS "mean(estimated_delay_secs)"
FROM gtfs_stop_time_delays
WHERE estimated_delay_secs IS NOT NULL AND estimated_delay_secs > 0

UNION

SELECT
	'negative' AS "group",
	min(estimated_delay_secs),
	max(estimated_delay_secs),
	count(estimated_delay_secs),
	sum(estimated_delay_secs) / count(estimated_delay_secs) AS "mean(estimated_delay_secs)"
FROM gtfs_stop_time_delays
WHERE estimated_delay_secs IS NOT NULL AND estimated_delay_secs < 0

UNION

SELECT
	'zero' AS "group",
	min(estimated_delay_secs),
	max(estimated_delay_secs),
	count(estimated_delay_secs),
	sum(estimated_delay_secs) / count(estimated_delay_secs) AS "mean(estimated_delay_secs)"
FROM gtfs_stop_time_delays
WHERE estimated_delay_secs IS NOT NULL AND estimated_delay_secs = 0
