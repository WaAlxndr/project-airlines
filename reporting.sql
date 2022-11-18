CREATE SCHEMA IF NOT EXISTS reporting;
CREATE OR REPLACE VIEW reporting.flight
 AS
 SELECT flight.month,
    flight.day_of_month,
    flight.day_of_week,
    flight.op_unique_carrier,
    flight.tail_num,
    flight.op_carrier_fl_num,
    flight.origin_airport_id,
    flight.dest_airport_id,
    flight.crs_dep_time,
    flight.dep_time,
    flight.dep_delay_new,
    flight.dep_time_blk,
    flight.crs_arr_time,
    flight.arr_time,
    flight.arr_delay_new,
    flight.arr_time_blk,
    flight.cancelled,
    flight.crs_elapsed_time,
    flight.actual_elapsed_time,
    flight.distance,
    flight.distance_group,
    flight.year,
    flight.carrier_delay,
    flight.weather_delay,
    flight.nas_delay,
    flight.security_delay,
    flight.late_aircraft_delay,
    flight.id,
        CASE
            WHEN flight.dep_delay_new > 0::double precision THEN 1
            ELSE 0
        END AS is_delayed
   FROM flight
  WHERE flight.cancelled = 0;
CREATE OR REPLACE VIEW reporting.top_reliability_roads AS
WITH flight_agg AS
(
	SELECT
		f.origin_airport_id,
		alo.name AS origin_airport_name,
		f.dest_airport_id,
		ald.name AS dest_airport_name,
		f.year,
		COUNT(*) AS cnt,
		AVG(f.is_delayed) AS reliability	
	FROM
		reporting.flight f
		INNER JOIN airport_list alo ON f.origin_airport_id = alo.origin_airport_id
		INNER JOIN airport_list ald ON f.dest_airport_id = ald.origin_airport_id
	GROUP BY
		f.origin_airport_id,
		alo.name,
		f.dest_airport_id,
		ald.name,
		f.year
	HAVING
		COUNT(*) > 10000
)
SELECT
	origin_airport_id,
	origin_airport_name,
	dest_airport_id,
	dest_airport_name,
	flight_agg.year,
	cnt,
	reliability,
	DENSE_RANK() OVER(ORDER BY reliability ASC) AS nb
FROM
	flight_agg;
CREATE OR REPLACE VIEW reporting.year_to_year_comparision AS
SELECT
	f.year,
	f.month,
	COUNT(*) AS flights_amount,
	AVG(f.is_delayed) AS reliability
FROM
	reporting.flight f
GROUP BY
	f.year,
	f.month
;
CREATE OR REPLACE VIEW reporting.day_to_day_comparision AS
SELECT
	f.year,
	f.day_of_week,
	COUNT(*) AS flights_amount
FROM
	reporting.flight f
GROUP BY
	f.year,
	f.day_of_week
;
CREATE OR REPLACE VIEW reporting.day_by_day_reliability AS
SELECT
	to_date(f.year || '-' || lpad(f.month::text,2,'0') || '-' || lpad(f.day_of_month::text,2,'0'), 'YYYY-MM-DD') AS date,
	AVG(is_delayed) AS reliability
FROM
	reporting.flight f
GROUP BY
	to_date(f.year || '-' || lpad(f.month::text,2,'0') || '-' || lpad(f.day_of_month::text,2,'0'), 'YYYY-MM-DD')
