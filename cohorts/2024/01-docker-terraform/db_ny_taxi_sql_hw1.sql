-- #3
-- How many taxi trips were totally made on September 18th 2019?
select
    count(1)
from
    public.green_taxi_data
where
    date(lpep_pickup_datetime) = '2019-09-18'
    AND date(lpep_dropoff_datetime) = '2019-09-18';

-- answer: 15612
--#4
--Which was the pick up day with the largest trip distance 
--Use the pick up time for your calculations.
select
    date(lpep_pickup_datetime) as pickup_day,
    max(trip_distance)
from
    public.green_taxi_data
group by
    1
order by
    max(trip_distance) desc
limit
    1;
--2019-09-26 341.64

--#5
with cte_loc as (
    select
        "LocationID",
        "Borough"
    from
        public.taxi_zone_lookup
    where
        "Borough" != 'Unknown'
)
select
    l."Borough",
    sum(t.total_amount) as ttl_sum
from
    public.green_taxi_data t
    join cte_loc l ON t."PULocationID" = l."LocationID"
where
    date(t.lpep_pickup_datetime) = '2019-09-18'
group by
    l."Borough"
having
    sum(t.total_amount) >= 50000
order by
    sum(t.total_amount) desc;

-- "Brooklyn"	96333.23999999958
-- "Manhattan"	92271.29999999849
-- "Queens"	78671.70999999924
-- "Bronx"	32830.089999999975
-- "Staten Island"	342.59000000000003
--#6
with cte_loc as (
    select
        "LocationID",
        "Zone"
    from
        public.taxi_zone_lookup -- 	where "Zone"='Astoria'
        --7	"Astoria"
)
select
    l."Zone" as DOZone,
    max(tip_amount)
from
    public.green_taxi_data t
    join cte_loc l ON t."DOLocationID" = l."LocationID"
where
    date_part('month', date(lpep_pickup_datetime)) = 9
    and date_part('year', date(lpep_pickup_datetime)) = 2019
    and "PULocationID" = 7
group by
    l."Zone"
order by
    max(tip_amount) desc;

--"JFK Airport"	62.31