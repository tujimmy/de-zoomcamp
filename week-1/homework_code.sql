Question 1:

docker build --help

Question 2:

docker run -it python:3.9 bash
pip list

Question 3:

select 
cast(lpep_dropoff_datetime as date) as day,
cast(lpep_pickup_datetime as date) as day, count(1)
from 
green_taxi_trips y
where cast(lpep_dropoff_datetime as date) = '2019-01-15'
and cast(lpep_pickup_datetime as date) = '2019-01-15'
group by cast(lpep_dropoff_datetime as date),
cast(lpep_pickup_datetime as date)

Question 4:

select 
cast(lpep_pickup_datetime as date) as day, max(trip_distance) as trip

from 
green_taxi_trips y
-- where cast(lpep_dropoff_datetime as date) = '2019-01-15'
-- and cast(lpep_pickup_datetime as date) = '2019-01-15'
group by cast(lpep_pickup_datetime as date)
order by trip desc

Question 5:

select 
cast(lpep_pickup_datetime as date) as pickup,
-- cast(lpep_dropoff_datetime as date) as dropoff,
passenger_count,
count(1)

from 
green_taxi_trips y
where 
-- cast(lpep_dropoff_datetime as date) = '2019-01-01'
cast(lpep_pickup_datetime as date) = '2019-01-01'
and passenger_count in ('2', '3')
group by
cast(lpep_pickup_datetime as date),
-- cast(lpep_dropoff_datetime as date),
passenger_count


Question 6:

select 
-- y."PULocationID",
-- y."DOLocationID",
-- z.*
z."Zone",
max(y."tip_amount") as tip
from 
green_taxi_trips y JOIN zones z
ON y."DOLocationID" = z."LocationID"
WHERE z."Zone" in ('Central Park', 'Jamaica', 'South Ozone Park', 'Long Island City/Queens Plaza')
AND y."PULocationID" in ('7', '8', '179')
GROUP BY z."Zone"


SELECT distinct "Zone", "LocationID"
from zones
WHERE "Zone" like '%Astoria%'