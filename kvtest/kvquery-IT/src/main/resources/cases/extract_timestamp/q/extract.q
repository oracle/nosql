SELECT extract(year from t.ts0) as year, 
	   extract(month from t.ts0) as month,
	   extract(day from t.ts0) as day,
	   extract(hour from t.ts0) as hour,
	   extract(minute from t.ts0) as min,
	   extract(second from t.ts0) as sec,
	   extract(millisecond from t.ats9[0]) as ms,	   
	   extract(microsecond from t.ats9[0]) as us,	   
	   extract(nanosecond from t.ats9[0]) as ns,
	   extract(week from t.rec.ts1) as week,
	   extract(isoweek from t.rec.ts1) as isoweek,	   
	   extract(quarter from t.rec.ts1) as quarter,
	   extract(day_of_week from t.ts0) as day_of_week,
	   extract(day_of_month from t.rec.ts1) as day_of_month,
	   extract(day_of_year from t.ats9[0]) as day_of_year
FROM Foo t 
ORDER BY id