SELECT id, 
       day_of_week(t.ts0) as d0, 
       day_of_month(t.rec.ts3) as d1, 
       day_of_year(t.mts6.values($key="k1")) as d2, 
       day_of_week(t.ats9[0]) as d3
FROM Foo t 
ORDER BY id
