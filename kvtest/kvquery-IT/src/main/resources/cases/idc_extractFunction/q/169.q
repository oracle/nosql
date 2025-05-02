#expression returns year using year() timestamp function for json atomic using cast
#2038 problem.Positive case
SELECT id,year(cast(t.json.y38 as timestamp))  FROM Extract t 
