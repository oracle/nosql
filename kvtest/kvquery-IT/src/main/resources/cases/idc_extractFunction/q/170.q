#expression returns hour using hour() timestamp function for json atomic using cast
#2038 problem. Positive case
SELECT id,hour(cast(t.json.Y38 as timestamp))  FROM Extract t 
