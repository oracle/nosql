#expression returns year using year() timestamp function with size() function for record
#Negative Case since query is on the record with timestamp 
SELECT id,year(t.rec.tsr3)  FROM Extract t WHERE size(t.rec.tsr3)>0
