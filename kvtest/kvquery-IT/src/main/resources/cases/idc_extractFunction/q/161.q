#expression returns year using year() timestamp function and has is of type operator
SELECT id,year(t.ts3)  FROM Extract t WHERE t.ts3 is of type(timestamp)
