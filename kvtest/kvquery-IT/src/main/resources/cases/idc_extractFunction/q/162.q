#expression returns year using year() timestamp function and has is not of type operator
SELECT id,year(t.ts3)  FROM Extract t WHERE t.ts3 is not of type(timestamp)
