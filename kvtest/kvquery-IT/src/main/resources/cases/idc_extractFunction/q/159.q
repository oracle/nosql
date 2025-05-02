#expression returns year using year() timestamp function and has exists operator
SELECT id,year(t.ts3)  FROM Extract t WHERE exists t.ts4
