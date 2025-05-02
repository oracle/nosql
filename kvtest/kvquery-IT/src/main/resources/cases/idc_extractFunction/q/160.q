#expression returns year using year() timestamp function and has not exists operator
SELECT id,year(t.ts3)  FROM Extract t WHERE not exists t.ts4
