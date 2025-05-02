#expression returns year using year() function with timestamp of all nine precision
SELECT id,year(t.ts0) as ts0,year(t.ts1) as ts1,year(t.ts2) as ts2,year(t.ts3) as ts3,year(t.ts4) as ts4,year(t.ts5) as ts5,year(t.ts6) as ts6,year(t.ts7) as ts7 ,year(t.ts8) as ts8 ,year(t.ts9) as ts9 FROM Extract t
