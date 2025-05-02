#expression returns hour using hour() function with timestamp of all nine precision
SELECT id,hour(t.ts0) as ts0,hour(t.ts1) as ts1,hour(t.ts2) as ts2,hour(t.ts3) as ts3,hour(t.ts4) as ts4,hour(t.ts5) as ts5,hour(t.ts6) as ts6,hour(t.ts7) as ts7 ,hour(t.ts8) as ts8 ,hour(t.ts9) as ts9 FROM Extract t
