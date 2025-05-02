#expression returns month using month() function with timestamp of all nine precision
SELECT id,month(t.ts0) as ts0,month(t.ts1) as ts1,month(t.ts2) as ts2,month(t.ts3) as ts3,month(t.ts4) as ts4,month(t.ts5) as ts5,month(t.ts6) as ts6,month(t.ts7) as ts7 ,month(t.ts8) as ts8 ,month(t.ts9) as ts9 FROM Extract t
