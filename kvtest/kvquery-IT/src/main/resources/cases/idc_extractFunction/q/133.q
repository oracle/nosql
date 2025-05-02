#expression returns day using day() function with timestamp of all nine precision
SELECT id,day(t.ts0) as ts0,day(t.ts1) as ts1,day(t.ts2) as ts2,day(t.ts3) as ts3,day(t.ts4) as ts4,day(t.ts5) as ts5,day(t.ts6) as ts6,day(t.ts7) as ts7 ,day(t.ts8) as ts8 ,day(t.ts9) as ts9 FROM Extract t
