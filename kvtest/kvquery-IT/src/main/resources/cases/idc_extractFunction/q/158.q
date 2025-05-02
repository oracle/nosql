#expression returns week,isoweek with sequence comparision operator <any,>=any  using timestamp function for Atomic type
SELECT id,week(t.ts3),isoweek(t.rec.tsr3)  FROM Extract t WHERE week(t.ts3)>=2 OR isoweek(t.rec.tsr3)<any 55 OR day(t.ts1)>=any 2
