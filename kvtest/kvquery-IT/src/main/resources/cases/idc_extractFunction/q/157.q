#expression returns week,isoweek with value comparision operator <,>,=  using timestamp function for Atomic and record type
SELECT id,week(t.ts3),isoweek(t.rec.tsr3)  FROM Extract t WHERE week(t.ts3)>2 OR isoweek(t.rec.tsr3)<55 OR day(t.ts1)=2
