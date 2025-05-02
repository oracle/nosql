#expression returns week,isoweek using Extract Function with Value Comparision Operator =,!=.Operation on Atomic type and Record.
SELECT id,extract(week from t.ts0) as ts0, extract(isoweek from t.rec.tsr1) as tsr1 FROM Extract t WHERE extract(week from t.ts1)=5 OR extract(isoweek from t.rec.tsr5)!=12 
