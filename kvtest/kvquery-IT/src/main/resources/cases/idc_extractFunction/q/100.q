#expression returns week,isoweek using Extract Function with Sequence Comparision Operator =any,!=any.Operation on Atomic type and Record.
SELECT id,extract(week from t.ts0) as ts0, extract(isoweek from t.ts1) as ts1 FROM Extract t WHERE extract(week from t.ts1)!=any 5 OR extract(isoweek from t.rec.tsr5)!=any 12 AND  extract(week from t.ts0) =any 0 
