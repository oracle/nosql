#expression returns nanosecond using a parenthesized expression with Extract Operator.
SELECT id,extract(nanosecond from t.ts0) as ts0, extract(nanosecond from t.ts9) as ts9 FROM Extract t WHERE extract(second from t.ts1)>any 5 OR (extract(isoweek from t.rec.tsr5)!=any 12) AND  extract(week from t.ts0) >=any 0 
