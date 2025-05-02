#expression returns nanosecond using Extract Function with Size function.
#Negative Case
SELECT id,extract(year from t.ts1) FROM Extract t WHERE size(t.rec.tsr1)>0 