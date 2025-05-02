#expression returns millisecond,microsecond,nanosecond using Extract Function with NOT opertor.Operation on Atomic type.
SELECT id,extract(millisecond from t.ts0) as ts0, extract(microsecond from t.ts1) as ts1,extract(nanosecond from t.ts2) as ts2 FROM Extract t WHERE NOT extract(millisecond from t.ts3)>5 OR NOT extract(microsecond from t.ts4)>3 OR NOT extract(nanosecond from t.ts5)>6
