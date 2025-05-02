#expression returns millisecond,microsecond,nanosecond and has logical operator NOT using timestamp function for Atomic type
SELECT id,millisecond(t.ts3),microsecond(t.ts3),nanosecond(t.ts8)  FROM Extract t WHERE NOT id=2 OR id=10 AND exists t.json
