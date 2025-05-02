#expression returns hour using hour() timestamp function for Atomic Type String using cast
SELECT id,hour(t.s)  FROM Extract t 
