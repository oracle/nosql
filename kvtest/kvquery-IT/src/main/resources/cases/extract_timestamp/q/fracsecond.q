SELECT millisecond(t.ts0), microsecond(t.ts0), nanosecond(t.ts0),  
	   millisecond(t.rec.ts1), microsecond(t.rec.ts1), nanosecond(t.rec.ts1),
       millisecond(t.rec.ts3), microsecond(t.rec.ts3), nanosecond(t.rec.ts3) 
FROM Foo t WHERE id = 1