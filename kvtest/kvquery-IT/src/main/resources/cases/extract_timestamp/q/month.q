SELECT month(t.ts0), month(t.rec.ts3), month(t.mts6.values($key="k1")), month(t.ats9[0]) FROM Foo t ORDER BY id
