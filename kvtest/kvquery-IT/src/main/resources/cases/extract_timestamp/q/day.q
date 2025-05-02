SELECT day(t.ts0), day(t.rec.ts3), day(t.mts6.values($key="k1")), day(t.ats9[0])  FROM Foo t ORDER BY id
