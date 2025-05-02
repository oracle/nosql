# size() + cast()
select a.ida1, b.idb1, b.idb2, f.idf1, f.idf2, f.idf3, g.idg1, g.g3 from nested tables (A a descendants (A.B b, A.F f on f.f4 is not null, A.G g on size(g.g3) > 1)) where g.idg1 > cast('2018-02-01' as timestamp)
