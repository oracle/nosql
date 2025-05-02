# order by + where + timestamp function
select g.ida1, g.idg1, g4, h.idh1, h.idh2, h3, h4 from nested tables (A.G g ancestors (A a) descendants (A.G.H h)) where g.g4 < cast('2018-04-01' as timestamp) order by g.ida1, g.idg1
