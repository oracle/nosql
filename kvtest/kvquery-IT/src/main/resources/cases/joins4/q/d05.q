# arithmetic expressions + secondary index A(a3)
select a.ida1, a3, d.idd1, d2, d3, d2 + d3 as sum, h.idh1, h.idh2 from nested tables (A a descendants (A.B.D d on d2 > 0 and d3 > 4005, A.G.H h on h.h4.hkey1 is not null)) where a3 = 'A33qcyUB24Iy2Vgy0YJ'
