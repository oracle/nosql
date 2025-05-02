# on clause + value comparison operator + logical operator + is null + timestamp functions
select a.ida1, a2, a3, g.idg1, g.g2, g.g3, g.g4, h.idh1, h.idh2, h.h3, h.h4 from nested tables (A.G.H h ancestors (A a on a2 is not null, A.G g on month(g.g4) <= 3 or g.g4 is null))
