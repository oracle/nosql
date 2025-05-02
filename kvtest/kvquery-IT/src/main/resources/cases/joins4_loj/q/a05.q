# on clause + value comparison operator + logical operator + is null + timestamp functions
select a.ida1, a2, a3, g.idg1, g.g2, g.g3, g.g4, h.idh1, h.idh2, h.h3, h.h4
from A.G.H h left outer join A a on h.ida1 = a.ida1 and a.a2 is not null
             left outer join A.G g on h.ida1 = g.ida1 and h.idg1 = g.idg1 and
                                      (month(g.g4) <= 3 or g.g4 is null)
