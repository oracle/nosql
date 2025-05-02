select distinct a.ida1 as a_ida1, g.g2 as g_g2, h.h3 as h_h3 from A.G g left outer join A a on g.ida1 = a.ida1
           left outer join A.G.H h on g.ida1 = h.ida1 and g.idg1 = h.idg1 where
                                 (h.h3 = 9223372036854775807 or h.h3 = -9223372036854775808)
