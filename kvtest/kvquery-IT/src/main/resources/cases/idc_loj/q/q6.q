select a.ida1 as a_ida1, j.idj1 as j_idj1 
from A a left outer join A.G g on a.ida1 = g.ida1 
         left outer join A.G.H h on g.ida1 = h.ida1 and g.idg1 = h.idg1 
         left outer join A.G.H.I i on i.ida1 = a.ida1 and i.idg1 = g.idg1 
                                   and i.idh1 = h.idh1 and i.idh2 = h.idh2 
                                   and i.ida1 = h.ida1 and h.idg1 = i.idg1 
                                   and i.ida1 = g.ida1 
         left outer join A.G.H.I.J  j on j.ida1 = a.ida1 and j.idg1 = g.idg1
                                   and j.idh1 = h.idh1 and j.idh2 = h.idh2 
                                   and j.idi1 = j.idi1 and i.ida1 = j.ida1 
                                   and i.idg1 = j.idg1 and i.idh1 = j.idh1 
                                   and i.idh2 = j.idh2 and h.ida1 = j.ida1 
                                   and h.idg1 = j.idg1 and g.ida1 = j.ida1 
                                   and  i.idi1 = j.idi1 
         left outer join A.G.H.I.J.K k on k.ida1 = a.ida1 and k.idg1 = g.idg1
                                   and k.idh1 = h.idh1 and k.idh2 = h.idh2 
                                   and k.idi1 = i.idi1 and k.idj1 = j.idj1 
                                   and j.ida1 = k.ida1 and j.idg1 = k.idg1 
                                   and j.idh1 = k.idh1 and j.idi1 = k.idi1 
                                   and i.ida1 = k.ida1 and i.idg1 = k.idg1 
                                   and i.idh1 = k.idh1 and i.idh2 = k.idh2 
                                   and h.ida1 = k.ida1 and h.idg1 = k.idg1 
                                   and g.ida1 = k.ida1 and j.idh2 = k.idh2 
         left outer join A.G.H.I.J.K.L l on l.ida1 = a.ida1 and l.idg1 = g.idg1 
                                   and l.idh1 = h.idh1 and l.idh2 = h.idh2 
                                   and l.idi1 = i.idi1 and l.idj1 = j.idj1 
                                   and l.idk1 = k.idk1 and l.ida1 = k.ida1 
                                   and l.idg1 = k.idg1 and l.idh1 = k.idh1 
                                   and l.idh2 = k.idh2 and l.idi1 = k.idi1 
                                   and l.idj1 = k.idj1 and l.ida1 = j.ida1 
                                   and l.idg1 = j.idg1 and l.idh1 = j.idh1 
                                   and l.idh2 = j.idh2 and l.idi1 = j.idi1 
                                   and l.ida1 = i.ida1 and l.idg1 = i.idg1 
                                   and l.idh1 = i.idh1 and l.idh2 = i.idh2 
                                   and l.ida1 = h.ida1 and l.idg1 = h.idg1 
                                   and l.ida1 = g.ida1 
where a.ida1 != "A11_Dgp2YJJdeO" 
order by a.ida1
