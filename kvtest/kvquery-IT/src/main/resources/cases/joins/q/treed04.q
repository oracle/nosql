select a.ida as a_ida,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd,
       e.ida as e_ida, e.idb as e_idb, e.ide as e_ide,
       f.ida as f_ida, f.idf as f_idf,
       j.ida as j_ida, j.idg as j_idg, j.idj as j_idj,
       h.ida as h_ida, h.idg as h_idg, h.idh as h_idh
from nested tables(A a descendants(A.B.C.D d,
                                   A.G.J j,
                                   A.F f,
                                   A.B.E e,
                                   A.G.H h))
order by a.ida
