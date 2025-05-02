select a.ida as a_ida,
       b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd,
       e.ida as e_ida, e.idb as e_idb, e.ide as e_ide,
       g.ida as g_ida, g.idg as g_idg,
       j.ida as j_ida, j.idg as j_idg, j.idj as j_idj,
       h.ida as h_ida, h.idg as h_idg, h.idh as h_idh
from nested tables(A a descendants(A.B.C.D d,
                                   A.G.J j,
                                   A.G g,
                                   A.B b,
                                   A.B.C c,
                                   A.B.E e,
                                   A.G.H h))
order by a.ida, b.idb, c.idc, d.idd, e.ide, g.idg, j.idj, h.idh
