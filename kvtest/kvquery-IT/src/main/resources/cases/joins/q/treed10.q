select a.ida as a_ida, a.a1,
       b.ida as b_ida, b.idb as b_idb,
       c.ida as c_ida, c.idb as c_idb, c.idc as c_idc,
       k.ida as k_ida, k.idg as k_idg, k.idj as k_idj, k.idk as k_idk
from nested tables(A a descendants(A.B b, A.B.C c, A.G.J.K k))
where a.a2 < 30
