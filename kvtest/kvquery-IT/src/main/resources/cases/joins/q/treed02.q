select a.ida as a_ida,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd,
       j.ida as j_ida, j.idg as j_idg, j.idj as j_idj
from nested tables(A a descendants(A.B.C.D d, A.G.J j))
order by a.ida


