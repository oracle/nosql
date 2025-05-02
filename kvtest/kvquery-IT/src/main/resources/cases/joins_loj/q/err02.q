# Error: Table A.G.J is not descendant of the table A.B.C.D

select a.ida as a_ida,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd,
       j.ida as j_ida, j.idg as j_idg, j.idj as j_idj
from A a left outer join A.B.C.D d on a.ida = d.ida
         left outer join A.G.J j on d.ida = j.ida and d.idb > 10
order by a.ida
