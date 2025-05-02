declare $ext1 integer; // = 30
select b.ida as b_ida, b.idb as b_idb,
       a.ida as a_ida,
       d.ida as d_ida, d.idb as d_idb, d.idc as d_idc, d.idd as d_idd,
       e.ida as e_ida, e.idb as e_idb, e.ide as e_ide
from nested tables(A.B b ancestors(A a)
                         descendants(A.B.C.D d on a.ida != 15 and d.idd <= $ext1,
                                     A.B.E e))
order by b.ida, b.idb, d.idc, d.idd, e.ide

