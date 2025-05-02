# index_storage_size on target and on secondary index used by query

select $a.ida as a_ida, $a.a1, 
       $d.ida as d_ida, $d.idb as d_idb, $d.idc as d_idc, $d.idd as d_idd,
       20 < index_storage_size($a, "a_idx_c1") and
            index_storage_size($a, "a_idx_c1") < 28 as size_a_idx_c1,
       50 < index_storage_size($d, "d_idx_d2_idb_c3") and
            index_storage_size($d, "d_idx_d2_idb_c3") < 65 as size_d_idx_d2_idb_c3 
from nested tables(A $a descendants(A.B.C.D $d))
where $a.c1 > 10
order by $a.ida
