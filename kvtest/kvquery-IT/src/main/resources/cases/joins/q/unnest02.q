select p.idp, c.idc, c.a1, $pa
from NESTED TABLES(P p descendants(P.C c)), c.arr[] as $pa
