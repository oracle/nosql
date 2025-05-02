# Error: at (3, 53) Table alias $$c cannot be referenced at this location
select *
from A.B.C c left outer join A a on c.ida = a.ida and
                                    (a.c1 is null or c.c1 = a.c1)
             left outer join A.B b on c.ida = b.ida and c.idb = b.idb
