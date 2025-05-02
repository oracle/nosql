# Error: Table A.B is neither ancestor nor descendant of the target table A.B

select *
from A b1 left outer join A.B b2 on b1.ida = b2.ida and b1.idb = b2.idb
