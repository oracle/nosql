select /*+ force_primary_index(A) */ *
from nested tables(A a descendants(A.B b))
order by a.ida desc
