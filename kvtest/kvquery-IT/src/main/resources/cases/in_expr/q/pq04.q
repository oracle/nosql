select /*+ FORCE_PRIMARY_INDEX(foo) */ 
       id
from foo f
where (foo1, foo2) in ((6, 3.8), (4, 3.5), (4, 3.6))
