select
  f.record.long + f.record.int + 1 as c1, 
  1+ f.record.long + f.record.int as c2,
  2 * (1 + 5 + f.record.long + f.record.int - 3 - 6) + 4 as c3,
  sum(f.record.int * f.record.double) + sum(f.record.double) as sum
from Foo f
group by f.record.long + f.record.int

