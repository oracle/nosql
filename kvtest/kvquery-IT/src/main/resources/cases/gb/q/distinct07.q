select distinct f.record.long, f.record.int
from foo f
where id1 = 0 and
      345 < f.xact.acctno and  f.xact.acctno < 500 and
      (f.xact.year = null or (1990 <  f.xact.year and f.xact.year < 2020))
order by f.record.int + f.record.long, f.record.int
