select distinct count(*) as count
from Foo f
group by f.xact.acctno, f.xact.year
