select f.xact.prodcat,
       array_collect(f.xact.acctno) as accounts,
       count(*) as cnt
from Foo f
where f.xact.year = 2000
group by f.xact.prodcat
