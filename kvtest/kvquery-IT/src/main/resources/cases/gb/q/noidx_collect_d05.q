select f.xact.prodcat,
       array_collect(distinct f.xact.acctno) as accounts,
       count(*) as cnt,
       count(distinct f.xact.acctno) as dcnt
from Foo f
where f.xact.year = 2000
group by f.xact.prodcat
order by count(distinct f.xact.acctno), f.xact.prodcat
