select f.xact.acctno, count(*) as cnt
from Foo f
where f.record.long = 10
group by f.xact.acctno
