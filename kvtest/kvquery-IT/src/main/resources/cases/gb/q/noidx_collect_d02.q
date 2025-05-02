select f.xact.prodcat,
       array_collect(distinct
                     { "acctno" : f.xact.acctno,
                       "amount" : f.xact.item.qty * f.xact.item.price
                     }) as accounts,
       count(*) as cnt
from Foo f
where f.xact.year = 2000
group by f.xact.prodcat
