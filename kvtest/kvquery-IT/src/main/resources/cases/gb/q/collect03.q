select f.xact.acctno,
       array_collect({ "prodcat" : f.xact.prodcat,
                       "qty" : f.xact.item.qty 
                     }) as collect,
       count(*) as cnt
from Foo f
where f.xact.year  = 2000
group by f.xact.acctno
