select f.xact.acctno,
       array_collect({ "prodcat" : f.xact.prodcat,
                       "year" : f.xact.year 
                     }) as collect,
       count(*) as cnt
from Foo f
group by f.xact.acctno
