select array_collect({ "id1" : f.id1,
                       "id2" : f.id2,
                       "id3" : f.id3,
                       "acctno" : f.xact.acctno,
                       "amount" : f.xact.item.qty * f.xact.item.price
                     }) as accounts,
       count(*) as cnt
from Foo f
