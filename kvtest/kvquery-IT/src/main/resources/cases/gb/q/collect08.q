select f.xact.state,
       array_collect({ "items" : f.xact.items, "acctno" : f.xact.acctno }) as collect
from bar f
group by f.xact.state
