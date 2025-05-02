select f.xact.acctno,
       sum(seq_transform(f.xact.items[] , $.price * $.qty)) as sales
from Bar f
where f.xact.year = 2000 and f.xact.items[].qty >any 2
group by f.xact.acctno
