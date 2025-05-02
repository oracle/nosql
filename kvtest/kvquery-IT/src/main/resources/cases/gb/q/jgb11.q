select f.xact.acctno, 
       sum(seq_transform(f.xact.items[] , $.price * $.qty)) as sales
from Bar f
where f.xact.year = 2000
group by f.xact.acctno
