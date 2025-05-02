select f.xact.acctno, 
       f.xact.year, 
       sum(seq_transform(f.xact.items[] , $.price * $.qty)) as sales
from Bar f
group by f.xact.acctno, f.xact.year
order by sum(seq_transform(f.xact.items[] , $.price * $.qty)) desc, f.xact.acctno
