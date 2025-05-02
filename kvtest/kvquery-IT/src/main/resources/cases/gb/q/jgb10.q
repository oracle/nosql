select f.xact.year, 
       sum(seq_transform(f.xact.items[] , $.price * $.qty)) as sales
from Bar f
where f.xact.acctno = 345
group by f.xact.year
