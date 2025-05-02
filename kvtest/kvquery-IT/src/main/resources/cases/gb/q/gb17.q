select acctno, count(b.xact.year)
from boo b
group by acctno
