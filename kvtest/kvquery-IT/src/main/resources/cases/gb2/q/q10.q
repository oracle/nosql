select ADOU, count(*), count(ASTR), sum(ADOU), avg(ADOU), max(ADOU), min(ADOU) 
from T2 
group by ADOU
