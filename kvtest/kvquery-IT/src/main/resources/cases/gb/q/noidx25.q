select number, sum(decimal) as sum
from numbers
where id < 10
group by number
