select e.info.age, sum(e.info.income) AS sum_of_income
from employee e 
group by e.info.age
