update foo f 
add f.info.phones 1 $[$element.areacode > 400]
where id = 23

select f.info.phones
from foo f
where id = 23
