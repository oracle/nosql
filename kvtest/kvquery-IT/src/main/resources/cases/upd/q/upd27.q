update foo f 
put f.info.children $.values() 
where id = 22

select f.info.children
from foo f
where id = 22
