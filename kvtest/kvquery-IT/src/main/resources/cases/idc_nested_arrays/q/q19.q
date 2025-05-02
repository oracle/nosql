select id
from nestedTable nt
where exists  nt.addresses[$element.city > "R"]
