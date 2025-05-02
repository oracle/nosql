select id, num
from NumTable n
where n.json[0] > num
