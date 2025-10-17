select id
from foo $f
where row_metadata($f)[3].children.Anna.age = 10
