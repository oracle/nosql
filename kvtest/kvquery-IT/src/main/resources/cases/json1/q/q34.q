select id, f.info.address.(f.info.children.keys($key = "John")) 
from foo f

