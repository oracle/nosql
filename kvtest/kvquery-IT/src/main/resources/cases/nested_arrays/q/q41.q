select id
from foo f
where exists f.info.addresses[$element.state = "CA" and $element.city > "M"]
