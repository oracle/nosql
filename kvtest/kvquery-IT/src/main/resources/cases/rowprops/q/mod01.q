select id,
       year(modification_time($f)) >= 2020
from foo $f
where $f.address.state = "MA"
