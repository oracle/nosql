#TestDescription: regex_like function on child table

select regex_like(t.desc, "n.*one","i") 
from playerinfo.desc t
where t.id3=1
