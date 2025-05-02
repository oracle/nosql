#TestDescription: source is boolean type inside json
#Expected result: return false

select id1, regex_like(p.info.id.tier1rated,"true")
from playerinfo p
