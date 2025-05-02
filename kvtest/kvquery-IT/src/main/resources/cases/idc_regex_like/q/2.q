#TestDescription: Input string is variable of typr ANY_JSON_ATOMIC.STRING

declare $inputJson json;

select regex_like($inputJson.name, "A.*")
from playerinfo
where id1=1
