#TestDescription: Input string is variable of type boolean

declare $inputBool boolean;

select regex_like($inputBool, "A.*")
from playerinfo
where id1=1
