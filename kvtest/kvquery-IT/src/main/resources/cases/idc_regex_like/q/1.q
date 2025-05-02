#TestDescription: Input string is a variable of type string

declare $inputStr string;

select regex_like($inputStr, "Pass.*1")
from playerinfo
where id1=1
