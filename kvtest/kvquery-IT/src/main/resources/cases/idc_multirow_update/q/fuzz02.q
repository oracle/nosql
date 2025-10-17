update A a
set ida2 = 9223372036854775807,
add a.ida3 1 "!@#$%^&*()",
set ida4 = 5.0987,
put a.ida5 {"new_entry": cast(-3.4028235E+38 as double)}
where ida1 = "VDVFY#BHJ###hbvfh$%^&*(((()&*@&#vbhdbjvbh"

select *
from A
where ida1 = "VDVFY#BHJ###hbvfh$%^&*(((()&*@&#vbhdbjvbh"