update Foo f
set f.info.values[] = 
    case when $ is of type (NUMBER) then $ + 10
         when $ is of type (string) then seq_concat()
         when $ is of type (map(any)) then {"new":"map"}
         else "????"
    end
where id = 9

select f.info.values
from Foo f
where id = 9
