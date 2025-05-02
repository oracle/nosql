# field step + or opeartor + is of type operator
select *
from A a left outer join A.B b on a.ida1 = b.ida1 and
            (b.b4.comment='positive integer' or b.b4.extra is of type (String))
