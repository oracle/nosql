select a.ida1 as a_ida1, a.a2 as a_a2, a.a3 as a_a3 from A a left outer join A.B b on a.ida1 = b.ida1 where a.a2 is not null and (a.a2 = 2147483647 or a.a2 = -2147483648)
