# Error: Table A is not descendant of the table A.B.C
select * from A.B b left outer join A.B.C c on b.ida = c.ida and b.idb = c.idb
					left outer join A a on b.ida = a.ida
