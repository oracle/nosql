# Error: Invalid expression in ON clause of left outer join: OP_OR

select * from A.B b left outer join A.B.C c on b.ida = c.ida or c.ida > 0