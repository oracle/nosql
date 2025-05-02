#cot with n that does not resolve to a number
select cot(t.doc.str), cot(t.doc.bool),cot(t.doc.obj) from functional_test t where t.id=7