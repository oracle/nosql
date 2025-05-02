#abs with n that does not resolve to a number
select abs(t.doc.str), abs(t.doc.bool),abs(t.doc.obj) from functional_test t where t.id=7