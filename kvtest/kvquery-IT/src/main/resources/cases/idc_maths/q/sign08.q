#sign with n that does not resolve to a number
select sign(t.doc.str), sign(t.doc.bool),sign(t.doc.obj) from functional_test t where t.id=7