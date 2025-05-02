#floor with n that does not resolve to a number
select floor(t.doc.str), floor(t.doc.bool),floor(t.doc.obj) from functional_test t where t.id=7