#tan with n that does not resolve to a number
select tan(t.doc.str), tan(t.doc.bool),tan(t.doc.obj) from functional_test t where t.id=7