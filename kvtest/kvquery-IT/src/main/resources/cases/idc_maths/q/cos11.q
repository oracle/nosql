#cos with n that does not resolve to a number
select cos(t.doc.str), cos(t.doc.bool),cos(t.doc.obj) from functional_test t where t.id=7