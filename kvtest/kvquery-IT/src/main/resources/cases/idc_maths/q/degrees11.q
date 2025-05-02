#degrees with n that does not resolve to a number
select degrees(t.doc.str), degrees(t.doc.bool),degrees(t.doc.obj) from functional_test t where t.id=7