#sin with n that does not resolve to a number
select sin(t.doc.str), sin(t.doc.bool),sin(t.doc.obj) from functional_test t where t.id=7