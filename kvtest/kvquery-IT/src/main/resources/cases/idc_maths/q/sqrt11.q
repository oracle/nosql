#sqrt with n that does not resolve to a number
select sqrt(t.doc.str), sqrt(t.doc.bool),sqrt(t.doc.obj) from functional_test t where t.id=7