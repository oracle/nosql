#exp with n that does not resolve to a number
select exp(t.doc.str), exp(t.doc.bool),exp(t.doc.obj) from functional_test t where t.id=7