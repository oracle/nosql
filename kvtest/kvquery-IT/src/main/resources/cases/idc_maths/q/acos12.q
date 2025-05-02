#acos with n that does not resolve to a number
select acos(t.doc.str), acos(t.doc.bool),acos(t.doc.obj) from functional_test t where t.id=7