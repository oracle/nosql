#radians with n that does not resolve to a number
select radians(t.doc.str), radians(t.doc.bool),radians(t.doc.obj) from functional_test t where t.id=7