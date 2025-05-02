#Test Description: regex_like function in update predicate. 
# Type: Negative.

update foo f
set f.str = "ABC"
where regex_like(str,"abc")
