
#
# This query will raise an IAE during runtime.
#
# Actually this has been fixed in KVSTORE-968. But I leave this comment here
# for historical reasons.
#
# This is not technically a bug, but it is VERY subtle (and confusing). So, here is
# what is going on:
#
# The query is rewritten internally as
#
# select [| f.complex1.map.values() |] from foo f;
#
# where I use "[| |]" as a symbol for the *conditional* array constructor added 
# by the SELECT clause. [| |] is added because the expr i.complex1.map.values() 
# may return more than one items.
#
# Now, remember that the end result of any query is a set of records. In this 
# case, each record will have just one field. What should be the data type of 
# that field? The compiler cannot know whether i.complex1.map.values() will 
# actually return one item or more. So, as far as the compiler can tell,
# [| i.complex1.map.values() |] will return either an array of "something" or a 
# single "something". As a result, and because in this case the "something" is 
# a subtype of JSON, the compiler assigns type JSON to the expression 
# [| i.complex1.map.values() |] and to the field of the result record type. This
# means that if an array is indeed constructed during runtime, the type of that
# array will be ARRAY(JSON) (because the array has to be inserted into a record
# field of type JSON). And this leads to the observed IAE: For the single row in
# table idxtest, i.complex1.map.values() returns 2 strongly-typed arrays, which
# have to be inserted into an ARRAY(JSON), which is not allowed.
#
# For now the only "fix" is a user fix: the user should put an explicit array
# constructor around i.complex1.map.values(). I think this is ok for people who
# work only with strongly typed data. Such people know their data and know what
# to expect, so they should use the explicit array constructor in this case
# (which would also give them predictable results). In other words, implicit
# and conditional array construction was added to help pure JSON apps, and it
# makes sense there.
#
# For the future, I think we should have a "json" mode as a config param for
# each query. Setting the json mode to off, will effectively remove the JSON
# type from the type system, and also add some extra type checking. 
#
select f.complex1.map.values() 
from foo f
