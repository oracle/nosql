compiled-query-plan

{
"query file" : "insert/q/ins05.q",
"plan" : 
{
  "iterator kind" : "INSERT_ROW",
  "row to insert (potentially partial)" : 
{
  "str" : "abc",
  "id1" : 1,
  "id2" : 105,
  "info" : null,
  "rec1" : {
    "long" : 120,
    "rec2" : {
      "str" : "dfg",
      "num" : 1E+1,
      "arr" : [10, 4, 6]
    },
    "recinfo" : null,
    "map" : {
      "bar" : "dff",
      "foo" : "xyz"
    }
  }
},
  "value iterators" : [

  ],
  "TTL iterator" :
  {
    "iterator kind" : "CONST",
    "value" : 3
  }
}
}