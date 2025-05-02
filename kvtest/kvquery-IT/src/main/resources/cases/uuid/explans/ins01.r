compiled-query-plan
{
"query file" : "uuid/q/ins01.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "uid" : "18acbcb90137b04fc8099f70812f20240356",
  "int" : 1
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$$bad1",
  "SELECT expressions" : [
    {
      "field name" : "bad1",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$$bad1"
      }
    }
  ]
}
}
