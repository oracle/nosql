compiled-query-plan

{
"query file" : "insert/q/ins11.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "c1" : 8,
  "c2" : 100,
  "t" : null
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$$t1",
  "SELECT expressions" : [
    {
      "field name" : "t1",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$$t1"
      }
    }
  ]
}
}