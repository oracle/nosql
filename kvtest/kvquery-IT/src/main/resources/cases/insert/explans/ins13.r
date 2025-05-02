compiled-query-plan

{
"query file" : "insert/q/ins13.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "id" : 5,
  "name" : "jack"
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$$tIdentity",
  "SELECT expressions" : [
    {
      "field name" : "tIdentity",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$$tIdentity"
      }
    }
  ]
}
}