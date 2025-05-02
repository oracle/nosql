compiled-query-plan

{
"query file" : "schemaless/q/ins03.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "acct_id" : 3,
  "user_id" : 1,
  "a" : [10, 11, 12],
  "b" : {
    "c" : "xyz",
    "d" : {
      "e" : [1]
    }
  }
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$$viewers",
  "SELECT expressions" : [
    {
      "field name" : "viewers",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$$viewers"
      }
    }
  ]
}
}