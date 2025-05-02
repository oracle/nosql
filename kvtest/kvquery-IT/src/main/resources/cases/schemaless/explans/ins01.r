compiled-query-plan

{
"query file" : "schemaless/q/ins01.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "acct_id" : 1,
  "user_id" : 1,
  "a" : 1,
  "b" : 2
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