compiled-query-plan

{
"query file" : "schemaless/q/ins10.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "majorKey1" : "ab",
  "majorKey2" : "cd",
  "minorKey" : "min6",
  "a" : 3,
  "b" : 5
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$j",
  "SELECT expressions" : [
    {
      "field name" : "j",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$j"
      }
    },
    {
      "field name" : "Column_2",
      "field expression" : 
      {
        "iterator kind" : "FUNC_REMAINING_DAYS",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$j"
        }
      }
    }
  ]
}
}