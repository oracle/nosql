compiled-query-plan

{
"query file" : "schemaless/q/ins05.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "user_id" : 1
},
    "column positions" : [ 0, -1, -1 ],
    "value iterators" : [
      {
        "iterator kind" : "CAST",
        "target type" : "Integer",
        "quantifier" : "?",
        "input iterator" :
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$id5"
        }
      },
      {
        "iterator kind" : "CAST",
        "target type" : "Json",
        "quantifier" : "?",
        "input iterator" :
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$doc1"
        }
      },
      {
        "iterator kind" : "CAST",
        "target type" : "Json",
        "quantifier" : "?",
        "input iterator" :
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$doc2"
        }
      }
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