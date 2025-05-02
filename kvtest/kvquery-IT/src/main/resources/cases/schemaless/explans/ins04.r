compiled-query-plan

{
"query file" : "schemaless/q/ins04.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "acct_id" : 4,
  "user_id" : 1
},
    "column positions" : [ -1 ],
    "value iterators" : [
      {
        "iterator kind" : "CAST",
        "target type" : "Json",
        "quantifier" : "?",
        "input iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "a"
            },
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$a"
            },
            {
              "iterator kind" : "CONST",
              "value" : "b"
            },
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : false,
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 3
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "a"
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : "c"
            },
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$c"
            },
            {
              "iterator kind" : "CONST",
              "value" : "d"
            },
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
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