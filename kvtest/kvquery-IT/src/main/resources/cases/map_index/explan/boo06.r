compiled-query-plan

{
"query file" : "map_index/q/boo06.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Boo",
      "row variable" : "$$b",
      "index used" : "idx7",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"expenses.\"[]\"":3},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$b",
    "WHERE" : 
    {
      "iterator kind" : "GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "values()",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "expenses",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b"
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 10
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b"
          }
        }
      }
    ]
  }
}
}