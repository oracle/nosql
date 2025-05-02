compiled-query-plan

{
"query file" : "time/q/q09.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "foo2",
      "row variable" : "$$f",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "ARRAY_SLICE",
        "low bound" : 0,
        "high bound" : 0,
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "json",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "2022-03-18T00:00:00.000000000Z"
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
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}