compiled-query-plan

{
"query file" : "gb/q/gb15.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$f",
        "index used" : "primary index",
        "covering index" : true,
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "id1" : { "end value" : 1, "end inclusive" : false } }
          }
        ],
        "index filtering predicate" :
        {
          "iterator kind" : "LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        },
        "position in join" : 0
      },
      "FROM variable" : "$$f",
      "GROUP BY" : "No grouping expressions",
      "SELECT expressions" : [
        {
          "field name" : "Column_1",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "No grouping expressions",
  "SELECT expressions" : [
    {
      "field name" : "Column_1",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "Column_1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ]
}
}