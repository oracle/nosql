compiled-query-plan

{
"query file" : "json_idx/q/partial_mq02.q",
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
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "idx_children_both",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.children.keys()":"Anna","info.children.values().age":10},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "age",
        "input iterator" :
        {
          "iterator kind" : "ARRAY_SLICE",
          "low bound" : 3,
          "high bound" : 3,
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "Anna",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "children",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            }
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
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}