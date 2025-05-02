compiled-query-plan

{
"query file" : "in_expr/q/q25.q",
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
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_bar1234",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.bar1":7,"info.bar2":3.0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":7,"info.bar2":3.4},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":7,"info.bar2":3.6},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "LESS_OR_EQUAL",
        "left operand" :
        {
          "iterator kind" : "CONST",
          "value" : 3.1
        },
        "right operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.bar2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      }
    ]
  }
}
}