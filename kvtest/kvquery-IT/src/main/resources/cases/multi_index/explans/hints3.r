compiled-query-plan

{
"query file" : "multi_index/q/hints3.q",
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
      "row variable" : "$$t",
      "index used" : "idx_a_rf",
      "covering index" : false,
      "index row variable" : "$$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"rec.a":10},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "GREATER_THAN",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 0
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "ca",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "c",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "rec",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 3
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "t",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$t"
        }
      }
    ]
  }
}
}