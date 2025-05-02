compiled-query-plan

{
"query file" : "multi_index/q/foo6.q",
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
          "equality conditions" : {},
          "range conditions" : { "rec.a" : { "start value" : 0, "start inclusive" : false } }
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "GREATER_OR_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec.f",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 0.0
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$t",
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