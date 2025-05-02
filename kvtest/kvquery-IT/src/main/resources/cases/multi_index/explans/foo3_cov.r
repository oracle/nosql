compiled-query-plan

{
"query file" : "multi_index/q/foo3_cov.q",
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
      "index used" : "idx_a_c_f",
      "covering index" : true,
      "index row variable" : "$$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"rec.a":10,"rec.c[].ca":3},
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
    "FROM variable" : "$$t_idx",
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        }
      }
    ]
  }
}
}