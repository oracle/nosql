compiled-query-plan

{
"query file" : "array_index/q/slicing04.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$t",
      "index used" : "idx_d_f",
      "covering index" : false,
      "index row variable" : "$$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"rec.d[].d2":15},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec.d[].d3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 3
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "ANY_LESS_OR_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "d2",
        "input iterator" :
        {
          "iterator kind" : "ARRAY_SLICE",
          "low bound" : 0,
          "high bound" : 1,
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "d",
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
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 15
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
            "variable" : "$$t"
          }
        }
      }
    ]
  }
}
}