compiled-query-plan

{
"query file" : "multi_index/q/foo4_cov.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "distinct by fields at positions" : [ 0, 1, 2 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$t",
        "index used" : "idx_d_f",
        "covering index" : true,
        "index row variable" : "$$t_idx",
        "index scans" : [
          {
            "equality conditions" : {"rec.d[].d2":10},
            "range conditions" : {}
          }
        ],
        "index filtering predicate" :
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id1",
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
        },
        {
          "field name" : "id2_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t_idx"
            }
          }
        },
        {
          "field name" : "id3_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t_idx"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "id1",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "id1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}