compiled-query-plan

{
"query file" : "array_index/q/slicing09.q",
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
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "rec.d[].d2" : { "start value" : 15, "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "ANY_GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "d2",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 1,
              "high bound" : 3,
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
        {
          "iterator kind" : "ANY_GREATER_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "d2",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 2,
              "high bound" : 3,
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
        }
      ]
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