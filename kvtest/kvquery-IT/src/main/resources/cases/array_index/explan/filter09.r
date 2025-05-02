compiled-query-plan

{
"query file" : "array_index/q/filter09.q",
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
          "equality conditions" : {},
          "range conditions" : { "rec.d[].d2" : { "start value" : 11, "start inclusive" : false, "end value" : 20, "end inclusive" : false } }
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "ANY_EQUAL",
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
          "value" : 12
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
        "field name" : "d3",
        "input iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "predicate iterator" :
          {
            "iterator kind" : "AND",
            "input iterators" : [
              {
                "iterator kind" : "LESS_THAN",
                "left operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 11
                },
                "right operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "d2",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                }
              },
              {
                "iterator kind" : "LESS_THAN",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "d2",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 20
                }
              }
            ]
          },
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "d",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_FILTER",
              "predicate iterator" :
              {
                "iterator kind" : "GREATER_THAN",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "a",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              },
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
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 12
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