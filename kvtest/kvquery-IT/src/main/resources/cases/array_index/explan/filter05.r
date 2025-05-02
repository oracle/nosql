compiled-query-plan

{
"query file" : "array_index/q/filter05.q",
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
      "row variable" : "$$f",
      "index used" : "idx_a_c_f",
      "covering index" : false,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "ANY_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec.c[].ca",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
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
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
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
                "value" : 10
              },
              "right operand" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$element"
              }
            },
            {
              "iterator kind" : "LESS_OR_EQUAL",
              "left operand" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$element"
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
          "field name" : "b",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "predicate iterator" :
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
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 3
              }
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "rec",
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