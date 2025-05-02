compiled-query-plan

{
"query file" : "idc_in_expr/q/q30.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "ComplexType",
      "row variable" : "$$f",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "OP_EXISTS",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "predicate iterator" :
            {
              "iterator kind" : "IN",
              "left-hand-side expressions" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "work",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "home",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                }
              ],
              "right-hand-side expressions" : [
                {
                  "iterator kind" : "ARRAY_FILTER",
                  "input iterator" :
                  {
                    "iterator kind" : "EXTERNAL_VAR_REF",
                    "variable" : "$arr3"
                  }
                }
              ]
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "phones",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "address",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            }
          }
        },
        {
          "iterator kind" : "OP_EXISTS",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "predicate iterator" :
            {
              "iterator kind" : "IN",
              "left-hand-side expressions" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "work",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                }
              ],
              "right-hand-side expressions" : [
                {
                  "iterator kind" : "ARRAY_FILTER",
                  "input iterator" :
                  {
                    "iterator kind" : "EXTERNAL_VAR_REF",
                    "variable" : "$arr4"
                  }
                }
              ]
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "phones",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "address",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            }
          }
        },
        {
          "iterator kind" : "LESS_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "CONST",
            "value" : 9
          },
          "right operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        },
        {
          "iterator kind" : "LESS_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
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
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}