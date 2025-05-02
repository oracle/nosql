compiled-query-plan

{
"query file" : "maths/q/ln04.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "order by fields at positions" : [ 10 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "math_test",
        "row variable" : "$$math_test",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"id":6},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"id":7},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$math_test",
      "SELECT expressions" : [
        {
          "field name" : "ic",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ic",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$math_test"
            }
          }
        },
        {
          "field name" : "lnic",
          "field expression" : 
          {
            "iterator kind" : "LN",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "ic",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$math_test"
                }
              }
            ]
          }
        },
        {
          "field name" : "lc",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "lc",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$math_test"
            }
          }
        },
        {
          "field name" : "lnlc",
          "field expression" : 
          {
            "iterator kind" : "LN",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "lc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$math_test"
                }
              }
            ]
          }
        },
        {
          "field name" : "fc",
          "field expression" : 
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "fc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$math_test"
                }
              },
              {
                "iterator kind" : "CONST",
                "value" : 2
              }
            ]
          }
        },
        {
          "field name" : "lnfc",
          "field expression" : 
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "LN",
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "fc",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$math_test"
                    }
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : 2
              }
            ]
          }
        },
        {
          "field name" : "dc",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "dc",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$math_test"
            }
          }
        },
        {
          "field name" : "lndc",
          "field expression" : 
          {
            "iterator kind" : "LN",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "dc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$math_test"
                }
              }
            ]
          }
        },
        {
          "field name" : "nc",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "nc",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$math_test"
            }
          }
        },
        {
          "field name" : "lnnc",
          "field expression" : 
          {
            "iterator kind" : "LN",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "nc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$math_test"
                }
              }
            ]
          }
        },
        {
          "field name" : "sort_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$math_test"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "ic",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "ic",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "lnic",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "lnic",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "lc",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "lc",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "lnlc",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "lnlc",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "fc",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "fc",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "lnfc",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "lnfc",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "dc",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "dc",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "lndc",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "lndc",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "nc",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "nc",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "lnnc",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "lnnc",
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