compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_bar2_tmarr_kvstore_1129_bugfix_regression.q",
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
      "target table" : "bar2",
      "row variable" : "$$b",
      "index used" : "idx_year_month",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$b",
    "WHERE" : 
    {
      "iterator kind" : "OR",
      "input iterators" : [
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 0,
              "high bound" : 0,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "tmarr",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$b"
                }
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        },
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 1,
              "high bound" : 1,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "tmarr",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$b"
                }
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        },
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 2,
              "high bound" : 2,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "tmarr",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$b"
                }
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 2
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
            "variable" : "$$b"
          }
        }
      },
      {
        "field name" : "tm",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "tm",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b"
          }
        }
      },
      {
        "field name" : "age",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "age",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b"
          }
        }
      },
      {
        "field name" : "year0",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_SLICE",
            "low bound" : 0,
            "high bound" : 0,
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tmarr",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b"
              }
            }
          }
        }
      },
      {
        "field name" : "month0",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_SLICE",
            "low bound" : 0,
            "high bound" : 0,
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tmarr",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b"
              }
            }
          }
        }
      },
      {
        "field name" : "year1",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_SLICE",
            "low bound" : 1,
            "high bound" : 1,
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tmarr",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b"
              }
            }
          }
        }
      },
      {
        "field name" : "month1",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_SLICE",
            "low bound" : 1,
            "high bound" : 1,
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tmarr",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b"
              }
            }
          }
        }
      },
      {
        "field name" : "year2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_SLICE",
            "low bound" : 2,
            "high bound" : 2,
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tmarr",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b"
              }
            }
          }
        }
      },
      {
        "field name" : "month2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_SLICE",
            "low bound" : 2,
            "high bound" : 2,
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tmarr",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b"
              }
            }
          }
        }
      }
    ]
  }
}
}