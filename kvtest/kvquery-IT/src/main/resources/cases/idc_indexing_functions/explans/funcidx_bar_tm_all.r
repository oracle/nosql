compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_bar_tm_all.q",
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
      "target table" : "bar",
      "row variable" : "$$b",
      "index used" : "idx_year_month_day",
      "covering index" : false,
      "index row variable" : "$$b_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "year#tm",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
              "input iterator" :
              {
                "iterator kind" : "CAST",
                "target type" : "Timestamp(9)",
                "quantifier" : "",
                "input iterator" :
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$tm1"
                }
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "month#tm",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
              "input iterator" :
              {
                "iterator kind" : "CAST",
                "target type" : "Timestamp(9)",
                "quantifier" : "",
                "input iterator" :
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$tm1"
                }
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "day#tm",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
              "input iterator" :
              {
                "iterator kind" : "CAST",
                "target type" : "Timestamp(9)",
                "quantifier" : "",
                "input iterator" :
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$tm1"
                }
              }
            }
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$b",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
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
          "right operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$tm1"
              }
            }
          }
        },
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
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
          "right operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$tm1"
              }
            }
          }
        },
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
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
          "right operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$tm1"
              }
            }
          }
        },
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
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
          "right operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$tm1"
              }
            }
          }
        },
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
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
          "right operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "CAST",
              "target type" : "Timestamp(9)",
              "quantifier" : "",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$tm1"
              }
            }
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
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      },
      {
        "field name" : "Column_5",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      },
      {
        "field name" : "Column_6",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      },
      {
        "field name" : "Column_7",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      },
      {
        "field name" : "Column_8",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      },
      {
        "field name" : "Column_9",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      },
      {
        "field name" : "Column_10",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      }
    ]
  }
}
}