compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_bar_tm01.q",
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
      "index used" : "idx_hour_min_sec_ms_micro_ns",
      "covering index" : true,
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
              "field name" : "hour#tm",
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
                  "variable" : "$tm2"
                }
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "minute#tm",
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
                  "variable" : "$tm2"
                }
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "second#tm",
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
                  "variable" : "$tm2"
                }
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "millisecond#tm",
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
                  "variable" : "$tm2"
                }
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "nanosecond#tm",
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
                  "variable" : "$tm2"
                }
              }
            }
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$b_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b_idx"
          }
        }
      }
    ]
  }
}
}