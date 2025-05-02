compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_bar2_tmarr_year_2019_month_gt_1.q",
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
          "equality conditions" : {"year#tmarr[]":2019},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$b",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
      "input iterator" :
      {
        "iterator kind" : "ARRAY_FILTER",
        "predicate iterator" :
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$element"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 1
          }
        },
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
        "field name" : "elements_with_year_2019",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "ARRAY_FILTER",
              "predicate iterator" :
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 2019
                }
              },
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
          ]
        }
      },
      {
        "field name" : "elements_with_month_greater_than_1_from_arrays_where_at_least_one_element_has_year_2019",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "ARRAY_FILTER",
              "predicate iterator" :
              {
                "iterator kind" : "GREATER_THAN",
                "left operand" :
                {
                  "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              },
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
          ]
        }
      }
    ]
  }
}
}