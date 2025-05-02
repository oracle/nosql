compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_bar2_starr_2017_11.q",
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
      "index used" : "idx_starr",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"substring#starr[]@,0,4":"2017"},
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
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FN_SUBSTRING",
            "input iterators" : [
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$element"
              },
              {
                "iterator kind" : "CONST",
                "value" : 5
              },
              {
                "iterator kind" : "CONST",
                "value" : 2
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "11"
          }
        },
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "starr",
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
        "field name" : "elements_with_year_2017",
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
                  "iterator kind" : "FN_SUBSTRING",
                  "input iterators" : [
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$element"
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 0
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 4
                    }
                  ]
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : "2017"
                }
              },
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "starr",
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
        "field name" : "elements_with_month_11_from_arrays_where_at_least_one_element_has_year_2017",
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
                  "iterator kind" : "FN_SUBSTRING",
                  "input iterators" : [
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$element"
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 5
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 2
                    }
                  ]
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : "11"
                }
              },
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "starr",
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