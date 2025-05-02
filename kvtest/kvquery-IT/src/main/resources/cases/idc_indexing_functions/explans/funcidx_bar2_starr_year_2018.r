compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_bar2_starr_year_2018.q",
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
      "target table" : "bar2",
      "row variable" : "$$b",
      "index used" : "idx_starr_year",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"substring#starr[]@,0,4":"2018"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$b",
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
        "field name" : "elements_with_year_2018",
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
                  "value" : "2018"
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