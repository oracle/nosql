compiled-query-plan

{
"query file" : "queryspec/q/funcidx10.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0, 1 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Viewers",
      "row variable" : "$$v",
      "index used" : "idx9_year_month",
      "covering index" : false,
      "index row variable" : "$$v_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "substring#info.shows[].seasons[].episodes[].date@,5,2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "03"
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$v",
    "SELECT expressions" : [
      {
        "field name" : "acct_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "acct_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v"
          }
        }
      },
      {
        "field name" : "user_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "user_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v"
          }
        }
      },
      {
        "field name" : "months",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "SEQ_MAP",
              "mapper iterator" :
              {
                "iterator kind" : "FN_SUBSTRING",
                "input iterators" : [
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$sq1"
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
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "date",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "episodes",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "seasons",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "shows",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "info",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$v"
                        }
                      }
                    }
                  }
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