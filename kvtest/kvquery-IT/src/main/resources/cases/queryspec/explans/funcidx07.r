compiled-query-plan

{
"query file" : "queryspec/q/funcidx07.q",
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
      "target table" : "Viewers",
      "row variable" : "$$v",
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
    "FROM variable" : "$$v",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FN_SUBSTRING",
        "input iterators" : [
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
        "value" : "2021"
      }
    },
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
      }
    ]
  }
}
}