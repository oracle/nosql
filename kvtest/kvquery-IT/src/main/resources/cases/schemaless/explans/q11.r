compiled-query-plan

{
"query file" : "schemaless/q/q11.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "distinct by fields at positions" : [ 1, 2 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Viewers",
        "row variable" : "$$v",
        "index used" : "idx_country_showid_date",
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
          "iterator kind" : "ANY_LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "shows[].seasons[].episodes[].date",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$v_idx"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "2021-01-01"
          }
        },
        "position in join" : 0
      },
      "FROM variable" : "$$v",
      "SELECT expressions" : [
        {
          "field name" : "lastName",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "lastName",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$v"
            }
          }
        },
        {
          "field name" : "acct_id_gen",
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
          "field name" : "user_id_gen",
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
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "lastName",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "lastName",
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