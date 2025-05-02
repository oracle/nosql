compiled-query-plan

{
"query file" : "idc_schemaless/q/q29.q",
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
      "index used" : "idx_country_showid_date",
      "covering index" : true,
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
          "iterator kind" : "FN_SUBSTRING",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "shows[].seasons[].episodes[].date",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$v_idx"
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
      "position in join" : 0
    },
    "FROM variable" : "$$v_idx",
    "SELECT expressions" : [
      {
        "field name" : "acct_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#acct_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v_idx"
          }
        }
      },
      {
        "field name" : "user_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#user_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v_idx"
          }
        }
      }
    ]
  }
}
}