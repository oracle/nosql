compiled-query-plan

{
"query file" : "schemaless/q/q03.q",
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
      "index used" : "idx_country_genre",
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
        "iterator kind" : "ANY_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "shows[].genres[]",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "comedy"
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
      }
    ]
  }
}
}