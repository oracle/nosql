compiled-query-plan

{
"query file" : "schemaless/q/q02.q",
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
      "target table" : "Viewers",
      "row variable" : "$$v",
      "index used" : "idx_country_genre",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"country":"USA","shows[].genres[]":"comedy"},
          "range conditions" : {}
        }
      ],
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