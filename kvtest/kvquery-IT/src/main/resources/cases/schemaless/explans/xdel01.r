compiled-query-plan
{
"query file" : "schemaless/q/xdel01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "DELETE_ROW",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Viewers",
        "row variable" : "$$viewers",
        "index used" : "primary index",
        "covering index" : true,
        "index scans" : [
          {
            "equality conditions" : {"acct_id":100,"user_id":2},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$viewers",
      "SELECT expressions" : [
        {
          "field name" : "acct_id_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "acct_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$viewers"
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
              "variable" : "$$viewers"
            }
          }
        }
      ]
    }
  }
}
}
