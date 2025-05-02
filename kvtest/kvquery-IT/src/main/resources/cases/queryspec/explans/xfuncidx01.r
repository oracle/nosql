compiled-query-plan
{
"query file" : "queryspec/q/xfuncidx01.q",
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
        "target table" : "Users",
        "row variable" : "$$users",
        "index used" : "primary index",
        "covering index" : true,
        "index scans" : [
          {
            "equality conditions" : {"id":4},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$users",
      "SELECT expressions" : [
        {
          "field name" : "id_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$users"
            }
          }
        }
      ]
    }
  }
}
}
