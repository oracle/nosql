compiled-query-plan

{
"query file" : "delete/q/del02.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "input iterator" :
    {
      "iterator kind" : "DELETE_ROW",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "Foo",
          "row variable" : "$$foo",
          "index used" : "primary index",
          "covering index" : true,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : { "id" : { "start value" : 20, "start inclusive" : false } }
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$foo",
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
                "variable" : "$$foo"
              }
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$delcount-0",
  "GROUP BY" : "No grouping expressions",
  "SELECT expressions" : [
    {
      "field name" : "numRowsDeleted",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "numRowsDeleted",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$delcount-0"
          }
        }
      }
    }
  ]
}
}