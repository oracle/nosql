compiled-query-plan

{
"query file" : "delete/q/rdel06.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "DELETE_ROW",
    "positions of primary key columns in input row" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$f",
        "index used" : "idx_areacode",
        "covering index" : true,
        "index row variable" : "$$f_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "info.address.phones[].areacode" : { "end value" : 600, "end inclusive" : false } }
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f_idx",
      "SELECT expressions" : [
        {
          "field name" : "id",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        }
      ]
    }
  }
}
}