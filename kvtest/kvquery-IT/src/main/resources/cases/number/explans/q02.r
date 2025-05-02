compiled-query-plan

{
"query file" : "number/q/q02.q",
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
      "target table" : "NumTable",
      "row variable" : "$$NumTable",
      "index used" : "idx_num1",
      "covering index" : true,
      "index row variable" : "$$NumTable_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "num" : { "start value" : 0, "start inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$NumTable_idx",
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
            "variable" : "$$NumTable_idx"
          }
        }
      }
    ]
  }
}
}