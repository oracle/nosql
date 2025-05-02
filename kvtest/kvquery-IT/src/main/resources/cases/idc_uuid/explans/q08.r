compiled-query-plan

{
"query file" : "idc_uuid/q/q08.q",
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
      "target table" : "bar2",
      "row variable" : "$$bar2",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$bar2",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "String",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "uid1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$bar2"
            }
          }
        }
      }
    ]
  }
}
}