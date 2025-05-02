compiled-query-plan

{
"query file" : "idc_uuid/q/up01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [  ],
    "update clauses" : [
      {
        "iterator kind" : "SET",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "uid3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b"
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : "18acbcb9-137b-4fc8-99f7-812f20240369"
        }
      }
    ],
    "update TTL" : false,
    "isCompletePrimaryKey" : true,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "bar1",
        "row variable" : "$$b",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"uid1":"ffacbcb9-137b-4fc8-99f7-812f20240359","id":4,"firstName":"John"},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$b",
      "SELECT expressions" : [
        {
          "field name" : "b",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b"
          }
        }
      ]
    }
  }
}
}