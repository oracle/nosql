compiled-query-plan

{
"query file" : "uuid/q/q02.q",
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
      "target table" : "foo",
      "row variable" : "$$foo",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"uid1":"18acbcb9-137b-4fc8-99f7-812f20240356"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$foo",
    "SELECT expressions" : [
      {
        "field name" : "uid1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "uid1",
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
}