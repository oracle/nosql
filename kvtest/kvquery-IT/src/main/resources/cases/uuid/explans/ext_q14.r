compiled-query-plan

{
"query file" : "uuid/q/ext_q14.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "primary key bind expressions" : [
    {
      "iterator kind" : "EXTERNAL_VAR_REF",
      "variable" : "$uuid1_1"
    },
    null,
    null
  ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "foo",
      "row variable" : "$$foo",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"uid1":"00000000-0000-0000-0000-000000000000","int":1,"str":"Tom"},
          "range conditions" : {}
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$uuid1_1"
        }
      ],
      "map of key bind expressions" : [
        [ 0, -1, -1 ]
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$foo",
    "SELECT expressions" : [
      {
        "field name" : "uid2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "uid2",
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