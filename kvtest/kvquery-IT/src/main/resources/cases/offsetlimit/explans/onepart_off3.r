compiled-query-plan

{
"query file" : "offsetlimit/q/onepart_off3.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$Foo",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id1":8,"id2":4},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$Foo",
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo"
          }
        }
      },
      {
        "field name" : "id2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo"
          }
        }
      },
      {
        "field name" : "id3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo"
          }
        }
      },
      {
        "field name" : "id4",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id4",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo"
          }
        }
      }
    ],
    "OFFSET" :
    {
      "iterator kind" : "CONST",
      "value" : 2
    }
  }
}
}