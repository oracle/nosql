compiled-query-plan

{
"query file" : "prim_index_sort/q/sort3.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "primary key bind expressions" : [
    {
      "iterator kind" : "EXTERNAL_VAR_REF",
      "variable" : "$id1"
    },
    null
  ],
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
          "equality conditions" : {"id1":0,"id2":1,"id3":0},
          "range conditions" : { "id4" : { "start value" : "", "start inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$id1"
        },
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$id3"
        },
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$id4"
        }
      ],
      "map of key bind expressions" : [
        [ 0, -1, 1, 2, -1 ]
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
    ]
  }
}
}