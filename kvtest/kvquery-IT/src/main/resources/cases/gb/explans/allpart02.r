compiled-query-plan
SFW([15], 6, 16)
[
  FROM:
  RECV([12], 6, 13)
  [
    Sort Field Positions : 0,
    DistributionKind : ALL_PARTITIONS,
    Number of Registers :15,
    Number of Iterators :6,
    SFW([12], 6, 13)
    [
      FROM:
      BASE_TABLE([5], 0, 1, 2, 3, 4)
      Index entry regs: ([11], 6, 7, 8, 9, 10)
      [
        Foo via covering primary index
        KEY: {}
        RANGE: null
      ] as $$f

      GROUP BY:
      Grouping by the first expression in the SELECT list

      SELECT:
      FIELD_STEP([6])
      [
        VAR_REF($$f_idx)([11], 6, 7, 8, 9, 10),
        id1,
        theFieldPos : 0
      ],
      FUNC_COUNT_STAR([13])
      [

      ]

      LIMIT:
      CONST([14])
      [
        5
      ]
    ]
  ] as $from-0

  GROUP BY:
  Grouping by the first expression in the SELECT list

  SELECT:
  FIELD_STEP([6])
  [
    VAR_REF($from-0)([12], 6, 13),
    id1,
    theFieldPos : 0
  ],
  FUNC_SUM([16])
  [
    FIELD_STEP([13])
    [
      VAR_REF($from-0)([12], 6, 13),
      Column_2,
      theFieldPos : 1
    ]
  ]

  LIMIT:
  CONST([14])
  [
    5
  ]
]