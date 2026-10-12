let
 Source = Lakehouse.Contents(null),
 ws = Source{[workspaceId="11111111-1111-4111-8111-111111111111"]}[Data],
 lh = ws{[lakehouseId="22222222-2222-4222-8222-222222222222"]}[Data],
 s = lh{[Schema="gold",Id="sales",ItemKind="Table"]}[Data]
in
 s
