let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"k","v"}),
    Result = Table.Group(s, {"k"}, {{"total", each List.Sum([v]), type number}})
in
    Result
