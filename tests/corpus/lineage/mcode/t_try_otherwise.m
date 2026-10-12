let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"a"}),
    Result = Table.AddColumn(s, "c", each try [a] otherwise null)
in
    Result
