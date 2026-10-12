let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"a","b"}),
    Result = Table.AddColumn(s, "c", each [a] + [b], type number)
in
    Result
