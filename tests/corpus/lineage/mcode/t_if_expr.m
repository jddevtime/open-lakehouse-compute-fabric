let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"a","b"}),
    Result = Table.AddColumn(s, "c", each if [a] > 0 then [b] else 0)
in
    Result
