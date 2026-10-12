let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    Result = Table.SelectRows(Source, each [a] > 1)
in
    Result
