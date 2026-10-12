let
    #"Source Data" = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    #"Kept" = Table.SelectColumns(#"Source Data", {"a"})
in
    #"Kept"
