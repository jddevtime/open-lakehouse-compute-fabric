let
 S = Sql.Database("abc.datawarehouse.fabric.microsoft.com","wh1",[Query="SELECT id FROM mart.sales"])
in
 S
