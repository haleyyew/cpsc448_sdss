Split string into a vector, delimitted by comma.
Example: 
"p.modelmagerr_r, p.modelmagerr_i, p.modelmagerr_z"
=>
["p.modelmagerr_r", "p.modelmagerr_i", "p.modelmagerr_z"]

#####SELECT:
rm 'TOP' followed by a number
Example: 
"TOP 1000 objID" => "objID"

rm substring "x." of every "x.attribute"
"p.modelmagerr_r" => "modelmagerr_r"

rm keyword "as" followed by the variable name
"up_name as name" => "up_name"

rm string constants ''
"'<a target=info href>'" =>

rm operators such as "+"
"+" =>

rm brackets "(", ")", "[", "]" of functions, but keep attribute inside bracket
"dbo.fphototypen(p.type)" => "fphototypen","type"

rm keyword "cast" but keep the attribute inside the brackets
"cast(x.objid as varchar(20)" => objid


#####FROM:
rm user created tables "#x"
"#x x" =>

rm variable names
"specphotoall p" => "specphotoall"

rm arguments of function calls, database prefix "dbo.", and brackets
"dbo.fFootprintEq(133.1709,33.4172,2.3)" => "fFootprintEq"


#####WHERE:
rm operators "=", "<", ">", etc.
"up_id = objid" => "up_id","objid"

rm variable prefix "x."
"u.up_id" => "up_id"

rm logical keywords
"up_id=up_id and objid=objid" => "up_id","up_id","objid","objid"

rm numerical and string constants
"specObjId=0" => "specObjId"
"tableName=’Glossary’" => "tableName"

rm brackets but keep attributes
"(r - extinction_r)" => "r","extinction_r"


#####ORDER BY:
rm variable prefix "x."
"x.up_id" => "up_id"

#####GROUP BY:
rm variable prefix "x."
"x.up_id" => "up_id"
