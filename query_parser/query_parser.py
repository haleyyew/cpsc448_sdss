__author__ = 'Vitor Makiyama Hirota, edited by Haoran Yu'
import query_processing

import os
import clr
import sys

import System
sys.path.append('C:/Program Files (x86)/Microsoft SQL Server/130/SDK/Assemblies')
clr.AddReference('Microsoft.SqlServer.TransactSql.ScriptDom')
import Microsoft.SqlServer.TransactSql.ScriptDom as sd
parser = sd.TSql100Parser(1)

def getString (node):
    return ''.join ([ t.Text for t in
					  list( node.ScriptTokenStream)[ node.FirstTokenIndex : node.LastTokenIndex+1]])
	
def _clause (node) :
	try :
		return getString (node)
	except :
		return None


def split_string_and_store(token_group,str, session_tokens):
	session_tokens.split_string_and_store(token_group,str)


def parse(session_tokens, filename):

	with open( filename ) as f :
		for line in f :
			stream = System.IO.StringReader ( line.lower () )
			fragment, parse_errors = parser.Parse (stream)
			stream.Close()
			errors = ""
			if parse_errors.Count :
				errors = ( "The following errors were caught:\n ")
				for err in parse_errors :
					errors += ( err.Message, "\n " )

			try :
				for stmt in fragment.Batches[0].Statements :

					qe = stmt.QueryExpression
					query = {
						'modifiers' : [ _clause(qe.TopRowFilter ) , qe.UniqueRowFilter ] ,
						'select' : ', '.join (map( getString , qe.SelectElements ) ) ,
						'from' : _clause ( qe.FromClause ) ,
						'where' : _clause ( qe.WhereClause ) ,
						'orderby' : _clause (qe.OrderByClause ) ,
						'groupby' : _clause ( qe.GroupByClause )
					}
					print '--'
					print 'Query : ' , line
					for key in [ 'modifiers' , 'select' , 'from' , 'where' , 'orderby' ,'groupby' ] :
						print '-' , key
						print ' query : ' , query [ key ]
						if type(query [ key ]) is str:
							split_string_and_store(key, query[key], session_tokens)

			except :
				print sys.exc_info ( )
			finally :
				print '' . join ( errors )


