from flask import request,Flask,render_template
from py2neo import Database,Graph
import neo4j
import pandas as pd
import json


app = Flask(__name__,template_folder='templates')

uri        = "bolt://192.168.137.162:7687"
userName   = "neo4j"
pwd        = "123456"

g = Graph(username=userName,password=pwd)

#perform random walk on the given inputs
def createQuery(queryTerm,nodeType):
	q = queryTerm.strip(" ")
	query = "match (m:"+nodeType+" {name: \""+q+"\"}) call algo.randomWalk.stream(id(m),10,1,{path:true}) yield nodeIds unwind nodeIds as nodeId match (node)-[r]-(n) WHERE id(node) = nodeId RETURN node.name,type(r),n.name"
	print(query)
	return query


#search the knowledge graph depending on the user input
def searchGraph(q,nodeType):
    if(nodeType is "Movie"):
        q=createQuery(q,"Movie")
    elif(nodeType is "PERSON"):
        q=createQuery(q,"PERSON")
    elif(nodeType is "DATE"):
        q=createQuery(q,"DATE")
    x = g.run(q)
    y = x.data()
    print(y)
    data_set = list()
    dic = {}
    for x in y: 
        print(x.values())
        data_set.append(list(x.values()))
    return data_set
#     return 


@app.route('/search')
def my_route():
  movie = request.args.get('movie',default=None)
  person = request.args.get('person',default=None)
  date = request.args.get('date',default=None)
  if not (movie is None):
    result=searchGraph(movie,"Movie")
  elif not (person is None) :
    result = searchGraph(person,"PERSON")
  elif not (date is None):
    result = searchGraph(date,"DATE")
  print(result)
  return render_template('view.html',your_list=result)


if __name__ == '__main__':
    app.debug = True
    #authenticate(uri, userName, password)
    app.run(host = '0.0.0.0',port=5005)