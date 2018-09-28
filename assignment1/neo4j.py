from py2neo import Graph, Node, Relationship, NodeMatcher

graph = Graph("http://127.0.0.1:7474", username="neo4j", password="417059")

matcher = NodeMatcher(graph)

node = matcher.match('Person')
