from py2neo import Graph, Node, Relationship, NodeMatcher

graph = Graph(username="neo4j", password="417059")

matcher = NodeMatcher(graph)

node = matcher.match('Person')
