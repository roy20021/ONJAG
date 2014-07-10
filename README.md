ONJAG
=====

Overlays Not Just a Graph

This abstraction has developed as part of the Master thesis of Computer Science by Andrea Esposito at University of Pisa (2014). <br/>
The thesis's title is: <i>"ONJAG, network overlays supporting graph processing"</i>.

It runs over <a href="https://github.com/apache/spark">Spark</a> a <i>"general engine for large-scale data processing"</i> (1.0.0).

The idea is to provide a stack based networked view of the graph computations. Like the Pregel framework the graph processing is described as a network simulation where vertices and edges are the nodes and links respectively. In addition ONJAG enables the definition of multiple parallel Pregel-like computations, called Protocols. The main provided feature is the intra-communications between Protocols allowing interactions, i.e. each Protocol can exchange information with other Protocols in order to orchestrate a complex but convenient computation. Mainly our focus has been on the developing of a Peer-to-Peer (P2P) protocol stack emphasizing the topology overlays which could be exploited.

Further details are available directly in the thesis.
