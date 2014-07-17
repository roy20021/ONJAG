ONJAG
=====

Overlays Not Just a Graph

This abstraction was developed as part of the Master thesis of Computer Science by Andrea Esposito at University of Pisa (2014). <br/>
The thesis's title is: <i>"ONJAG, network overlays supporting distributed graph processing"</i>.

It runs over <a href="https://github.com/apache/spark">Spark</a>, a <i>"general engine for large-scale data processing"</i>. Version 1.0.0.

The idea is to provide a stack based view of the graph computations. Like the Pregel framework the graph processing is described as a network simulation where vertices and edges are the nodes and links respectively. In addition ONJAG enables the definition of multiple parallel Pregel-like computations, called Protocols. The main provided feature is the intra-communications between Protocols allowing interactions, i.e. each Protocol can exchange information with other Protocols in order to orchestrate a complex but convenient computation. Mainly our focus has been on the developing of a Peer-to-Peer (P2P) protocol stack emphasizing the topology overlays which could be exploited.

Further details are available directly in the thesis.

Example
=====

```scala
val idleProtocol = new IdleProtocol()
val onjag = new OnJag(sc, OnJag.DEFAULT_STORAGE_LEVEL, idleProtocol)
onjag.setMaxStep(1000)

println("Parsing input file...")
val vertices = input.map(line => {
      val fields = line.split("\t")
      val (id, body) = (fields(0).toInt, fields(1).replace("\\n", "\n"))
      val links = body.split(",").map(strLink => strLink.toInt).map(x => x.asInstanceOf[Any])
      val data = Array(links.asInstanceOf[Array[Any]])
      (id, onjag.createVertex(id, data))
    })

val messages = onjag.createInitMessages(vertices, new Array[Any](1))
val checkpointConditions = List(Some(new SuperstepIntervalCondition(100)))

println("OnJag Execution Started")
val result = onjag.run(sc, vertices, messages, checkpointConditions, numPartitions)
println("OnJag Execution Ended")
```
