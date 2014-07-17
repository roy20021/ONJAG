ONJAG
=====

Overlays Not Just a Graph

This abstraction was developed as part of the Master thesis of Computer Science by Andrea Esposito at University of Pisa (2014). <br/>
The thesis's title is: <i>"ONJAG, network overlays supporting distributed graph processing"</i>.

It runs over <a href="https://github.com/apache/spark">Spark</a>, a <i>"general engine for large-scale data processing"</i> (1.0.0).

The idea is to provide a stack based view of the graph computations. Like the Pregel framework the graph processing is described as a network simulation where vertices and edges are the nodes and links respectively. In addition ONJAG enables the definition of multiple parallel Pregel-like computations, called Protocols. The main provided feature is the intra-communications between Protocols allowing interactions, i.e. each Protocol can exchange information with other Protocols in order to orchestrate a complex but convenient computation. Mainly our focus has been on the developing of a Peer-to-Peer (P2P) protocol stack emphasizing the topology overlays which could be exploited.

Further details are available directly in the thesis.
=====

Example

<!-- HTML generated using hilite.me --><div style="background: #ffffff; overflow:auto;width:auto;border:solid gray;border-width:.1em .1em .1em .8em;padding:.2em .6em;"><pre style="margin: 0; line-height: 125%"><span style="color: #008800; font-weight: bold">val</span> idleProtocol <span style="color: #008800; font-weight: bold">=</span> <span style="color: #008800; font-weight: bold">new</span> <span style="color: #BB0066; font-weight: bold">IdleProtocol</span><span style="color: #333333">()</span>
<span style="color: #008800; font-weight: bold">val</span> onjag <span style="color: #008800; font-weight: bold">=</span> <span style="color: #008800; font-weight: bold">new</span> <span style="color: #BB0066; font-weight: bold">OnJag</span><span style="color: #333333">(</span>sc<span style="color: #333333">,</span> <span style="color: #BB0066; font-weight: bold">OnJag</span><span style="color: #333333">.</span><span style="color: #BB0066; font-weight: bold">DEFAULT_STORAGE_LEVEL</span><span style="color: #333333">,</span> idleProtocol<span style="color: #333333">)</span>
onjag<span style="color: #333333">.</span>setMaxStep<span style="color: #333333">(</span><span style="color: #0000DD; font-weight: bold">1000</span><span style="color: #333333">)</span>

println<span style="color: #333333">(</span><span style="background-color: #fff0f0">&quot;Parsing input file...&quot;</span><span style="color: #333333">)</span>
<span style="color: #008800; font-weight: bold">val</span> vertices <span style="color: #008800; font-weight: bold">=</span> input<span style="color: #333333">.</span>map<span style="color: #333333">(</span>line <span style="color: #008800; font-weight: bold">=&gt;</span> <span style="color: #333333">{</span>
      <span style="color: #008800; font-weight: bold">val</span> fields <span style="color: #008800; font-weight: bold">=</span> line<span style="color: #333333">.</span>split<span style="color: #333333">(</span><span style="background-color: #fff0f0">&quot;\t&quot;</span><span style="color: #333333">)</span>
      <span style="color: #008800; font-weight: bold">val</span> <span style="color: #333333">(</span>id<span style="color: #333333">,</span> body<span style="color: #333333">)</span> <span style="color: #008800; font-weight: bold">=</span> <span style="color: #333333">(</span>fields<span style="color: #333333">(</span><span style="color: #0000DD; font-weight: bold">0</span><span style="color: #333333">).</span>toInt<span style="color: #333333">,</span> fields<span style="color: #333333">(</span><span style="color: #0000DD; font-weight: bold">1</span><span style="color: #333333">).</span>replace<span style="color: #333333">(</span><span style="background-color: #fff0f0">&quot;\\n&quot;</span><span style="color: #333333">,</span> <span style="background-color: #fff0f0">&quot;\n&quot;</span><span style="color: #333333">))</span>
      <span style="color: #008800; font-weight: bold">val</span> links <span style="color: #008800; font-weight: bold">=</span> body<span style="color: #333333">.</span>split<span style="color: #333333">(</span><span style="background-color: #fff0f0">&quot;,&quot;</span><span style="color: #333333">).</span>map<span style="color: #333333">(</span>strLink <span style="color: #008800; font-weight: bold">=&gt;</span> strLink<span style="color: #333333">.</span>toInt<span style="color: #333333">).</span>map<span style="color: #333333">(</span>x <span style="color: #008800; font-weight: bold">=&gt;</span> x<span style="color: #333333">.</span>asInstanceOf<span style="color: #333333">[</span><span style="color: #333399; font-weight: bold">Any</span><span style="color: #333333">])</span>
      <span style="color: #008800; font-weight: bold">val</span> data <span style="color: #008800; font-weight: bold">=</span> <span style="color: #BB0066; font-weight: bold">Array</span><span style="color: #333333">(</span>links<span style="color: #333333">.</span>asInstanceOf<span style="color: #333333">[</span><span style="color: #333399; font-weight: bold">Array</span><span style="color: #333333">[</span><span style="color: #333399; font-weight: bold">Any</span><span style="color: #333333">]])</span>
      <span style="color: #333333">(</span>id<span style="color: #333333">,</span> onjag<span style="color: #333333">.</span>createVertex<span style="color: #333333">(</span>id<span style="color: #333333">,</span> data<span style="color: #333333">))</span>
    <span style="color: #333333">})</span>

<span style="color: #008800; font-weight: bold">val</span> messages <span style="color: #008800; font-weight: bold">=</span> onjag<span style="color: #333333">.</span>createInitMessages<span style="color: #333333">(</span>vertices<span style="color: #333333">,</span> <span style="color: #008800; font-weight: bold">new</span> <span style="color: #BB0066; font-weight: bold">Array</span><span style="color: #333333">[</span><span style="color: #333399; font-weight: bold">Any</span><span style="color: #333333">](</span><span style="color: #0000DD; font-weight: bold">1</span><span style="color: #333333">))</span>
<span style="color: #008800; font-weight: bold">val</span> checkpointConditions <span style="color: #008800; font-weight: bold">=</span> <span style="color: #BB0066; font-weight: bold">List</span><span style="color: #333333">(</span><span style="color: #BB0066; font-weight: bold">Some</span><span style="color: #333333">(</span><span style="color: #008800; font-weight: bold">new</span> <span style="color: #BB0066; font-weight: bold">SuperstepIntervalCondition</span><span style="color: #333333">(</span><span style="color: #0000DD; font-weight: bold">100</span><span style="color: #333333">)))</span>

println<span style="color: #333333">(</span><span style="background-color: #fff0f0">&quot;OnJag Execution Started&quot;</span><span style="color: #333333">)</span>
<span style="color: #008800; font-weight: bold">val</span> result <span style="color: #008800; font-weight: bold">=</span> onjag<span style="color: #333333">.</span>run<span style="color: #333333">(</span>sc<span style="color: #333333">,</span> vertices<span style="color: #333333">,</span> messages<span style="color: #333333">,</span> checkpointConditions<span style="color: #333333">,</span> numPartitions<span style="color: #333333">)</span>
println<span style="color: #333333">(</span><span style="background-color: #fff0f0">&quot;OnJag Execution Ended&quot;</span><span style="color: #333333">)</span>
</pre></div>


