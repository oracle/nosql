/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */
package oracle.kv.impl.async.perf;

/**
 * Performance metrics mechanism for async.
 *
 * <h1>General Notes for Performance Data</h1>
 *
 * <p>This package specifically deals with async performance. However, the
 * async code is the frist package that the general performance-related
 * mechanism applied.  Therefore, I am putting the documentation in this
 * package, although it applies to a more general context.
 *
 * <h2>Design Trade-off and Rationale</h2>
 *
 * <p>There are several desirable traits for the performance metrics
 * mechanism.
 * <ol>
 * <li>Maintainability. We want our performance mechanism code easy to modify
 * and extend. One of the most outstanding issue is backward compatibility.
 * Because the performance data is exchanged across various components either
 * by serialization or through some API, we must ensure the evolved code does
 * not break components that is running the old code. Another issue is the
 * formatting and interpretation of the data: it is easier to maintain the code
 * if related data are represented in a uniform way. For example, it is less
 * error prone if we represent all duration with nano-seconds.</li>
 * <li>Usability. The performance data is used in several different ways. The
 * data is serialized into text form and dumped to log files. The log files are
 * either inspected manually or parsed and analyzed with tools. The data is
 * also serialized and send over the network to monitoring services such as
 * Kibana Dashboard. Furthermore, we expose API for querying the performance
 * data programmatically. The usability trait menifest into two traits of
 * different aspects.
 * <ol>
 * <li>Programmability. We want the data to be presented in a way that is easy
 * to program with. This can be considered as maintainability for the consumer
 * code of the data, but we usually do not consider that consumer code (e.g.,
 * bash/python analysis tool or Kibana code) as part of our code. For text log
 * files, this trait means the text format is easy to parse and feed to the
 * analysis tool. For the monitoring service, this means the performance data
 * is serialized in a compatible way with the service. For querying with the
 * API, this means the ease of use for the API.  We currently, however, do not
 * have much concrete experience on how the API is used.
 * </li>
 * <li>Readability. We want the data to be presented in a way that is easy to
 * read by human. This trait only applies to the log file since other forms of
 * usage (analysis tool, monitoring service or query through API) are not
 * interactions with human beings. There are two aspects that affect the
 * readability of the data. The first aspect is the format to represent the
 * data hierarchy. For example, we have been using a format which uses
 * indentation for the structure of hierarchy (see
 * StatsPacket#getFormattedStats). This format is much readable than CSV files
 * where a header row contains all the fully-qualified names which uses dot to
 * separate the subcomponent of the hierarchy. The second aspect is the
 * interpretation of the data including selecting the proper unit of the value
 * (e.g., second or milli-second) and the human-readable representation (e.g.,
 * posix timestamp vs. date time), etc.</li>
 * </ol>
 * </li>
 * </ol>
 *
 * <p>The above desirable traits in many cases conflict with each other. For
 * our design, when trade-offs are made, maintainability always has the highest
 * priority, and then programmability which puts readability at the least
 * priority.  The rationale is that maintainability costs the most engineering
 * efforts where there is very few work-around. On the other hand we can write
 * convenient methods or specialized tools to circumvent programmability and
 * readability issue which is expected to be easier to design and maintain. We
 * will probably need those specialized addition in the first place. We weigh
 * programmability over readability because programmability has to do with
 * interaction with client or third-party code and is therefore harder to
 * maintain while readability can be solved entirely with a specialized tool.
 * In addition to the consideration of code maintainance, readability is also
 * more or less subjective. Verbose and elaborated data can be thought of as
 * favorable to some but clutered to others.
 *
 * <p>Here are some of the trade-offs we made.
 * <ul>
 * <li>Json objects vs. Java objects. This is the most important trade-off we
 * made between maintainability and programmability. The key difference between
 * Json and Java objects is that Json object can be schemaless. That is, we do
 * not need to make the guarantee of what performance data we produce. This is
 * not possible for a Java object: the API determines what data is in the
 * object. This schemaless feature helps to solve the backward compatibility
 * issue. On the other hand, it makes the use of the performance data a bit
 * more difficult. The user of the data must assume changes. They may assume
 * some conventions of the structure of the data but need to guard against when
 * the assumptions become invalid.</li>
 * <li>Nano-seconds as a uniform duration unit. This is a trade-off between
 * maintainability/programmability vs. readability. Time values in nano-seconds
 * are usually not easy to read because most of the duration values are in the
 * range of mili-seconds or seconds. However, having all the duration in our
 * code in nano-seconds result in less conversion code. It is easier for the
 * user to program against the performance data as well. Deciding on the proper
 * unit for readability can sometimes be complicated as well.</li>
 * <li>Performance data dump format. I have decided to dump performance json
 * document in one line. This, to me, is a trade-off between programmability
 * and readability. The log file where the data is dumped is analyzed either
 * manually or by a tool. Making it one line eases the parsing effort which I
 * prefer, but it is more difficult to read the content of the json
 * document. Also pretty-print the json document makes the log bloated which
 * may have a negative impact on readability.</li>
 * </ul>
 *
 * <p>It is worth noting that we only need to make trade-off when there is a
 * conflict. Sometimes adding some more human readable data can easily
 * reconcile the conflict.
 *
 * <h2>Using Performance Data from Json Documents</h2>
 *
 * <p>This section discussses the cases on how the performance data can be
 * consumed in Json format.
 *
 * <p>We do not know much about how our users utilize the performance data.
 * There are currently two use cases that I have experienced. For one, the
 * cloud team has implemented a data pipeline to feed performance data as
 * key-value pairs to Kibana Dashboard, which plots values over time.  For the
 * other, I have developped analysis scripts to plot graphs based on the dumped
 * performance data in the stat files. In both case, there is a common pattern.
 * The performance data are generated as json documents over time. There is a
 * grouping phase where the values representing the same metrics are grouped
 * into different time series. There is a displaying phase where a time series
 * of interest is selected and plotted out. The two use cases differ with how
 * grouping and selection of display is done. In the cloud pipeline, the
 * performance data is shipped as key-value pairs. The time series groups by
 * the key which is also indexed. Time series of interest are selected with
 * elastic search and plotted out in a interactive manner.  In my analysis
 * scripts, I hard-coded the routine for both grouping the metrics of interest
 * into time series and for plotting them out. I think these two use patterns
 * are a good demonstration of two common use patterns.
 *
 * <p>In the cloud case, the grouping phase does not care about the schema of
 * the performance data. During the display phase, users also do not need to
 * figure out the exact schema, but rely on keywords and search tools for data
 * selection. This pattern fits well with the general design goal of
 * performance data. In particular, the maintainance cost for backward
 * compatibility is low. However, to achieve this, we need the support of a
 * search tool. Furthermore, we need to provide convenience method to generate
 * key-value pairs from the json document.
 * This is supported with {@code PerfUtil#depthFirstSearchIterator}
 * and {@code PerfUtil#breadthFirstSearchIterator}
 * where it iterates through json-pointer value pairs. It is worth noting that
 * for the grouping phase to work successfully, the generated key-value pair
 * must have a key that is self-contained. For example, for the following json
 * documents
 * <pre>
 * {@code
 * {"op" : "get", "latency" : 1}
 * {"op" : "put", "latency" : 2}
 * }
 * </pre>
 * can generate four key-value pairs: "op"/"get", "op"/"put", "latency"/1,
 * "latency"/2. When we group key "latency" into a time series, we will not be
 * able to separate out latency for different ops. The cloud pipeline would
 * probably want to stay agnostic about the document schema and therefore, it
 * is the responsibility of the KV code to design a reasonable schema to
 * accomodate such issue.
 *
 * <p>In the analysis-scripting case, without the support of a search engine,
 * it seems best for me to parse, group and plot metrics of interest based on
 * the schema. This adds to the maintainance cost since whenever we change the
 * schema of the performance data, the code needs to change. I am resorting to
 * code modularity for lowering the maintainance cost. On the other hand, this
 * gives me flexibility to group data in multiple ways and to plot specialized
 * graphs.
 *
 * <h1>Async Performance Data</h1>
 *
 * <p>For the async dialog layer, we collect two types of performance metrics:
 * dialog metrics and transport metrics, corresponding to the dialog and
 * transport layer of the async code. The dialog layer manages dialogs which
 * consist of request and response pairs. The dialog metrics include dialog
 * throughput, latency, etc. The transport layer is shared among all the
 * dialogs and manages connections, task execution and read/write events of
 * dialog frames. The transport metrics include executor business, channel
 * read/write latency, etc.
 *
 * <p>The async performance code adopts the following convention. Objects named
 * *PerfTracker implements perf trackers to collect the metrics which are named
 * *Perf and implements {@code JsonSerializable}.
 */
