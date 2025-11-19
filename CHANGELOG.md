## The Oracle NoSQL Database (Release 25.3.21 Community Edition) Change Log  

Release 25.3.21 Community Edition

### Upgrade Requirements

Release 25.3 supports upgrades starting with the 22.3 release. To upgrade a store directly to the current release, the store must be running release 22.3 or later.

If you have a store running a 20.x, 21.x or 22.1/22.2 release, we recommend that you upgrade it to the current release by first upgrading to a 23.x or 24.x release, and then upgrading from that release to the current release. If you have a store running a 19.x or earlier release and you are an Enterprise Edition user, please contact support. If you are a Community Edition user with this issue, please post a question to the [NoSQL Database Discussions forum on Oracle Communities](https://community.oracle.com/tech/developers/categories/nosql_database_discuss).

For more information, see the section on [Upgrading an Existing Oracle NoSQL Database Deployment](https://www.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/25.1&id=NSADM-GUID-A768BFD0-E205-48E3-855D-9616526DE014) in the Admin Guide.

### Changes in 25.3.21 Community Edition

### New Features

1.  Added support for before image in the Streams API. A user can configure
	subscription streams to include the before images of streaming operations.
	For example, when streaming an update operation on a subscribed table,
	the subscription will include both the current image (the update operation)
	as well as the before image (the row as it appeared before the update).

    \[KVSTORE-2394\]

2.  Added support for streaming by transaction in the Streams API. A user
	can now configure subscription streams to stream transactions instead of
	individual put and delete operations. For example, if an operation
	updates or deletes multiple rows in a single transaction against
	a subscribed table, the subscription will deliver the entire
	transaction as a single streaming event. Operations in the transaction
	will be accessible from within that streaming event.

    \[KVSTORE-2341\]

4.  Added support for an experimental version user-supplied row metadata.
	A user can add their own metadata to any row during a write operation
	using a new API on the Row interface and ExecuteOptions for SQL writing
	queries.  The row metadata can be any JSON construct: Object, array,
	String, Number, Boolean, or null.  This metadata can be retrieved
	through the table API or through a new SQL row_metadata function.
	User-supplied metadata is also available in the Streams API.  See
	Known Issues (DOC team add hot link to section) in the Release Notes
	for additional information.

    \[KVSTORE-2627\]

### Bug and Performance Fixes

1.  Fixed a bug where, when running multiple commands on the admin CLI, the
	exit code of runadmin was always zero. After the fix, runadmin returns
	the exit code of the last executed admin command.

    \[KVSTORE-2582\]

2.  Fixed a bug where one StreamAuthenticator was configured to support all the
	stream feeders and the service dispatcher.   When a large number of regions are
	configured it is possible to get a security check failure.   After the fix, mutiple
	StreamAuthenticators are configured.

    \[KVSTORE-2808\]

3.  Changed how hard rollback is handled.

	Previously, whenever master replica syncup required the rolling back of
	any completed transaction, the rollback would be logged as a SEVERE message.
	The system would attempt a restart and throw a RollbackProhibitedException
	if TXN_ROLLBACK_DISABLED was configured to true, or if more transactions
	than set in TXN_ROLLBACK_LIMIT were rolled back.  Now rollback and restart
	will always happen. If only non-durable transactions are rolled back then
	the rollback will be logged at the INFO level; if durable transactions
	are rolled back the rollback will be logged at the SEVERE level.

	As a result of this change, the exception RollbackProhibitedException
	has been deprecated, and the configurations TXN_ROLLBACK_DISABLED and
	TXN_ROLLBACK_LIMIT have been disabled and deprecated.

    \[KVSTORE-2636\] \[KVSTORE-2686\]

4.  Fixed a case where a durable transaction could be rolled back after a change in mastership.

	After a mastership change, JE will no longer advance the DTVLSN until a
	quorum has acknowledged a new record. After a node becomes master, it will
	now commit a NullTxn as a way to ensure that any in-flight non-durable
	transactions at the time of failure in the preceding term are made durable
	in a new term without having to wait for the application itself to create
	a durable transaction. That is, it speeds up the final state of these
	non-durable transactions, since the ack of the null txn advances the DTVLSN.
	See Section 5.4.2 at https://raft.github.io/raft.pdf.

    \[KVSTORE-2657\]

5.  Fixed a possible quorum loss in arbiter if one node fails.
	When a cluster was started with two replication nodes and an arbiter, and
	one replication node was isolated, and an attempt of writes was performed
	to the other replication node, then the writes failed because the arbiter
	could not start the replica-feeder handshake in time.

    \[KVSTORE-2654\]

6.  Fixed an issue where an internal thread pool may create excessive threads under
	certain circumstances such as repeated network connections. The issue may be
	observed as higher CPU utilization. A thread dump would typically reveal
	many threads with the name pattern "NioEndpointGroup.backup".

    \[KVSTORE-2719\]

7.  Fixed a bug where after the partition scan is complete (and the fact
	persisted) the source shard thinks the partition is now owned by the target
	shard. However the failure prevents the EOD from being sent so the
	target shard never completes the transfer. The failure causes the source to
	cancel the migration, but no action is taken to restore the partition, making
	a partition unavailable for extended period of time.
	The failure on the source did not result in a migration error state
	on the target. The target simply aborted the migration and
	rescheduled it with the target migration state == PENDING.
	The monitor thread on the source will only take action if the
	target migration state is ERROR.

	Partition migration state flow before fix :
	PENDING --> RUNNING --> PENDING 

	Made a fix where LAST_RECORD_MARKER asserts that last record has been
	processed by the source.
	Made changes in the MigrationTarget, whenever the EOD is
	not received on the Target shard and LAST_RECORD_MARKER has been received
	by the Target, it will cancel the migration resulting in target migration
	state as ERROR.
	
	TargetMonitorExecutor sees the target state as ERROR, leading to the failed
	method being called. The failed method calls manager.removeRecord, removing
	the source migration record and updating the local topology to rollback
	ownership of the partition to the source shard.

	Partition migration state flow after fix :
	PENDING --> RUNNING --> ERROR

    \[KVSTORE-456\]

8.  Fixed an issue where executions of DDL operations through KVStore.execute()
	may repeatedly fail with AuthenticationRequiredException even after
	re-authentication. This failure throws an exception with stack trace
	similar to the following:

	oracle.kv.AuthenticationRequiredException: Authentication required for access
			at oracle.kv.impl.security.AccessCheckerImpl.checkAccess(AccessCheckerImpl.java:216)
			at oracle.kv.impl.security.ExecutionContext.create(ExecutionContext.java:159)
			at oracle.kv.impl.security.SecureProxy$CheckingHandler$1.execute(SecureProxy.java:561)
			at oracle.kv.impl.security.SecureProxy$CheckingHandler$1.execute(SecureProxy.java:552)
			at oracle.kv.impl.fault.ProcessFaultHandler.execute(ProcessFaultHandler.java:147)
			at oracle.kv.impl.security.SecureProxy$CheckingHandler.invoke(SecureProxy.java:550)
			at oracle.kv.impl.security.SecureProxy.invokeNonObject(SecureProxy.java:109)
			at oracle.kv.impl.util.AbstractInvocationHandler.invoke(AbstractInvocationHandler.java:107)
			at jdk.proxy2.$Proxy13.canHandleDDL(Unknown Source)

    \[KVSTORE-2679\]

9.  Fixed a bug where the DDL for functional indexes was incorrectly generated by
	the Migrator tool and in the "show ddl" output from the SQL shell.

    \[KVSTORE-2702\]

10. Improved the JSON representation of indexes in the table schema to better
	support functional indexes in the output of DESC AS JSON TABLE.

    \[KVSTORE-2724\]

11. Fixed a bug in NUMBER value serialization: when a negative NUMBER value
	contained two consecutive zero bytes at even positions(starting from index 0,
	e.g., -2.0001), it was not serialized correctly, which could result in incorrect
	sorting order.

	For example:
	
	create table foo(id integer, n number, primary key(id));\
	insert into foo values(1, -2.001);\
	insert into foo values(2, -2.0001);\
	insert into foo values(3, -2);\
	select * from foo order by n;
	
	Returned:
	
	{"id":1,"n":-2.001}
	{"id":3,"n":-2}
	{"id":2,"n":-2.0001}
	
	Expected result:
	
	{"id":1,"n":-2.001}
	{"id":2,"n":-2.0001}
	{"id":3,"n":-2}
	
	The fix updates the serialization logic but does not modify existing data in
	the store. To correct affected values, applications must rewrite the
	corresponding rows. Alternatively, the Migrator tool can be used to export and
	re-import the data to apply the updated serialization.
	
	The fix applies only to problematic NUMBER values, specifically, negative values
	that contain two consecutive zero bytes at even positions. If no such values
	exist in the data, no action is needed.

    \[KVSTORE-2514\]

### Changes in 25.1.13 Community Edition

### New Features

1.  Extended the SQL dialect of Oracle NoSQL to support inner joins among tables in the same table hierarchy. Two or more tables in the same hierarchy may be joined in a query. There are no restrictions on the position of the tables within the hierarchy. Self-joins (joining a table with itself) are also allowed. However, to avoid distributed joins (i.e., joins where matching rows may be located in different shards) the following restriction applies:
    
    The join predicates must include equality predicates between all the shard-key columns of the joined tables. More specifically, for any pair of joined tables, a row from one table will match with a row from the other table only if the 2 rows have the same values on their shard-key columns. This guarantees that the 2 rows will be located on the same replication node.
    
    See the following for an example of this feature. To show some examples of join queries we use the following tables that model a (simple) email application:
    
    create table users(uid integer, user\_info json, primary key(uid))
    
    create table users.messages(msgid integer, msg\_info json, primary key(msg\_id))
    
    create table users.inbox(msgid integer, primary key(msgid))
    
    create table users.sent(msgid long, primary key(msgid))
    
    create table users.deleted(msgid long, primary key(msgid))
    
    The users table stores information for each user account. The users.messages table is a child table of users and stores each message sent/received from each user. The other 3 tables are also children of users and they model different email folders. They store a message primary key (uid and msgid) for the messages that belong to the corresponding folder. All tables share the same shard-key column, i.e., the uid column. A sample user row and an associated message row are shown below:
    
    {
      "uid" : 2,
      "user\_info" : {
          "userName" : "mark",
          "emailAddr" : "mark.doe@example.com",
          "firstName" : "Mark",
          "lastName" : "Doe"
          "organization" : "NoSQL"
      }
    }
    
    {
      "uid" : 2,
      "msgid" : 10,
      "msg\_info" : {
          "sender" : "mark",
          "receivers" : \[ "dave", "john", "george" \],
          "views" : \[ "2024-07-01", "2024-07-02", "2024-07-05" \],
          "size" : 20,
          "date" : "2024-07-01",
          "thread\_id" : 1
      }
    }
    
    The following query selects the messages that (a) belong to users working on NoSQL, and (b) are in the inbox folder of their corresponding user. The join predicates "msg.uid = u.uid" and "msg.uid = inbox.uid" make the query satisfy the above restriction.
    
    SELECT msg.uid, msg.msgid, msg.msg\_info
    FROM users u, users.inbox inbox, users.messages msg
    WHERE msg.uid = u.uid and msg.uid = inbox.uid and
          msg.msgid = inbox.msgid and
          u.user\_info.organization = "NoSQL"
    
    The following query returns for each message M in the inbox folder, the number of messages that belong to the same user and the same thread as M.
    
    SELECT msg1.uid, msg1.msgid, count(\*)
    FROM users.messages msg1, users.messages msg2, users.inbox inbox
    WHERE msg2.uid = msg1.uid and
          msg1.uid = inbox.uid and
          msg2.msg\_info.thread\_id = msg1.msg\_info.thread\_id and
          msg1.msgid != msg2.msgid
    GROUP BY msg1.uid, msg1.msgid
    
    The following query groups the inbox messages of each user according to their view date and computes the number of messages viewed per date. Notice that in this query we have to use the "$" in the alias of the users.messages table. Without the "$", the unnesting expression in the FROM clause would be msg.content.views\[\], and msg.content would be interpreted as a table name.
    
    SELECT $msg.uid, $view\_date, count(\*)
    FROM users.messages $msg, users.inbox $inbox, $msg.content.views\[\] as $view\_date
    WHERE $msg.uid = $inbox.uid and $msg.msgid = $inbox.msgid
    GROUP BY $msg.uid, $view\_date
    
    \[KVSTORE-2193\]
    
2.  Extended the SQL UPDATE statement to support updates of json fields via the json merge patch specification (described here: https://datatracker.ietf.org/doc/html/rfc7386). For example, assuming that "info" is a column of type json in a table called "Foo", the following query updates the info column of the row with id = 0. It sets the value of the "state" field inside the "address" field to "OR". It also sets the value of the "firstName" field to the value of the $name external variable. If any of the targeted fields ("address", "state", and "firstName") does not exist in info, it is inserted.
    
    update Foo f
    json merge f.info with patch { "address" : { "state" : "OR" }, "firstName" : $name }
    where id = 0
    
    And the following query removes the "city" field, if it exists, from the address of the row with id = 2.
    
    update Foo f
    json merge f.info.address with patch { "city" : null }
    where id = 2
    
    \[KVSTORE-1015\]
    
3.  Add support for updating multiple records if the query predicate contains a shard key.
    
    Here is an example:
    
    create table users(groupId string,
                       uid integer,
                       name string,
                       level integer,
                       primary key(shard(groupId), uid))
    
    insert into users values("g01", 1, "alex", 0)
    insert into users values("g01", 2, "jack", 0)
    insert into users values("g02", 1, "flex", 0)
    
    update users set level = level + 1 where groupId = "g01"
    {"NumRowsUpdated":2}
    
    select \* from users;
    {"groupId":"g01","uid":1,"name":"alex","level":1}
    {"groupId":"g01","uid":2,"name":"jack","level":1}
    {"groupId":"g02","uid":1,"name":"flex","level":0}
    
    \[KVSTORE-2292\]
    
4.  Group commit for transactions on the Master node
    
    Transactions on the master node that use a SyncPolicy.SYNC can now be configured to be fsynced as a group instead of requiring each transaction to be individually fsynced. This can greatly improve performance by reducing the number of fsyncs. A transaction will be considered durable when a quorum of nodes has fsynced and acknowledged the transaction.
    
    Transactions using this feature will be fsynced after either a configured number of transactions have been buffered, or a configured time interval has passed. The configurations are:
    
    *   ReplicationConfig.MASTER\_GROUP\_COMMIT\_INTERVAL (je.rep.masterGroupCommitInterval)
    *   ReplicationConfig.MASTER\_MAX\_GROUP\_COMMIT (je.rep.masterMaxGroupCommit)
    
    This feature is disabled by default.
    
    \[KVSTORE-2502\]
    
5.  DbVerify will no longer fail when run on a newly created environment
    
    DbVerify fails if it cannot find the MetadataTable when run on a KV environment, since this indicates catastrophic corruption in KV. However, this is an acceptable condition when the KV environment is newly created as the MetadataTable has not been created yet, so this failure is no longer reported for newly created environments.
    
    \[KVSTORE-2522\]
    
6.  EnvironmentFailureException error messages now include a suggestion on how to handle the error.
    
    The suggestions are:
    
    *   RESTART\_PROCESS - Restart the process.
    *   REOPEN\_ENVIRONMENT - Close and reopen the environment.
    *   HUMAN\_INTERVENTION - Close the process and wait for human intervention.
    
    The suggestion can also be retrieved programatically by calling EnvironmentFailureException.getSuggestion().
    
    \[KVSTORE-2279\]
    

### Bug and Performance Fixes

1.  Fixed an issue that in rare cases, an elasticity operation under network partition can temporarily result in data being hosted on two rep groups. This is a split-brain issue and can cause data loss. An indication of the issue is that the two messages like the following can be observed one after another:
    
    2025-02-28 15:39:22.527 UTC INFO \[rg1-rn1\] Migration source detected failure of SourceRecord\[PARTITION-493, rg1, rg2-rn1\], target returned PENDING, removing completed record
    2025-02-28 15:40:32.142 UTC SEVERE \[rg1-rn1\] uncaught exception in thread
    java.lang.IllegalStateException: \[PARTITION-493\]  shard=rg1 is associated with the same shard in both the current(seq #: 1,301) and local topologies but is associated with a different shard rg2 in the new topology(seq#: 1,302).
    
    The issue may eventually recover by itself when a new topology is broadcast.
    
    \[KVSTORE-2276\]\[KVSTORE-2640\]
    
2.  Fixed a bug that write operations may fail in some cases in absence of elastic operation.
    
    \[KVSTORE-2610\]
    
3.  Fixed a bug where write operations including puts and deletions might be missed in elastic operations.
    
    \[KVSTORE-2373\]
    
4.  Fixed a bug where a subscription stream might fail to re-authenticate itself during streaming if the source kvstore is overloaded.
    
    \[KVSTORE-2571\]
    
5.  Modified the implementation of the Password store file format so it will refuse to store alias names or secret values that start or end with a space or other whitespace character. Previously these whitespace characters were trimmed from the values stored, meaning that aliases or secrets with these whitespace characters would not work correctly. Leading and trailing whitespace is still permitted for aliases and secrets stored in an Oracle Wallet.
    
    \[KVSTORE-2437\]
    
6.  Fixed an issue where the ping command when used as a single subcommand within runadmin would return a zero exit code regardless of ping's output. For example, if one of the shards in the kvstore does not have quorum, the following command would return zero exit code:
    
    java -jar KVHOME/kvstore.jar runadmin -host <hostname> -port <portname> ping
    
    Now this command will return the correct error code of 3, similar to the stand-alone ping utility command.
    
    \[KVSTORE-2163\]
    
7.  Modified Admin CLI commands 'topology change-repfactor', 'topology rebalance', and 'topology redistribute' to check if newly added SN storage directory sizes are smaller than existing in-use sizes. In this case, the topology will not be able to use new SNs. The topology commands now include information about the issue to alert users.
    
    A sample warning message:
    
    Note: Some new SN storage directory sizes are smaller than the smallest
    existing storage directory size, which may prevent new SNs from being
    used in the new topology. The smallest existing SN storage directory
    size is 2147483648, but the following smaller new SN storage directory
    sizes were specified: {sn4={/usr/kvroot4=1073741824},
    sn5={/usr/kvroot5=1073741824}}
    
    \[KVSTORE-2036\]
    
8.  When setting a storage directory size parameter on an SN, the system now verifies that the specified sizes for all storage directories on the same device do not sum to a size exceeding the total storage size of the device. Before the fix, makebootconfig and 'plan change-storagedir' checked the size of each storage directory, but not the sum of all directories on the same device.
    
    \[KVSTORE-1684\]

9.  Fixed a problem that could cause the JVM to fail when starting a replica node process because of unsupported JVM arguments. The fix is to identify known obsolete arguments for a specific Java version and exclude them before the process is started.

    \[KVSTORE-2509\]    
    
10.  Fixed an issue in Admin CLI pool create command so that it is no longer possible to use the empty string or a string of blank spaces when specifying a pool name or zone name. The system now returns a warning instead.
    
     \[KVSTORE-1694\]
    
 ### Deprecations

 None
