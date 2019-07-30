# LN Gossip
Simulation of gossip protocol in the lightning network. Aimed to compare the latency and bandwidth in flooding, inventory based and set reconciliation based gossip protocols. 

Data gathered by running a [forked](https://github.com/carlaKC/lnd/tree/carla-tracklightningmessages) mainnet LND node which saves records of every incoming/outgoing wire message, as well as specific messages for `channel_update` and `channel_annoucment` since these messages produce the majoirty of bandwidth usage on the network (the fork could be extended to include further message types if desired).

#### Prerequisites
A connection to a `wirewatcher` DB with `channel_updates`, `ln_messages`, `channel_announcements` tables populated must be provided. 

A connection to a `lngosisp` database with the following schema is required:
```
create table received_messages(
	uuid bigint, 
	node_id varchar(255), 
	first_seen int,
	last_seen int,
	seen_count int, 
	label varchar(100),

	primary key(uuid, node_id)
);
``` 
A copy of the channel graph as obtained from LND's describe graph endpoint. 

#### Install
Get and install the project:

`go get github.com/carlaKC/lngossip`

`go install github.com/carlaKC/lngossip`

Run the executable with the required flags:

`$GOPATH/bin/lngossip`
 * `--db_label={label uniquely identifying simulation}`
 * `--start_time={start time of date set with format Y-M-D H:M:S}` 
 * `--duration_minutes={load messages until start+duration}`
 * `--db={DB URI}`
 * `--wirewatcher_db={wirewatcher DB URI}`
 * `--chan_graph={path to channel graph obtained from describe graph}`


#### Relay Behaviour
This simulator aims to replicate the following relay protocols:
1. The existing relay protocol as specified in Bolt 11
2. An inventory based relay protocol
3. Set reconcilliation using minisketch

The base case behaviour aims to be implementation agnostic, so rules for relay are taken directly from the Bolt rather than examining any specific implementation.

Modelled Flooding Behaviour:
1. Nodes forward messages to all peers that have not previously sent them the message
2. If nodes receive a channel update that they already have a newer timestamp for, they do not forward it. 
3. (? TODO: de-duplicate per tick, check whether in spec/ implementations)