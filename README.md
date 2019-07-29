# LN Gossip
Simulation of gossip protocol in the lightning network. Aimed to compare the latency and bandwidth in flooding, inventory based and set reconciliation based gossip protocols. 

Data gathered by running a [forked](https://github.com/carlaKC/lnd/tree/carla-tracklightningmessages) mainnet LND node which saves records of every incoming/outgoing wire message, as well as specific messages for `channel_update` and `channel_annoucment` since these messages produce the majoirty of bandwidth usage on the network (the fork could be extended to include further message types if desired).

#### Relay Behaviour
This simulator aims to replicate the following relay protocols:
1. The existing relay protocol as specified in Bolt 11
2. An inventory based relay protocol
3. Set reconcilliation using minisketch

The base case behaviour aims to be implementation agnostic, so rules for relay are taken directly from the Bolt rather than examining any specific implementation.

Channel Announcements:
* Validation of messages: this would make the simulation a lot more computationally heavy; should there be a high portion of invalid messages in the data collected, which is doubtful, they will affect each relay protocol equally
* If the transaction referred to was not previously announced as a channel the message should be queued for rebroadcast
* If the transaction was previously used for a channel announcement, the node should blacklist the previous nodes that announced the channel


Channel Updates:
* If signature is not valid, do not process the message further
* If the timestamp is not greater than the last received channel update for the channel ID and node ID, ignore the message
* If the timestamp is equal to the last received channel update and the fields other than signature differ you may blacklist the node
* If the timestamp is unreasonably far in the future you may ignore the message
* Otherwise queue the message for rebroadcasting

Open Questions on behavior:
1. Do we de-dup channel announcements? "not previously announced as a channel" then queue

Modeled Behaviour:
1. Nodes forward messages to all peers that have not previously sent them the message
2. If nodes receive a channel update that they already have a newer timestamp for, they do not forward it. 
