## Module - Leader Election

This module will include different leader election mechanism in Adaptive Leader Replacement (PBFT).

Now this module supports RoundRobin, Paxos. Raft and other mechanism are pending to develop.

---
In this module, each node will hold a gRPC connection to other nodes for the potential consensus for leader election (some leader election process is done by a consensus instance).

That is, we have a specific port for each node, not the one for common consensus process, to go through the leader election.

Port: Common Node Port + 2000