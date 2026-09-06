# Architecture Overview

## High-Level Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                          CLIENT SIDE                            │
│                                                                 │
│   ┌──────────────┐        ┌─────────────────────────────────┐   │
│   │ Client-cli   │──────▶ │           Client                │   │
│   │ (main CLI)   │        │  (connect, put, get, erase,     │   │
│   └──────────────┘        │   migrate_slot, import_slot)    │   │
│                           └────────────┬────────────────────┘   │
└────────────────────────────────────────│────────────────────────┘
                                         │ TCP (client_port)
                                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                          SERVER SIDE                            │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                        Node                             │   │
│   │  main_loop()  ◀── Epoll ◀── Socket (client_port)        │   │
│   │  gossip()     ◀── thread  ◀── Socket (cluster_port)     │   │
│   │                                                         │   │
│   │  ┌──────────────────┐   ┌───────────────────────────┐   │   │
│   │  │ InstructionHandler│  │     ClusterState          │   │   │
│   │  │ handle_put        │  │  nodes: ClusterNode map   │   │   │
│   │  │ handle_get        │  │  slots: Slot[3]           │   │   │
│   │  │ handle_erase      │  │  myself: ClusterNode      │   │   │
│   │  │ handle_meet       │  └───────────────────────────┘   │   │
│   │  │ handle_migrate    │                                   │   │
│   │  │ handle_import     │   ┌───────────────────────────┐   │   │
│   │  │ handle_get_slots  │   │    IKeyValueStore         │   │   │
│   │  └──────────────────┘   │    (InMemoryKVS)           │   │   │
│   │                         │  put / get / erase         │   │   │
│   │  ┌──────────────────┐   └───────────────────────────┘   │   │
│   │  │ ProtocolHandler  │                                    │   │
│   │  │ get_metadata     │                                    │   │
│   │  │ get_command      │                                    │   │
│   │  │ get_payload      │                                    │   │
│   │  │ send_instruction │                                    │   │
│   │  └──────────────────┘                                    │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│         cluster_port ◀──────────────▶ cluster_port              │
│              (gossip / MEET / MIGRATE between nodes)             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Layer Breakdown

### 1. `net/` — Networking Primitives

#### `FileDescriptor`
RAII wrapper around a raw OS file descriptor. Non-copyable, movable. Closes the fd on destruction.

| Member | Description |
|--------|-------------|
| `FileDescriptor(int fd)` | Wraps an existing fd |
| `~FileDescriptor()` | Closes the fd |
| `unwrap() → int` | Returns the raw fd value |
| `operator int()` | Implicit conversion to int |

---

#### `Connection`
Wraps a `FileDescriptor` (via `shared_ptr`) plus an optional `sockaddr_in` for the remote peer. Represents one established TCP connection.

| Member | Inputs | Output | Description |
|--------|--------|--------|-------------|
| `Connection(FileDescriptor&&, sockaddr_in)` | fd, peer addr | — | Full constructor |
| `Connection(FileDescriptor&&)` | fd | — | No peer addr |
| `fd() → int` | — | raw fd | Returns the file descriptor |
| `is_connected() → bool` | — | bool | True if fd is valid |
| `send(string)` | data | bytes sent | Sends a string |
| `send(char*, size)` | data, len | bytes sent | Sends raw bytes |
| `send(span<char>)` | span | bytes sent | Sends a span |
| `receive(char*, size)` | buf, len | bytes received | Blocking receive into buffer |
| `receive(span<char>)` | span | bytes received | Blocking receive into span |
| `receive_all(ostream&)` | stream | bytes received | Drains all available data into stream |

Free functions:
- `net::send(fd, span)` / `net::send(fd, char*, size)` — raw fd sends
- `net::receive(fd, span)` / `net::receive(fd, char*, size)` — raw fd receives

---

#### `Socket`
Manages a listening or connecting TCP socket.

| Member | Inputs | Output | Description |
|--------|--------|--------|-------------|
| `Socket()` | — | — | Creates a TCP socket fd |
| `listen(port, queue_size=50)` | port | — | Binds and listens on port |
| `set_non_blocking() → bool` | — | success | Sets O_NONBLOCK |
| `set_keep_alive() → bool` | — | success | Sets SO_KEEPALIVE |
| `accept() → Connection` | — | Connection | Accepts an incoming connection |
| `connect(addr, port) → Connection` | ip string, port | Connection | Connects to remote host |
| `connect(port) → Connection` | port | Connection | Connects to localhost |
| `fd() → int` | — | raw fd | Returns underlying fd |

Free functions:
- `is_listening(fd)` / `is_listening(FileDescriptor&)` — checks if fd is in listening state

---

#### `Epoll`
Wraps Linux `epoll` for event-driven I/O multiplexing.

| Member | Inputs | Output | Description |
|--------|--------|--------|-------------|
| `Epoll(max_events=10)` | max_events | — | Creates epoll fd |
| `add_event(fd, events)` | fd, epoll flags | — | Registers fd with epoll (default: EPOLLIN|EPOLLET) |
| `remove_event(fd)` | fd | — | Removes fd from epoll |
| `reset_occurred_events()` | — | — | Clears the events buffer |
| `wait(timeout=-1) → int` | timeout ms | event count | Blocks until events arrive |
| `get_events() → vector<epoll_event>` | — | events | Returns the triggered events |
| `get_event_fd(index) → int` | index | fd | Gets fd from event at index |
| `get_epoll_fd() → int` | — | epoll fd | Returns the epoll fd itself |

---

### 2. `utils/` — Utility Types

#### `ByteArray`
Reference-counted byte buffer. Holds a `shared_ptr<IByteArrayResource>`. Shallow copies share the underlying buffer.

| Member | Inputs | Output | Description |
|--------|--------|--------|-------------|
| `new_allocated_byte_array(size)` | uint64 | ByteArray | Allocates a zeroed buffer |
| `new_allocated_byte_array(char*, size)` | ptr, size | ByteArray | Shallow copy (shared ownership) |
| `new_allocated_byte_array(string&)` | string | ByteArray | Deep copy from string |
| `new_allocated_byte_array(string&&)` | string | ByteArray | Move from string |
| `data() → char*` | — | ptr | Raw pointer to buffer |
| `size() → uint64_t` | — | size | Buffer size in bytes |
| `to_string() → string` | — | string | Copies buffer into std::string |
| `insert_byte_array(other, offset=0)` | ByteArray, offset | — | Copies `other` into this at offset |
| `resize(target_size)` | size | — | Reallocates the buffer |

`IByteArrayResource` — abstract resource interface (data, size, resize).
`AllocatedByteArrayResource` — concrete implementation; supports deep copy (owns buffer) and shallow copy (borrows pointer).

---

#### `Status`
Immutable result type. Constructed only via static factory methods.

| Factory | Description |
|---------|-------------|
| `Status::new_ok()` | Success |
| `Status::new_not_found(msg)` | Key not present |
| `Status::new_not_supported(msg)` | Operation not supported |
| `Status::new_invalid_argument(msg)` | Bad input |
| `Status::new_not_enough_memory(msg)` | Allocation failure |
| `Status::new_error(msg)` | Generic error |
| `Status::new_unknown_response(msg)` | Unrecognized protocol response |

Predicates: `is_ok()`, `is_not_found()`, `is_error()`, etc. `get_msg() → string`.

---

#### `Options` / `ReadOptions` / `WriteOptions`
Currently empty tag types passed to KVS operations for future extensibility.

---

### 3. `KVS/` — Key-Value Store

#### `IKeyValueStore` (abstract)
Interface for all KVS backends. Non-copyable.

| Method | Inputs | Output | Description |
|--------|--------|--------|-------------|
| `put(key, value, options)` | string key, ByteArray value | Status | Insert or overwrite a key |
| `get(key, value, options)` | string key, ByteArray& out | Status | Read value into `out`; returns NOT_FOUND if missing |
| `erase(key, options)` | string key | Status | Delete a key |
| `contains_key(key)` | string key | bool | True if key exists |
| `get_size()` | — | uint64 | Number of stored keys |

---

#### `InMemoryKVS : IKeyValueStore`
Hash-map backed implementation. Stores `unordered_map<string, ByteArray>`.

Implements all five `IKeyValueStore` methods. `get` supports `ReadOptions` offset/size for partial reads. `put` supports `WriteOptions` offset for partial writes.

---

### 4. `node/` — Distributed Node

#### `Cluster.hpp` — Data Structures & Gossip

**Constants**
- `CLUSTER_AMOUNT_OF_SLOTS = 3` — total keyspace slots
- `CLUSTER_NAME_LEN = 40`, `CLUSTER_IP_LEN = 15`

**Key structs**

| Struct | Fields | Description |
|--------|--------|-------------|
| `ClusterNodeGossipData` | name, ip, cluster_port, client_port, served_slots (bitset), num_slots_served | Wire-serializable node info |
| `ClusterNode` | extends GossipData + `outgoing_link: Connection` | Full node with live connection |
| `Slot` | served_by, amount_of_keys, state (NORMAL/MIGRATING/IMPORTING), migration_partner | Per-slot metadata |
| `SlotGossipData` | slot_number, amount_of_keys, state, migration_partner_name, served_by_name | Wire-serializable slot info |
| `ClusterState` | nodes (map), size, slots (vector), myself, part_of_cluster | Full cluster view held by each node |
| `ClusterGossipMsg` | nodes[], slots[], sender | Gossip message payload |

**Free functions**

| Function | Inputs | Output | Description |
|----------|--------|--------|-------------|
| `send_ping(link, state)` | Connection*, ClusterState | — | Sends gossip PING to one peer |
| `send_ping(state)` | ClusterState | — | Sends PING to all known peers |
| `handle_ping(link, state, cmd)` | Connection, ClusterState, Command | — | Merges received gossip into local state |
| `add_node(state, name, ip, cluster_port, client_port)` | ClusterState, node params | Status | Registers a new node in the cluster |
| `check_key_slot_served_and_send_moved(key, conn, state)` | key, Connection, ClusterState | bool | If this node doesn't serve key's slot, sends MOVED redirect; returns false |
| `check_slot_served_and_send_moved(slot, conn, state)` | slot#, Connection, ClusterState | bool | Same but by slot number |
| `get_key_hash(key) → uint16_t` | key string | slot# | Hashes key (respecting `{tag}` syntax) to a slot index |

---

#### `ProtocolHandler.hpp` — Wire Protocol

**Packet format:**
```
[ MetaData ] [ cmd1_size | cmd1 ] ... [ cmdN_size | cmdN ] [ payload_size | payload ]
```

**`MetaData` struct:** `argc` (uint16), `instruction` (uint8 enum), `command_size` (uint64), `payload_size` (uint64)

**`Instruction` enum (15 values):**
`PUT, GET, ERASE, GET_RESPONSE, OK_RESPONSE, ERROR_RESPONSE, CLUSTER_PING, MEET, MOVE, IMPORT_SLOT, MIGRATE_SLOT, ASK, NO_ASKING_ERROR, CLUSTER_MIGRATION_FINISHED, GET_SLOTS`

**Command field enums** — typed index constants for each instruction's argument vector (e.g. `CommandFieldsPut::c_KEY = 0`).

**Free functions**

| Function | Inputs | Output | Description |
|----------|--------|--------|-------------|
| `get_metadata(conn)` | Connection | MetaData | Reads and deserializes the metadata header |
| `get_command(conn, argc, cmd_size)` | Connection, counts | Command (vector\<string\>) | Reads the command arguments |
| `get_payload(conn, size) → ByteArray` | Connection, size | ByteArray | Reads raw payload bytes |
| `get_payload(conn, dest, size)` | Connection, char*, size | — | Reads payload into existing buffer |
| `send_instruction(conn, cmd, instr, payload, size)` | Connection, Command, Instruction, optional payload | bytes sent | Serializes and sends a full packet |
| `send_instruction(conn, status)` | Connection, Status | bytes sent | Sends OK_RESPONSE or ERROR_RESPONSE from a Status |
| `send_instruction(conn, cmd, instr, string payload)` | Connection, Command, Instruction, string | bytes sent | Overload with string payload |
| `get_command_size(cmd)` | Command | uint64 | Computes total serialized command byte size |
| `serialize_command(cmd, buf)` | Command, span\<char\> | — | Writes command into buffer |
| `serialize_slots(slots, conn)` | vector\<Slot\>, Connection | — | Serializes slot info and sends it |

---

#### `InstructionHandler.hpp` — Request Handlers

All functions take a `Connection&` (to reply on), relevant `Command`, the local `IKeyValueStore`, and/or `ClusterState`. They perform the operation and send a response back on the connection.

| Function | Key Behavior |
|----------|--------------|
| `handle_put(conn, metadata, cmd, kvs, state)` | Checks slot ownership; reads payload; writes to KVS; replies OK or ERROR |
| `handle_get(conn, cmd, kvs, state)` | Checks slot ownership; reads from KVS with offset/size; replies GET_RESPONSE or NOT_FOUND |
| `handle_erase(conn, cmd, kvs, state)` | Checks slot ownership; deletes key from KVS; replies OK or NOT_FOUND |
| `handle_meet(conn, cmd, state)` | Adds the described node to ClusterState via `cluster::add_node` |
| `handle_migrate_slot(conn, cmd, state)` | Marks slot as MIGRATING; streams all keys in that slot to the importing node |
| `handle_import_slot(conn, cmd, state)` | Marks slot as IMPORTING; receives streamed keys and writes them to local KVS |
| `handle_migration_finished(cmd, state)` | Updates slot state back to NORMAL after migration completes |
| `handle_get_slots(conn, cmd, state)` | Serializes and sends the full slot table to the requester |

---

#### `Node` — Main Server Class

Owns the KVS, ClusterState, Epoll, and all active connections. Entry point is `start()`.

| Member | Description |
|--------|-------------|
| `new_in_memory_node(name, client_port, cluster_port, ip, serve_all_slots)` | Static factory; creates Node with InMemoryKVS |
| `start()` | Sets running=true, spawns gossip thread, calls main_loop() |
| `stop()` | Sets running=false, detaches gossip thread |
| `stop_gossiping()` | Stops gossip thread without stopping main loop |
| `main_loop()` (private) | Epoll event loop: accepts new connections on client/cluster sockets, dispatches `handle_connection` for readable fds |
| `gossip()` (private) | Background thread: periodically calls `cluster::send_ping(state)` to all peers |
| `handle_connection(conn)` | Reads MetaData+Command from connection, calls `execute_instruction` |
| `execute_instruction(conn, metadata, cmd)` | Dispatch table: routes each `Instruction` to the correct `instruction_handler::handle_*` |
| `disconnect(conn)` (private) | Removes connection from epoll and fd map |
| `get_kvs()` | Returns reference to KVS |
| `get_cluster_state()` | Returns reference to ClusterState |
| `set_cluster_state(state)` | Replaces ClusterState |
| `get_connections_epoll()` | Returns reference to the Epoll instance |

**Internal state:**
- `kvs_` — unique_ptr to IKeyValueStore
- `cluster_state_` — ClusterState
- `connections_epoll_` — Epoll
- `fd_to_connection_` — map from fd int to Connection
- `running_`, `gossiping_` — atomic bools
- `gossip_thread_` — std::thread

---

### 5. `client/` — Client Library

#### `Client`
Manages connections to multiple nodes and routes requests to the correct node based on slot ownership.

| Method | Inputs | Output | Description |
|--------|--------|--------|-------------|
| `connect_to_node(ip, port)` | ip, port | Status | Opens TCP connection to a node; stores in `nodes_connections_` |
| `disconnect_all()` | — | — | Closes all connections |
| `put_value(key, ByteArray, offset)` | key, value, offset | Status | Hashes key to slot, routes to owning node, sends PUT |
| `put_value(key, string, offset)` | key, value string | Status | Overload |
| `put_value(key, char*, size, offset)` | key, raw bytes | Status | Overload |
| `get_value(key, ByteArray&, offset, size)` | key, out buffer | Status | Routes GET to correct node; fills buffer |
| `erase_value(key)` | key | Status | Routes ERASE to correct node |
| `get_update_slot_info()` | — | Status | Sends GET_SLOTS to a node; updates `slots_nodes_` cache |
| `migrate_slot(slot, ip, port)` | slot#, target node | Status | Sends MIGRATE_SLOT instruction |
| `import_slot(slot, ip, port)` | slot#, source node | Status | Sends IMPORT_SLOT instruction |
| `add_node_to_cluster(name, ip, client_port, cluster_port)` | node params | Status | Sends MEET instruction to introduce a new node |
| `get_slot_nodes()` | — | array ref | Returns the slot→node mapping cache |
| `get_nodes_connections()` | — | map ref | Returns all open connections |

**Private routing helpers:**
- `get_node_connection_by_slot(slot)` — looks up `slots_nodes_[slot]` and returns its Connection pointer
- `get_random_connection()` — returns any open connection (used when slot info is unknown)
- `handle_move(cmd, slot)` — on MOVE response, connects to redirected node and retries
- `handle_ask(cmd)` — on ASK response, retries with ASKING flag to the new node
- `handle_no_ask_error(cmd)` — on NO_ASKING_ERROR, connects to correct node without ASKING
- `update_slot_info(data)` — parses GET_SLOTS response and populates `slots_nodes_`
- `handle_slot_migration(slot, ip, port, instruction)` — shared logic for migrate/import

**Internal state:**
- `slots_nodes_` — `array<string, 3>` mapping slot index → node IP:port string
- `nodes_connections_` — `unordered_map<string, Connection>` keyed by IP:port string

---

## Data Flow: PUT Request

```
Client                          Node
  │                               │
  │──PUT(key, value, offset)─────▶│ main_loop() / epoll fires
  │                               │ handle_connection(conn)
  │                               │   get_metadata(conn)      → MetaData{PUT}
  │                               │   get_command(conn, ...)  → ["key", cur_size, offset]
  │                               │ execute_instruction(...)
  │                               │   instruction_handler::handle_put(...)
  │                               │     check_key_slot_served_and_send_moved(key)
  │                               │     get_payload(conn, payload_size) → ByteArray
  │                               │     kvs_.put(key, value)
  │◀──OK_RESPONSE─────────────────│     send_instruction(conn, Status::new_ok())
```

## Data Flow: Slot Migration

```
Client            Node A (migrating)          Node B (importing)
  │                      │                           │
  │──MIGRATE_SLOT(1,B)──▶│                           │
  │                      │──IMPORT_SLOT(1,A)────────▶│
  │                      │   (keys streamed A→B)      │
  │                      │◀──OK───────────────────────│
  │                      │  slot[1].state = NORMAL    │
  │◀──OK─────────────────│                            │
```

## Data Flow: Gossip

```
Node A (gossip thread)            Node B
  │                                  │
  │  every NODE_PING_PAUSE ms        │
  │──CLUSTER_PING(nodes, slots)─────▶│
  │                                  │  handle_ping(...)
  │                                  │  merges ClusterGossipMsg into ClusterState
```

---

## Class Dependency Graph

```
Node
 ├── IKeyValueStore ◀── InMemoryKVS
 ├── ClusterState
 │    └── ClusterNode
 │         └── Connection ◀── FileDescriptor
 ├── Epoll ◀── FileDescriptor
 ├── ProtocolHandler (free fns, uses Connection + ByteArray)
 └── InstructionHandler (free fns, uses all of the above)

Client
 ├── Connection (map of open connections)
 └── ProtocolHandler (free fns)

ByteArray
 └── IByteArrayResource ◀── AllocatedByteArrayResource

Status (standalone result type)
Options / ReadOptions / WriteOptions (empty tag types)
```
