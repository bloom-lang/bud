# BFS: A distributed file system in Bloom

In this document we'll use what we've learned to build a piece of systems software using Bloom.  The libraries that ship with Bud provide many of the building blocks we'll need to create a distributed,
``chunked'' file system in the style of the Google File System (GFS):

 * a [key-value store](https://github.com/bloom-lang/bud-sandbox/blob/master/kvs/kvs.rb) (KVS)
 * [nonce generation](https://github.com/bloom-lang/bud-sandbox/blob/master/ordering/nonce.rb)
 * a [heartbeat protocol](https://github.com/bloom-lang/bud-sandbox/blob/master/heartbeat/heartbeat.rb)

## High-level architecture

![BFS Architecture](bfs_arch.png?raw=true)

BFS implements a chunked, distributed file system (mostly) in the Bloom
language.  BFS is architecturally based on [BOOM-FS](http://db.cs.berkeley.edu/papers/eurosys10-boom.pdf), which is itself based on
the Google File System (GFS).  As in GFS, a single master node manages
file system metadata, while data blocks are replicated and stored on a large
number of storage nodes.  Writing or reading data involves a multi-step
protocol in which clients interact with the master, retrieving metadata and
possibly changing state, then interact with storage nodes to read or write
chunks.  Background jobs running on the master will contact storage nodes to
orchestrate chunk migrations, during which storage nodes communicate with
other storage nodes.  As in BOOM-FS, the communication protocols and the data
channel used for bulk data transfer between clients and datanodes and between
datanodes is written outside Bloom (in Ruby).

## [Basic File System](https://github.com/bloom-lang/bud-sandbox/blob/master/bfs/fs_master.rb)

Before we worry about any of the details of distribution, we need to implement the basic file system metadata operations: _create_, _remove_, _mkdir_ and _ls_.
There are many choices for how to implement these operations, and it makes sense to keep them separate from the (largely orthogonal) distributed file system logic.
That way, it will be possible later to choose a different implementation of the metadata operations without impacting the rest of the system.
Another benefit of modularizing the metadata logic is that it can be independently tested and debugged.  We want to get the core of the file system
working correctly before we even send a whisper over the network, let alone add any complex features.

### Protocol

    module FSProtocol
      state do
        interface input, :fsls, [:reqid, :path]
        interface input, :fscreate, [] => [:reqid, :name, :path, :data]
        interface input, :fsmkdir, [] => [:reqid, :name, :path]
        interface input, :fsrm, [] => [:reqid, :name, :path]
        interface output, :fsret, [:reqid, :status, :data]
      end
    end

We create an input interface for each of the operations, and a single output interface for the return for any operation: given a request id, __status__ is a boolean
indicating whether the request succeeded, and __data__ may contain return values (e.g., _fsls_ should return an array containing the array contents).

### Implementation

We already have a library that provides an updateable flat namespace: the key-value store.  We can easily implement the tree structure of a file system over a key-value store
in the following way:

 1. keys are paths
 2. directories have arrays containing child entries (base names)
 3. files values are their contents

<!--- (**JMH**: I find it a bit confusing how you toggle from the discussion above to this naive file-storage design here.  Can you warn us a bit more clearly that this is a starting point focused on metadata, with (3) being a strawman for data storage that is intended to be overriden later?)
--->
Note that (3) is a strawman: it will cease to apply when we implement chunked storage later.  It is tempting, however, to support (3) so that the resulting program is a working
standalone file system.

We begin our implementation of a KVS-backed metadata system in the following way:


    module KVSFS
      include FSProtocol
      include BasicKVS
      include TimestepNonce

If we wanted to replicate the master node's metadata we could consider mixing in a replicated KVS implementation instead of __BasicKVS__ -- but more on that later.

### Directory Listing 

The directory listing operation is implemented by a simple block of Bloom statements:

        kvget <= fsls { |l| [l.reqid, l.path] }
        fsret <= (kvget_response * fsls).pairs(:reqid => :reqid) { |r, i| [r.reqid, true, r.value] }
        fsret <= fsls  do |l|
          unless kvget_response.map{ |r| r.reqid}.include? l.reqid
            [l.reqid, false, nil]
          end
        end

If we get a __fsls__ request, probe the key-value store for the requested by projecting _reqid_, _path_ from the __fsls__ tuple into __kvget__.  If the given path
is a key, __kvget_response__ will contain a tuple with the same _reqid_, and the join on the second line will succeed.  In this case, we insert the value
associated with that key into __fsret__.  Otherwise, the third rule will fire, inserting a failure tuple into __fsret__.


### Mutation

The logic for file and directory creation and deletion follow a similar logic with regard to the parent directory:

        check_parent_exists <= fscreate { |c| [c.reqid, c.name, c.path, :create, c.data] }
        check_parent_exists <= fsmkdir { |m| [m.reqid, m.name, m.path, :mkdir, nil] }
        check_parent_exists <= fsrm { |m| [m.reqid, m.name, m.path, :rm, nil] }
    
        kvget <= check_parent_exists { |c| [c.reqid, c.path] }
        fsret <= check_parent_exists  do |c|
          unless kvget_response.map{ |r| r.reqid}.include? c.reqid
            puts "not found #{c.path}" or [c.reqid, false, "parent path #{c.path} for #{c.name} does not exist"]
          end
        end
    

Unlike a directory listing, however, these operations change the state of the file system.  In general, any state change will involve 
carrying out two mutating operations to the key-value store atomically:

 1. update the value (child array) associated with the parent directory entry
 2. update the key-value pair associated with the object in question (a file or directory being created or destroyed).

The following Bloom code carries this out:

        temp :dir_exists <= (check_parent_exists * kvget_response * nonce).combos([check_parent_exists.reqid, kvget_response.reqid])
        fsret <= dir_exists do |c, r, n|
          if c.mtype == :rm
            unless can_remove.map{|can| can.orig_reqid}.include? c.reqid
              [c.reqid, false, "directory #{} not empty"]
            end
          end
        end
    
        # update dir entry
        # note that it is unnecessary to ensure that a file is created before its corresponding
        # directory entry, as both inserts into :kvput below will co-occur in the same timestep.
        kvput <= dir_exists do |c, r, n|
          if c.mtype == :rm
            if can_remove.map{|can| can.orig_reqid}.include? c.reqid
              [ip_port, c.path, n.ident, r.value.clone.reject{|item| item == c.name}]
            end
          else
            [ip_port, c.path, n.ident, r.value.clone.push(c.name)]
          end
        end
    
        kvput <= dir_exists do |c, r, n|
          case c.mtype
            when :mkdir
              [ip_port, terminate_with_slash(c.path) + c.name, c.reqid, []]
            when :create
              [ip_port, terminate_with_slash(c.path) + c.name, c.reqid, "LEAF"]
          end
        end


<!--- (**JMH**: This next sounds awkward.  You *do* take care: by using <= and understanding the atomicity of timesteps in Bloom.  I think what you mean to say is that Bloom's atomic timestep model makes this easy compared to ... something.)
Note that we need not take any particular care to ensure that the two inserts into __kvput__ occur together atomically.  Because both statements use the synchronous 
-->
Note that because both inserts into the __kvput__ collection use the synchronous operator (`<=`), we know that they will occur together in the same fixpoint computation or not at all.
Therefore we need not be concerned with explicitly sequencing the operations (e.g., ensuring that the directory entries is created _after_ the file entry) to deal with concurrency:
there can be no visible state of the database in which only one of the operations has succeeded.

If the request is a deletion, we need some additional logic to enforce the constraint that only an empty directory may be removed:


        check_is_empty <= (fsrm * nonce).pairs {|m, n| [n.ident, m.reqid, terminate_with_slash(m.path) + m.name] }
        kvget <= check_is_empty {|c| [c.reqid, c.name] }
        can_remove <= (kvget_response * check_is_empty).pairs([kvget_response.reqid, check_is_empty.reqid]) do |r, c|
          [c.reqid, c.orig_reqid, c.name] if r.value.length == 0
        end
        # delete entry -- if an 'rm' request,
        kvdel <= dir_exists do |c, r, n|
          if can_remove.map{|can| can.orig_reqid}.include? c.reqid
            [terminate_with_slash(c.path) + c.name, c.reqid]
          end
        end


Recall that when we created KVSFS we mixed in __TimestepNonce__, one of the nonce libraries.  While we were able to use the _reqid_ field from the input operation as a unique identifier
for one of our KVS operations, we need a fresh, unique request id for the second KVS operation in the atomic pair described above.  By joining __nonce__, we get
an identifier that is unique to this timestep.


## [File Chunking](https://github.com/bloom-lang/bud-sandbox/blob/master/bfs/chunking.rb)

Now that we have a module providing a basic file system, we can extend it to support chunked storage of file contents.  The metadata master will contain, in addition to the KVS
structure for directory information, a relation mapping a set of chunk identifiers to each file

        table :chunk, [:chunkid, :file, :siz]

and relations associating a chunk with a set of datanodes that host a replica of the chunk.  

        table :chunk_cache, [:node, :chunkid, :time]

(**JMH**: ambiguous reference ahead "these latter")
The latter (defined in __HBMaster__) is soft-state, kept up to data by heartbeat messages from datanodes (described in the next section).

To support chunked storage, we add a few metadata operations to those already defined by FSProtocol:

    module ChunkedFSProtocol
      include FSProtocol
    
      state do
        interface :input, :fschunklist, [:reqid, :file]
        interface :input, :fschunklocations, [:reqid, :chunkid]
        interface :input, :fsaddchunk, [:reqid, :file]
        # note that no output interface is defined.
        # we use :fsret (defined in FSProtocol) for output.
      end
    end

 * __fschunklist__ returns the set of chunks belonging to a given file.  
 * __fschunklocations__ returns the set of datanodes in possession of a given chunk.
 * __fsaddchunk__ returns a new chunkid for appending to an existing file, guaranteed to be higher than any existing chunkids for that file, and a list of candidate datanodes that can store a replica of the new chunk.

We continue to use __fsret__ for return values.

### Lookups

Lines 34-44 are a similar pattern to what we saw in the basic FS: whenever we get a __fschunklist__ or __fsaddchunk__ request, we must first ensure that the given file
exists, and error out if not.  If it does, and the operation was __fschunklist__, we join the metadata relation __chunk__ and return the set of chunks owned
by the given (existent) file:

        chunk_buffer <= (fschunklist * kvget_response * chunk).combos([fschunklist.reqid, kvget_response.reqid], [fschunklist.file, chunk.file]) { |l, r, c| [l.reqid, c.chunkid] }
        chunk_buffer2 <= chunk_buffer.group([chunk_buffer.reqid], accum(chunk_buffer.chunkid))
        fsret <= chunk_buffer2 { |c| [c.reqid, true, c.chunklist] }

### Add chunk

If it was a __fsaddchunk__ request,  we need to generate a unique id for a new chunk and return a list of target datanodes.  We reuse __TimestepNonce__ to do the former, and join a relation
called __available__ that is exported by __HBMaster__ (described in the next section) for the latter:

        temp :minted_chunk <= (kvget_response * fsaddchunk * available * nonce).combos(kvget_response.reqid => fsaddchunk.reqid) {|r| r if last_heartbeat.length >= REP_FACTOR}
        chunk <= minted_chunk { |r, a, v, n| [n.ident, a.file, 0]}
        fsret <= minted_chunk { |r, a, v, n| [r.reqid, true, [n.ident, v.pref_list.slice(0, (REP_FACTOR + 2))]]}
        fsret <= (kvget_response * fsaddchunk).pairs(:reqid => :reqid) do |r, a|
          if available.empty? or available.first.pref_list.length < REP_FACTOR
            [r.reqid, false, "datanode set cannot satisfy REP_FACTOR = #{REP_FACTOR} with [#{available.first.nil? ? "NIL" : available.first.pref_list.inspect}]"]
          end
        end

Finally, it was a __fschunklocations__ request, we have another possible error scenario, because the nodes associated with chunks are a part of our soft state.  Even if the file
exists, it may not be the case that we have fresh information in our cache about what datanodes own a replica of the given chunk:

        fsret <= fschunklocations do |l|
          unless chunk_cache_alive.map{|c| c.chunkid}.include? l.chunkid
            [l.reqid, false, "no datanodes found for #{l.chunkid} in cc, now #{chunk_cache_alive.length}, with last_hb #{last_heartbeat.length}"]
          end
        end

Otherwise, __chunk_cache__ has information about the given chunk, which we may return to the client:

        temp :chunkjoin <= (fschunklocations * chunk_cache_alive).pairs(:chunkid => :chunkid)
        host_buffer <= chunkjoin {|l, c| [l.reqid, c.node] }
        host_buffer2 <= host_buffer.group([host_buffer.reqid], accum(host_buffer.host))
        fsret <= host_buffer2 {|c| [c.reqid, true, c.hostlist] }


## Datanodes and Heartbeats

### [[Datanode]](https://github.com/bloom-lang/bud-sandbox/blob/master/bfs/datanode.rb)

A datanode runs both Bud code (to support the heartbeat and control protocols) and pure Ruby (to support the data transfer protocol).  A datanode's main job is keeping the master 
aware of it existence and its state, and participating when necessary in data pipelines to read or write chunk data to and from its local storage.

    module BFSDatanode
      include HeartbeatAgent
      include StaticMembership
      include TimestepNonce
      include BFSHBProtocol

By mixing in HeartbeatAgent, the datanode includes the machinery necessary to regularly send status messages to the master.  __HeartbeatAgent__ provides an input interface
called __payload__ that allows an agent to optionally include additional information in heartbeat messages: in our case, we wish to include state deltas which ensure that
the master has an accurate view of the set of chunks owned by the datanode.  

When a datanode is constructed, it takes a port at which the embedded data protocol server will listen, and starts the server in the background:

        @dp_server = DataProtocolServer.new(dataport)
        return_address <+ [["localhost:#{dataport}"]]

At regular intervals, a datanode polls its local chunk directory (which is independently written to by the data protocol):

        dir_contents <= hb_timer.flat_map do |t|
          dir = Dir.new("#{DATADIR}/#{@data_port}")
          files = dir.to_a.map{|d| d.to_i unless d =~ /^\./}.uniq!
          dir.close
          files.map {|f| [f, Time.parse(t.val).to_f]}
        end

We update the payload that we send to the master if our recent poll found files that we don't believe the master knows about:


        to_payload <= (dir_contents * nonce).pairs do |c, n|
          unless server_knows.map{|s| s.file}.include? c.file
            #puts "BCAST #{c.file}; server doesn't know" or [n.ident, c.file, c.time]
            [n.ident, c.file, c.time]
          else
            #puts "server knows about #{server_knows.length} files"
          end
        end

Our view of what the master ``knows'' about reflects our local cache of acknowledgement messages from the master.  This logic is defined in __HBMaster__.

### [Heartbeat master logic](https://github.com/bloom-lang/bud-sandbox/blob/master/bfs/hb_master.rb)

On the master side of heartbeats, we always send an ack when we get a heartbeat, so that the datanode doesn't need to keep resending its
payload of local chunks:

        hb_ack <~ last_heartbeat.map do |l|
          [l.sender, l.payload[0]] unless l.payload[1] == [nil]
        end

At the same time, we use the Ruby _flat_map_ method to flatten the array of chunks in the heartbeat payload into a set of tuples, which we
associate with the heartbeating datanode and the time of receipt in __chunk_cache__:

        chunk_cache <= (master_duty_cycle * last_heartbeat).flat_map do |d, l| 
          unless l.payload[1].nil?
            l.payload[1].map do |pay|
              [l.peer, pay, Time.parse(d.val).to_f]
            end 
          end
        end

We periodically garbage-collect this cache, removing entries for datanodes from whom we have not received a heartbeat in a configurable amount of time.
__last_heartbeat__ is an output interface provided by the __HeartbeatAgent__ module, and contains the most recent, non-stale heartbeat contents:

        chunk_cache <-(master_duty_cycle * chunk_cache).pairs do |t, c|
          c unless last_heartbeat.map{|h| h.peer}.include? c.node
        end

## [BFS Client](https://github.com/bloom-lang/bud-sandbox/blob/master/bfs/bfs_client.rb)

One of the most complicated parts of the basic GFS design is the client component.  To minimize load on the centralized master, we take it off the critical 
path of file transfers.  The client therefore needs to pick up this work.

We won't spend too much time on the details of the client code, as it is nearly all _plain old Ruby_.  The basic idea is:

 1. Pure metadata operations 
     * _mkdir_, _create_, _ls_, _rm_
     * Send the request to the master and inform the caller of the status.
     * If _ls_, return the directory listing to the caller.
 2. Append
     * Send a __fsaddchunk__ request to the master, which should return a new chunkid and a list of datanodes.
     * Read a chunk worth of data from the input stream.  
     * Connect to the first datanode in the list.  Send a header containing the chunkid and the remaining datanodes.
     * Stream the file contents.  The target datanode will then ``play client'' and continue the pipeline to the next datanode, and so on.
 2. Read
     * Send a __getchunks__ request to the master for the given file.  It should return the list of chunks owned by the file.
     * For each chunk,
         * Send a __fschunklocations__ request to the master, which should return a list of datanodes in possession of the chunk (returning a list allows the client to perform retries without more communication with the master, should some of the datanodes fail).
         * Connect to a datanode from the list and stream the chunk to a local buffer.
     * As chunks become available, stream them to the caller.


## [Data transfer protocol](https://github.com/bloom-lang/bud-sandbox/blob/master/bfs/data_protocol.rb)

The data transfer protocol comprises a set of support functions for the bulk data transfer protocol whose use is described in the previous section.
Because it is _plain old Ruby_ it is not as interesting as the other modules. It provides:

  * The TCP server code that runs at each datanode, which parses headers and writes stream data to the local FS (these files are later detected by the directory poll).
  * Client API calls to connect to datanodes and stream data.  Datanodes also use this protocol to pipeline chunks to downstream datanodes.
  * Master API code invoked by a background process to replicate chunks from datanodes to other datanodes, when the replication factor for a chunk is too low.

## [Master background process](https://github.com/bloom-lang/bud-sandbox/blob/master/bfs/background.rb)

So far, we have implemented the BFS master as a strictly reactive system: when clients make requests, it queries and possibly updates local state.
To maintain the durability requirement that `REP_FACTOR` copies of every chunk are stored on distinct nodes, the master must be an active system
that maintains a near-consistent view of global state, and takes steps to correct violated requirements.  

__chunk_cache__ is the master's view of datanode state, maintained as described by collecting and pruning heartbeat messages.  
After defining some helper aggregates (__chunk_cnts_chunk__ or replica count by chunk, and __chunk_cnt_host__ or datanode fill factor), 

        cc_demand <= (bg_timer * chunk_cache_alive).rights
        cc_demand <= (bg_timer * last_heartbeat).pairs {|b, h| [h.peer, nil, nil]}
        chunk_cnts_chunk <= cc_demand.group([cc_demand.chunkid], count(cc_demand.node))
        chunk_cnts_host <= cc_demand.group([cc_demand.node], count(cc_demand.chunkid))

we define __lowchunks__ as the set of chunks whose replication factor is too low:

        lowchunks <= chunk_cnts_chunk { |c| [c.chunkid] if c.replicas < REP_FACTOR and !c.chunkid.nil?}

We define __chosen_dest__ for a given underreplicated chunk as the datanode with
the lowest fill factor that is not already in possession of the chunk:

        sources <= (cc_demand * lowchunks).pairs(:chunkid => :chunkid) {|a, b| [a.chunkid, a.node]}
        # nodes not in possession of such chunks, and their fill factor
        candidate_nodes <= (chunk_cnts_host * lowchunks).pairs do |c, p|
          unless chunk_cache_alive.map{|a| a.node if a.chunkid == p.chunkid}.include? c.host
            [p.chunkid, c.host, c.chunks]
          end
        end
    
        best_dest <= candidate_nodes.argagg(:min, [candidate_nodes.chunkid], candidate_nodes.chunks)
        chosen_dest <= best_dest.group([best_dest.chunkid], choose(best_dest.host))

and the __best_src__ as any arbitrary node that is in possession of the chunk:

        best_src <= sources.group([sources.chunkid], choose(sources.host))

Finally, we initiate a background replication job for a given chunk by inserting
the chosen chunkid, source and destination into __copy_chunk__

        copy_chunk <= (chosen_dest * best_src).pairs(:chunkid => :chunkid) do |d, s|
          [d.chunkid, s.host, d.host]
        end



