# BFS: A distributed filesystem in Bloom

In this document we'll use what we've learned to build a piece of systems software using Bloom.  The libraries that ship with BUD provide many of the building blocks we'll need to create a distributed,
``chunked'' filesystem in the style of the Google Filesystem(GFS):

 * a [key-value store](https://github.com/bloom-lang/bud-sandbox/blob/master/kvs/kvs.rb), 
 * [nonce generation](https://github.com/bloom-lang/bud-sandbox/blob/master/ordering/nonce.rb)
 * a [heartbeat protocol](https://github.com/bloom-lang/bud-sandbox/blob/master/heartbeat/heartbeat.rb)

## High-level architecture

![Alt text](https://github.com/bloom-lang/bud/raw/master/doc/bfs_arch.png)

BFS implements a chunked, distributed filesystem (mostly) in the Bloom
language.  BFS is architecturally based on BOOMFS, which is itself based on
the Google Filesystem (GFS).  As in GFS, a single master node manages
filesystem metadata, while data blocks are replicated and stored on a large
number of storage nodes.  Writing or reading data involves a multi-step
protocol in which clients interact with the master, retrieving metadata and
possibly changing state, then interact with storage nodes to read or write
chunks.  Background jobs running on the master will contact storage nodes to
orchestrate chunk migrations, during which storage nodes communicate with
other storage nodes.  As in BFS, the communication protocols and the data
channel used for communication between clients and datanodes and between
datanodes is written outside Bloom (in Ruby).

## Basic Filesystem

Before we worry about any of the details of distribution, we need to implement the basic filesystem metadata operations: _create_, _remove_, _mkdir_ and _ls_.
There are many choices for how to implement these operations, and it makes sense to keep them separate from the (largely orthogonal) distributed filesystem logic.
That way, it will be possible later to choose a different implementation of the metadata operations without impacting the rest of the system.

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

We already have a library that provides an updateable flat namespace: the key-value store.  We can easily implement the tree structure of a filesystem over a key-value store
in the following way:

 1. keys are paths
 2. directories have arrays containing child entries (base names)
 3. files values are their contents

Note that (3) will cease to apply when we implement chunked storage later.  So we begin our implementation of a KVS-backed metadata system in the following way:


    module KVSFS
      include FSProtocol
      include BasicKVS
      include TimestepNonce

If we wanted to replicate the metadata master, we could consider mixing in a replicated KVS implementation instead of __BasicKVS__ -- but more on that later.

### Directory Listing 

The directory listing operation is very simple:

      bloom :elles do
        kvget <= fsls.map{ |l| [l.reqid, l.path] }
        fsret <= join([kvget_response, fsls], [kvget_response.reqid, fsls.reqid]).map{ |r, i| [r.reqid, true, r.value] }
        fsret <= fsls.map do |l|
          unless kvget_response.map{ |r| r.reqid}.include? l.reqid
            [l.reqid, false, nil]
          end
        end
      end

If we get a __fsls__ request, probe the key-value store for the requested by projecting _reqid_, _path_ from the __fsls__ tuple into __kvget__.  If the given path
is a key, __kvget_response__ will contain a tuple with the same _reqid_, and the join on the second line will succeed.  In this case, we insert the value
associated with that key into __fsret__.  Otherwise, the third rule will fire, inserting a failure tuple into __fsret__.


### Mutation

The logic for file and directory creation and deletion follow a similar logic with regard to the parent directory.  Unlike a directory listing, these operations change
the state of the filesystem.  In general, any state change will invove carrying out two mutating operations to the key-value store atomically:

 1. update the value (child array) associated with the parent directory entry
 2. update the key-value pair associated with the object in question (a file or directory being created or destroyed).


        dir_exists = join [check_parent_exists, kvget_response, nonce], [check_parent_exists.reqid, kvget_response.reqid]
    
        check_is_empty <= join([fsrm, nonce]).map{|m, n| [n.ident, m.reqid, terminate_with_slash(m.path) + m.name] }
        kvget <= check_is_empty.map{|c| [c.reqid, c.name] }
        can_remove <= join([kvget_response, check_is_empty], [kvget_response.reqid, check_is_empty.reqid]).map do |r, c|
          [c.reqid, c.orig_reqid, c.name] if r.value.length == 0
        end
    
        fsret <= dir_exists.map do |c, r, n|
          if c.mtype == :rm
            unless can_remove.map{|can| can.orig_reqid}.include? c.reqid
              [c.reqid, false, "directory #{} not empty"]
            end
          end
        end
    
        # update dir entry
        # note that it is unnecessary to ensure that a file is created before its corresponding
        # directory entry, as both inserts into :kvput below will co-occur in the same timestep.
        kvput <= dir_exists.map do |c, r, n|
          if c.mtype == :rm
            if can_remove.map{|can| can.orig_reqid}.include? c.reqid
              [ip_port, c.path, n.ident, r.value.clone.reject{|item| item == c.name}]
            end
          else
            [ip_port, c.path, n.ident, r.value.clone.push(c.name)]
          end
        end
    
        kvput <= dir_exists.map do |c, r, n|
          case c.mtype
            when :mkdir
              [ip_port, terminate_with_slash(c.path) + c.name, c.reqid, []]
            when :create
              [ip_port, terminate_with_slash(c.path) + c.name, c.reqid, "LEAF"]
          end
        end

Note that we need not take any particular care to ensure that the two inserts into __kvput__ occur together atomically.  Because both statements use the synchronous 
collection operator (<=) we know that they will occur together in the same fixpoint computation or not at all.

Recall that we mixed in __TimestepNonce__, one of the nonce libraries.  While we were able to use the _reqid_ field from the input operation as a unique identifier
for one of our kvs operations, we need a fresh, unique request id for the second kvs operation in the atomic pair described above.  By joining __nonce__, we get
an identifier that is unique to this timestep.


## File Chunking

Now that we have a module providing a basic filesystem, we can extend it to support chunked storage of file contents.  The metadata master will contain, in addition to the KVS
structure for directory information, a relation mapping a set of chunk identifiers to each file

        table :chunk, [:chunkid, :file, :siz]

and relations associating a chunk with a set of datanodes that host a replica of the chunk.  

        table :chunk_cache, [:node, :chunkid, :time]

These latter (defined in __HBMaster__) are soft-state, kept up to data by heartbeat messages from datanodes (described in the next section).

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

        chunk_buffer <= join([fschunklist, kvget_response, chunk], [fschunklist.reqid, kvget_response.reqid], [fschunklist.file, chunk.file]).map{ |l, r, c| [l.reqid, c.chunkid] }
        chunk_buffer2 <= chunk_buffer.group([chunk_buffer.reqid], accum(chunk_buffer.chunkid))
        fsret <= chunk_buffer2.map{ |c| [c.reqid, true, c.chunklist] }

### Add chunk

If it was a __fsaddchunk__ request,  we need to generate a unique id for a new chunk and return a list of target datanodes.  We reuse __TimestepNonce__ to do the former, and join a relation
called __available__ that is exported by __HBMaster__ (described in the next section) for the latter:

        minted_chunk = join([kvget_response, fsaddchunk, available, nonce], [kvget_response.reqid, fsaddchunk.reqid])
        chunk <= minted_chunk.map{ |r, a, v, n| [n.ident, a.file, 0] }
        fsret <= minted_chunk.map{ |r, a, v, n| [r.reqid, true, [n.ident, v.pref_list.slice(0, (REP_FACTOR + 2))]] }
        fsret <= join([kvget_response, fsaddchunk], [kvget_response.reqid, fsaddchunk.reqid]).map do |r, a|
          if available.empty? or available.first.pref_list.length < REP_FACTOR
            [r.reqid, false, "datanode set cannot satisfy REP_FACTOR = #{REP_FACTOR} with [#{available.first.nil? ? "NIL" : available.first.pref_list.inspect}]"]
          end
        end

Finally, it was a __fschunklocations__ request, we have another possible error scenario, because the nodes associated with chunks are a part of our soft state.  Even if the file
exists, it may not be the case that we have fresh information in our cache about what datanodes own a replica of the given chunk:

        fsret <= fschunklocations.map do |l|
          unless chunk_cache.map{|c| c.chunkid}.include? l.chunkid
            [l.reqid, false, "no datanodes found for #{l.chunkid} in cc, now #{chunk_cache.length}"]
          end
        end

Otherwise, __chunk_cache__ has information about the given chunk, which we may return to the client:

        chunkjoin = join [fschunklocations, chunk_cache], [fschunklocations.chunkid, chunk_cache.chunkid]
        host_buffer <= chunkjoin.map{|l, c| [l.reqid, c.node] }
        host_buffer2 <= host_buffer.group([host_buffer.reqid], accum(host_buffer.host))
        fsret <= host_buffer2.map{|c| [c.reqid, true, c.hostlist] }


## Datanodes and Heartbeats

## BFS Client

## Data transfer protocol

## Master background process


### I am autogenerated.  Please do not edit me.
