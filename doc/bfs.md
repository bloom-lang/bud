# BFS: A distributed filesystem in Bloom

In this document we'll use what we've learned to build a piece of systems software using Bloom.  
The libraries that ship with BUD provide many of the building blocks we'll need to create a distributed,
``chunked'' filesystem in the style of the Google Filesystem(GFS):
a key-value store([https://github.com/bloom-lang/bud-sandbox/blob/master/kvs/kvs.rb]), nonce generation, and a heartbeat protocol.

[gist: 880009]

<script src="https://gist.github.com/880009.js"> </script>

[<script src="https://gist.github.com/880009.js"> </script>]

[git://gist.github.com/880009.git]

[https://gist.github.com/raw/880009/c0d7f568ccdd4802a702e4cfe47b782645b34dfb/bfs_protos]