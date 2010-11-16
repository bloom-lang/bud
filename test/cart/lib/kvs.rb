require 'rubygems'
require 'bud'
require 'lib/reliable_delivery'

module BudKVS
  include Anise
  annotator :declare

  def state
    super
    table :bigtable, ['key'], ['value']
    table :stor_saved, ['server','client', 'key', 'reqid'], ['value']
    interface input, :kvstore, ['server', 'client', 'key', 'reqid'], ['value']
    interface input, :kvget, ['reqid'], ['key']
    interface output, :kvget_response, ['reqid'], ['key', 'value']
    scratch :can_store, ['ident'], ['payload']
  end

  # a class that mixes in KVS will probably provide
  # an implementation of indir which inserts into can_store
  # otherwise, input and internal actions are directly coupled
  # by the below
  declare 
    def indir
      can_store <= kvstore.map{|k| [k.reqid, nil] }
    end

  declare 
    def mutate
      stor_saved <= kvstore.map { |k| puts "saving" or k }
      readback = join [stor_saved, can_store], [stor_saved.reqid, can_store.ident]
      stor_saved <- readback.map{ |s, p| s }
      bigtable <+ readback.map do |s, p| 
        puts "BT: " + s.inspect or [s.key, s.value]
      end

      jst = join [bigtable, stor_saved, can_store], [bigtable.key, stor_saved.key], [stor_saved.reqid, can_store.ident]
      bigtable <- jst.map { |b, s, p| b }
    end
end


module ReplicatedKVS
  include Anise
  annotator :declare
  include BudKVS
  # Demand MulticastProtocol

  def state
    super
    scratch :rep_can_store, ['ident'], ['payload']
  end

  # jic
  declare
  def indir
    can_store <= rep_can_store.map {|c| c } 
  end
  
  declare
  def local_indir
    # if I am the master, multicast store requests
    send_mcast <= kvstore.map do |k| 
      unless members.include? [k.client]
        puts "MCASt" or [k.reqid, [k.server, @addy, k.key, k.reqid, k.value]] 
      end
    end

    # if I am a replica, store the payload of the multicast
    kvstore <= pipe_chan.map do |d|
      if d.payload.fetch(1) != @addy
        d.payload 
      end
    end

    # I can "write back" to the store if I am a replica,
    rep_can_store <= pipe_chan.map { |c| [c.ident, c.payload] }
    # or if I am the master and the multicast to replicas has succeeded
    rep_can_store <= mcast_done.map {|p| puts "master con store" or p}
  end
end

