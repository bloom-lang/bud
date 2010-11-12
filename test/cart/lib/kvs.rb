require 'rubygems'
require 'bud'
require 'lib/reliable_delivery'

#class BudKVS < BestEffortDelivery
module BudKVS
  include BestEffortDelivery
  include Anise
  annotator :declare

  def state
    super
    table :bigtable, ['key'], ['value']
    table :stor_saved, ['server','client', 'key', 'reqid'], ['value']
    table :member, ['peer']
    scratch :kvstore, ['server', 'client', 'key', 'reqid'], ['value']
    scratch :kvstore_indirected, ['server', 'client', 'key', 'reqid'], ['value']
    scratch :pipe_indirected, ['server', 'client', 'id'], ['payload']
  end

  declare
    def interpose
      pipe_indirected <= pipe_out.map {|p| p}
    end

  declare 
    def mutate
      stor_saved <= kvstore.map { |k| print @budtime.to_s + " indrSTOR" + k.inspect + "\n" or k }
      readback = join [stor_saved, pipe_indirected], [stor_saved.reqid, pipe_indirected.id]
      stor_saved <- readback.map{ |s, p| print @budtime.to_s + " remove " + s.inspect + "\n" or s }
      bigtable <+ readback.map do |s, p| 
        (print "->BT(" + @port.to_s + ":" + @budtime.to_s + "): " + s.key.to_s + " == " + s.value.to_s + "\n") or [s.key, s.value] 
        #(print "->BT(" + s.key.to_s + " == " + s.value.to_s + "\n") or [s.key, s.value] 
      end

      jst = join [bigtable, stor_saved, pipe_indirected], [bigtable.key, stor_saved.key], [stor_saved.reqid, pipe_indirected.id]
      bigtable <- jst.map { |b, s, p| b }
    end


  declare
    def replicate
      jrep = join [kvstore, member]
      pipe <= jrep.map do |s, m|
        if m.peer != s.client
          print "place on (" + @addy + " to " + m.peer + ") pipe: " + s.inspect + "\n" or [m.peer, @addy, s.reqid, [s.key, s.value]]
        end
      end
  
      kvstore <= pipe_chan.map do |p|
        if @addy == p.dst #and p.dst != p.src
          # FIXME!
          print @port.to_s + " chan in : " + p.inspect + "\n" or [p.dst, p.src, p.payload.fetch(0), p.id, p.payload.fetch(1)] 
          #[p.dst, p.src, p.payload[0], p.id, p.payload[1]] 
        end
      end

      # bootstrap slaves: they are not required to replicate data to the source.
      pipe_out <= jrep.map do |s, m| 
        if s.client == m.peer 
          print "bootstrapping " + s.reqid.to_s + "\n" or [m.peer, @addy, s.reqid, [s.key, s.value]] 
        end
      end
    end
end
