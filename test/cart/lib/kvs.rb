require 'rubygems'
require 'bud'
require 'lib/reliable_delivery'

class BudKVS < BestEffortDelivery
#class BudKVS < ReliableDelivery
  def state
    super
    table :bigtable, ['key'], ['value']
    #table :stor_saved, ['client', 'server', 'key', 'reqid', 'value']
    table :stor_saved, ['server','client', 'key', 'reqid', 'value']
    table :member, ['peer']
    scratch :kvstore, ['server', 'client', 'key', 'reqid'], ['value']
    scratch :kvstore_indirected, ['server', 'client', 'key', 'reqid'], ['value']
  end

  declare 
    def mutate
      stor_saved <= kvstore.map { |k| k }
      readback = join [stor_saved, pipe_out], [stor_saved.reqid, pipe_out.id]
      stor_saved <- readback.map{ |s, p| s }
      bigtable <+ readback.map do |s, p| 
        #print "->BT(#{@port}:#{@budtime}): #{s.key} == #{s.value}\n"
        [s.key, s.value] 
      end

      jst = join [bigtable, stor_saved, pipe_out], [bigtable.key, stor_saved.key], [stor_saved.reqid, pipe_out.id]
      bigtable <- jst.map { |b, s, p| b }
    end


  declare
    def replicate
      jrep = join [kvstore, member]
      pipe <= jrep.map do |s, m|
        if m.peer != s.client
          #print "place on (#{m.peer}) pipe: #{s.inspect}\n"
          [m.peer, @addy, s.reqid, [s.key, s.value]]
        end
      end
  
      kvstore <= pipe_chan.map do |p|
        if @addy == p.dst and p.dst != p.src
          # FIXME!
          #[p.dst, p.src, p.payload.index(0), p.id, p.payload.index(1)] 
          #print "RCV! #{@port}:#{@budtime}\n"
          [p.dst, p.src, p.payload[0], p.id, p.payload[1]] 
        end
      end

      # bootstrap slaves: they are not required to replicate data to the source.
      pipe_out <= jrep.map do |s, m| 
        if s.client == m.peer 
          [m.peer, @addy, s.reqid, [s.key, s.value]] 
        end
      end
    end
end
