require 'rubygems'
require 'bud'
require 'cidr/reliable_delivery'

#class BudKVS < BestEffortDelivery
class BudKVS < ReliableDelivery
  def state
    super
    table :bigtable, ['key'], ['value']
    table :stor_saved, ['client', 'server', 'key', 'reqid', 'value']
    table :member, ['peer']

    channel :kvstore_snd, 0, ['server', 'client', 'key', 'reqid'], ['value']

    scratch :kvstore, ['server', 'client', 'key', 'reqid'], ['value']
    scratch :kvdelete, ['server', 'client', 'key', 'reqid']
  end

  declare
    def kstore
      readback = join [stor_saved, pipe_out], [stor_saved.reqid, pipe_out.id]
      stor_saved <- readback.map{|s, p| s}
      stor_saved <+ kvstore.map{|k| k}
      bigtable <+ readback.map do |s, p| 
        [s.key, s.value] 
      end

      jst = join [bigtable, stor_saved, pipe_out], [bigtable.key, stor_saved.key], [stor_saved.reqid, pipe_out.id]
      bigtable <- jst.map { |b, s, p| b }
    end

  declare 
    def kclean
      #jde = join [kvdelete, bigtable], [kvdelete.key, bigtable.key]
      #bigtable <- jde.map{|d, b| b}
    end

  declare
    def resp
      #jresp = join [@delivery.pipe_out, 
      #resp_saved <+ 
      #jft = join [kvfetch, bigtable], [kvfetch.key, bigtable.key]
      #response <+ jft.map{|f, b| [f.client, f.server, b.key, b.value]}
      #resp_saved <= response.map{|r| r}
    end

  declare
    def replicate
      jrep = join [kvstore, member]
      pipe <+ jrep.map do |s, m|
        if m.peer != @addy and m.peer != s.client
          [m.peer, @addy, s.reqid, [s.key, s.value]]
        end
      end
  
      kvstore <+ pipe_chan.map do |p|
        if @addy == p.peer and p.peer != p.self
          [p.peer, p.self, p.payload[0], p.id, p.payload[1]] 
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
