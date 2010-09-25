require 'rubygems'
require 'bud'

require 'cidr/reliable_delivery'

class BudKVS < Bud

  def initialize(ip, port)
    @myid = "#{ip}:#{port}"
    #@delivery = BestEffortDelivery.new(ip, port+2)
    #@delivery.tick
    super
  end
  

  def state
    table :bigtable, ['key'], ['value']
    table :resp_saved, ['client', 'server', 'key', 'value']
    table :member, ['peer']
    #table :acked, ['client', 'server', 'key']

    channel :kvstore_snd, 0, ['server', 'client', 'key'], ['value']
    channel :ack, 0, ['client', 'server', 'key']

    scratch :kvstore, ['server', 'client', 'key'], ['value']
    scratch :kvdelete, ['server', 'client', 'key']
    #channel :kvfetch, 0, ['server', 'client', 'key']
    #channel :response, 0, ['client', 'server', 'key', 'value']
    channel :tickler, 0, ['server']
  end

  declare
    def kstore
      # recall the lesson from BFS: build in the interposition point early.
      #readback = join [bigtable, kvstore, @delivery.pipe_out], [bigtable.key, kvstore.key], [bigtable.key, @delivery.pipe_out.id]
      bigtable <+ kvstore.map{|s| print "FOO: #{s.inspect}\n"; [s.key, s.value] if s.server == "#{@ip}:#{@port}"}
      jst = join [bigtable, kvstore], [bigtable.key, kvstore.key]
      bigtable <- jst.map{|b, s| b if s.server == "#{@ip}:#{@port}"}
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
      #jrep = join [kvstore, member]
      #@delivery.pipe <= jrep.map do |s, m|
      #  [m.peer, @addy, s.key, s]
      #end

    end


end
