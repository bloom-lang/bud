require 'rubygems'
require 'bud'

class BudKVS < Bud

  def initialize(ip, port)
    @myid = "#{ip}:#{port}"
    super
  end
  

  def state
    table :bigtable, ['key'], ['value']
    table :resp_saved, ['client', 'server', 'key', 'value']
    table :member, ['peer']
    table :acked, ['client', 'server', 'key']

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
      bigtable <+ kvstore.map{|s| print "FOO: #{s.inspect}\n"; [s.key, s.value] if s.server == "#{@ip}:#{@port}"}
      jst = join [bigtable, kvstore], [bigtable.key, kvstore.key]
      bigtable <- jst.map{|b, s| b if s.server == "#{@ip}:#{@port}"}
    end

  declare 
    def kclean
      jde = join [kvdelete, bigtable], [kvdelete.key, bigtable.key]
      bigtable <- jde.map{|d, b| b}
    end

  declare
    def resp
      #jft = join [kvfetch, bigtable], [kvfetch.key, bigtable.key]
      #response <+ jft.map{|f, b| [f.client, f.server, b.key, b.value]}
      #resp_saved <= response.map{|r| r}
    end

  declare
    def replicate
      jrep1 = join [kvstore, member]
      #kvstore <+ jrep1.map{|s, m| [m.peer, s.key.to_s, Marshal.dump(s.value)] unless m.peer == "#{@ip}:#{@port}"}
      #kvstore <+ jrep1.map{|s, m| [m.peer, s.key.to_s, s.value.clone] unless m.peer == "#{@ip}:#{@port}"}
      kvstore_snd <+ jrep1.map do |s, m| 
          unless m.peer == "#{@ip}:#{@port}" or s.client == m.peer
            [m.peer, "#{@ip}:#{@port}", s.key.to_s, s.value]
          end
      end

      kvstore <= kvstore_snd.map do |s| 
        print "GOT STRSND #{s.inspect}\n"
        if s.server == @myid
          s
        end
      end

      jrep2 = join [kvstore, member]
      #kvdelete <+ jrep2.map{|d, m| [m.peer, s.key]}
    end


end
