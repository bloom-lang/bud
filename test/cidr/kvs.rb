require 'rubygems'
require 'bud'

class BudKVS < Bud

  def state
    table :bigtable, ['key'], ['value']
    table :resp_saved, ['client', 'server', 'key', 'value']
    table :member, ['peer']

    channel :kvstore, 0, ['server', 'key'], ['value']
    channel :kvdelete, 0, ['server', 'key']
    channel :kvfetch, 0, ['server', 'client', 'key']
    channel :response, 0, ['client', 'server', 'key', 'value']
    channel :tickler, 0, ['server']
  end

  declare
    def kstore
      bigtable <+ kvstore.map{|s| [s.key, s.value]}
      jst = join [bigtable, kvstore], [bigtable.key, kvstore.key]
      bigtable <- jst.map{|b, s| b}
    end

  declare 
    def kclean
      jde = join [kvdelete, bigtable], [kvdelete.key, bigtable.key]
      bigtable <- jde.map{|d, b| b}
    end

  declare
    def resp
      jft = join [kvfetch, bigtable], [kvfetch.key, bigtable.key]
      response <+ jft.map{|f, b| [f.client, f.server, b.key, b.value]}
      resp_saved <= response.map{|r| r}
    end

  declare
    def replicate
      jrep1 = join [kvstore, member]
      kvstore <+ jrep1.map{|s, m| [m.peer, s.key, s.value]}
      jrep2 = join [kvstore, member]
      kvdelete <+ jrep2.map{|d, m| [m.peer, s.key]}
    end


end
