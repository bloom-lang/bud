require 'rubygems'
require 'bud'

require 'examples/simple_broadcast'

class Vote < SimpleBroadcast
  #def initialize(ip, port)
  #  super ip, port
  #end

  def state
    # ideally this would 'just happen' as part of inheritance...
    super
    scratch :ballot, ['ballot']
    scratch :vote, ['ballot', 'vote']
    channel :vote_snd, 0, ['otherloc', 'myloc', 'ballot', 'vote']  
    table :vote_ledger, ['otherloc', 'myloc', 'ballot', 'vote'] 
    table :deliver_ledger, ['otherloc', 'ballot'] 
    scratch :mcnt, ['master'], ['cnt']
    scratch :vcnt, ['ballot', 'vote', 'cnt']
    scratch :status, ['ballot', 'status']    
  end

  def declaration
    super
    strata[1] = rules {
      mcnt <= members.group([members.myloc], count())
      vcnt <= vote_ledger.group([vote_ledger.ballot, vote_ledger.vote], count())
      message <+ ballot.map{|b| b}
      vote_ledger <= vote_snd.map{|v| print "mapping snd: #{v}\n"; v}

      # subclass provides some code that inserts into vote...
      deliver_ledger <= deliver.map{|d| [d.otherloc, d.message]}
      #deliver_ledger <= deliver.map{|d| [d.myloc, d.message]}

      vote_snd <+ join([vote, deliver_ledger]).map do |v, l|
        print "POSSIBLE VOTE (#{v.ballot} vs #{l.ballot} from #{@myloc}) with #{v.vote} to #{l.otherloc}\n"
        [l.otherloc, @myloc, l.ballot, v.vote] if v.ballot == l.ballot
      end
    }

  end
end

