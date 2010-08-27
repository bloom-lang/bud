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
    channel :vote, 0, ['otherloc', 'myloc', 'ballot', 'vote']  
    table :vote_ledger, ['otherloc', 'myloc', 'ballot', 'vote']  
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
      vote_ledger <= vote.map{|v| v}

      # subclass provides some code that inserts into vote...
    }

  end
end

