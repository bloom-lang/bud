# Note that simple and reliable broadcasts are correct when
# specified in a set-oriented fashion, even if the programmer 
# wasn't thinking about them that way (and was instead imagining
# tuple/packet -at-a-time processing).  Not so for FIFO.

# I cheated and created the "serializer" type, which has a destructive 
# each method and hence must be used carefully.  This was after struggling for 
# some time with implementing a serializer as a Bud program...

require 'rubygems'
require 'bud'

require 'examples/reliable_broadcast'

class FifoBroadcast < ReliableBroadcast
  attr_reader :myloc
  attr_reader :otherloc

  def initialize(ip, port)
    super ip, port
  end

  def state
    super
    scratch :fmessage, ['message']
    #serializer :fs, ['message', 'server', 'id']
    scratch :fs, ['message', 'server', 'id']
    #scratch :fdeliver, ['message', 'server', 'id']
    #scratch :fdeliver, ['message', 'server', 'id']
    serializer :fdeliver, ['id', 'server'], ['message']
    serializer :serial, ['message']
    table :seq, ['id'] 
    table :servseq, ['server'], ['id']
  end


  def declaration
    super
    
    # jacked up: pick a stratum that won't clobber those instantiated by our superclass
    strata[3] = rules {
      seq << [0] if seq.empty?
      serial <= fmessage.map{|f| [f.message]}
      
      # enqueue the messages via reliable broadcast
      j = join [serial, seq]
      rmessage <+ j.map {|f, s| [[f.message, s.id]] }

      seq <- j.map {|f, s| [s.id]}
      seq <+ j.map {|f, s| [s.id + 1]}


      servseq <= rdeliver.map do |r| 
        [r.sender, 0] unless servseq.map{|s| s.server}.include? r.sender
      end

      # now, because we might rdeliver multiple messages at once, we must serialize this table too
      j2 = join [rdeliver, servseq]
      fs <= j2.map do |d, s|
        if d.sender == s.server
          packed = d.message
          #print "comparing #{s.id} to #{packed[1]} in #{d.inspect}\n"
          if s.id == packed[1]
            [packed[0], d.sender, s.id]
          end
        end
      end


      servseq <- fs.map {|f| [f.server, f.id]}
      servseq <+ fs.map {|f| [f.server, f.id + 1]}
      fdeliver <+ fs.map{|f| [f.id, f.server, f.message]}

    }
  end
end

