require 'rubygems'
require 'bud'

class DepAnalysis < Bud
  include Anise
  annotator :declare  

  def state
    table :providing, ['pred', 'input']
    table :demanding, ['pred', 'input']
    table :depends_tc, ['head', 'body', 'via', 'neg', 'temporal']
    table :cycle, ['predicate', 'via', 'neg', 'temporal']
    table :closed,  ['predicate']
    table :underspecified, ['pred', 'other', 'kind']
    scratch :pairing, ['in', 'out']
    table :connected, ['in', 'out']

    table :source, ['pred']
    table :sink, ['pred']
  end

  declare
  def process
    closed <= join([providing, demanding], [providing.pred, demanding.pred]).map do |p, d|
      [p.pred] if p.input != d.input 
    end

    underspecified <= demanding.map do |d|
      unless closed.include? [d.pred]
        puts "UNDERSPEC'd " + d.pred or [d.pred, nil, "demanded tables not implemented"]
      end
    end

    pairing <= join([providing, providing]).map do |p1, p2|
      if p1.input and !p2.input
        [p1.pred, p2.pred]
      end
    end

    connected <= join([pairing, depends_tc], [pairing.in, depends_tc.body], [pairing.out, depends_tc.head]).map do |p, d|
      puts "CONNECTED: " + p.inspect or p
    end

    underspecified <= pairing.map do |p|
      #unless connected.include? p
      unless connected.map{|c| c.in}.include? p.in or connected.map{|c| c.in}.include? p.out
        [p.in, p.out, "unconnected dataflow"]
      end
    end  

    source <= providing.map do |p|
      if p.input and !depends_tc.map{|d| d.head}.include? p.pred
        [p.pred]
      end
    end

    sink <= providing.map do |p|
      if !p.input and !depends_tc.map{|d| d.body}.include? p.pred
        puts "SINK: " + p.inspect or [p.pred]
      end
    end


  end
  
end

