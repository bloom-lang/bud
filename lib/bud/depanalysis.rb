require 'rubygems'
require 'bud'

class DepAnalysis < Bud
  include Anise
  annotator :declare  

  def state
    table :providing, ['pred', 'input']
    table :depends_tc, ['head', 'body', 'via', 'neg', 'temporal']
    table :underspecified, ['pred', 'other', 'kind']

    #scratch :pairing, ['incol', 'outcol']
    table :pairing, ['incol', 'outcol']

    table :source, ['pred']
    table :sink, ['pred']
  end

  declare
  def process
    pairing <= join([providing, providing]).map do |p1, p2|
      if p1.input and !p2.input
        puts "pair off " + p1.inspect + " and " + p2.inspect
        [p1.pred, p2.pred]
      end
    end
  end

  declare
  def next_s
    source <= providing.map do |p|
      if p.input and !depends_tc.map{|d| d.head}.include? p.pred
        puts "SRC" 
        [p.pred]
      end
    end

    sink <= providing.map do |p|
      if !p.input and !depends_tc.map{|d| d.body}.include? p.pred
        [p.pred]
      end
    end
  end

  declare 
  def otherz
    underspecified <= pairing.map do |p|
      unless depends_tc.map{|d| d.body}.include? p.outcol or depends_tc.map{|d| d.head}.include? p.incol
        puts "UNCONNECTED" or [p.incol, p.outcol, "unconnected dataflow"]
      end
    end  
  end
end

