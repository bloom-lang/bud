require 'rubygems'
require 'bud'

class DepAnalysis
  include Bud

  state {
    table :providing, [:pred, :input]
    table :depends_tc, [:head, :body, :via, :neg, :temporal]
    table :underspecified, [:pred, :input]

    table :source, [:pred]
    table :sink, [:pred]
  }

  declare
  def next_s
    source <= providing.map do |p|
      if p.input and !depends_tc.map{|d| d.head}.include? p.pred
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
    underspecified <= providing.map do |p|
      if p.input
        unless depends_tc.map{|d| d.body if d.head != d.body}.include? p.pred 
          [p.pred, true]
        end
      else 
        unless depends_tc.map{|d| d.head if d.head != d.body}.include? p.pred 
          [p.pred, false]
        end
      end
    end  
  end
end

