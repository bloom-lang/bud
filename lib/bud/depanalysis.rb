require 'rubygems'
require 'bud'

class DepAnalysis #:nodoc: all
  include Bud

  state do

    table :depends_tc, [:lhs, :body]

    table :providing, [:pred, :input]
    table :underspecified, [:pred, :input]
    

    table :source, [:pred]
    table :sink, [:pred]
  end

  bloom :analysis do
    source <= providing do |p|
      if p.input and !depends_tc.map{|d| d.lhs}.include? p.pred
        [p.pred]
      end
    end

    sink <= providing do |p|
      if !p.input and !depends_tc.map{|d| d.body}.include? p.pred
        [p.pred]
      end
    end

    underspecified <= providing do |p|
      if p.input
        unless depends_tc.map{|d| d.body if d.lhs != d.body}.include? p.pred
          [p.pred, true]
        end
      else
        unless depends_tc.map{|d| d.lhs if d.lhs != d.body}.include? p.pred
          [p.pred, false]
        end
      end
    end
  end
end

