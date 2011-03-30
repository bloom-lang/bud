require 'rubygems'
require 'bud'

class DepAnalysis #:nodoc: all
  include Bud

  state do
    table :providing, [:pred, :input]
    table :depends_tc, [:head, :body, :via, :neg, :temporal]
    table :underspecified, [:pred, :input]

    table :source, [:pred]
    table :sink, [:pred]
  end

  def declaration
    strata[0] = lambda {
      source <= providing do |p|
        if p.input and !depends_tc.map{|d| d.head}.include? p.pred
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
          unless depends_tc.map{|d| d.body if d.head != d.body}.include? p.pred
            [p.pred, true]
          end
        else
          unless depends_tc.map{|d| d.head if d.head != d.body}.include? p.pred
            [p.pred, false]
          end
        end
      end
    }
  end
end

