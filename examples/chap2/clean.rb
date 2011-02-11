# simple grep
require 'rubygems'
require 'bud'

class Clean < Bud
  def state
    file_reader :text, 'ulysses.txt'
    file_reader :carlin, 'carlin.txt'
    scratch :matches, [:lineno, :text, :word]
    table :cleaned, [:lineno, :text]
  end
  
  declare
  def example
    matches <= join([text, carlin]).map{|t,c| [t.lineno, t.text, c.text] if t.text.include? c.text}

    cleaned <= text.map do |t|
      t unless matches.map{|m| [m.lineno]}.include? t.lineno
    end
    cleaned <= matches.map do |m|
      [m.lineno, m.text.gsub(Regexp.new(m.word), '^%$*@')]
    end
  end
end

program = Clean.new
program.tick
program.cleaned.sort.each{|t| puts t.text if t.text =~ /\@/}
