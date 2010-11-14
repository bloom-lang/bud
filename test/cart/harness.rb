require 'rubygems' 
require 'bud'
require '../../lib/bud/bud_meta'

# args: N, files_n, classes_m

def work
  ic = Bud.new('localhost', 56789)
  strat = ic.stratify(@shreddies)
  print "strat is #{strat.class}, #{strat.inspect}\n"
  ic.visualize(strat, "outp")
end

def instant(cls)
  print "try port #{@port}\n\n"
  sleep 1
  d = eval ("class FooBar < Bud\ninclude #{cls}\nend\n FooBar.new('localhost', #{@port})")
  d.shredded_rules.each {|d| @shreddies << d }
  @port = @port + 1
end

@shreddies = []
@port = 12345
files = []
classes = []
elems = ARGV[0].to_i
(1..ARGV.length-1).each do |i|
  if i <= elems
    print "DO #{ARGV[i]}\n"
    eval ( "require \"#{ARGV[i]}\"")
  else
    print "Work on #{ARGV[i]}\n"
    instant(ARGV[i])
    print "OK\n"
  end
end

work

