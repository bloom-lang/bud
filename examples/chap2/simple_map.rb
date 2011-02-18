# simple mapreduce
# run "ruby simple_map.rb 127.0.0.1:12345 ulysses_short_1.txt"
# run "ruby simple_map.rb 127.0.0.1:12346 ulysses_short_2.txt"
require 'rubygems'
require 'bud'
require 'zlib'

class SimpleMapper
  include Bud

  def initialize(ip, port, file, opts)
    @mapper = mapper
    @file = file
    super opts
  end
  
  state do
    file_reader :nodelist, 'mr_reducelist.txt'
    file_reader :inputs, @file
    scratch     :map_out, [:key, :uniq, :value]
    table       :kvs, [:key, :uniq] => [:value, :hashed]
    table       :kvs_addrs, [:key, :uniq] => [:value, :addr]
    channel     :reducers, [:@addr, :key, :value]
    scratch     :all, [:key, :value]
    scratch     :nodecnt, [:cnt]
  end

  declare
  def rules
    nodecnt <= nodelist.group([], count)

    inputs.each { |i| @mapper.do_map(map_out, i) }
        
    kvs <= join([map_out, nodecnt]).map do |mo,cnt|
      [mo.key, mo.uniq, mo.value, Zlib.crc32(mo.key) % cnt.cnt]
    end
      
    kvs_addrs <= join([kvs, nodelist], [kvs.hashed, nodelist.lineno]).map do |k, n|
      [k.key, k.uniq, k.value, n.text]
    end
    
    reducers <~ kvs_addrs.map{ |t| [t.addr, t.key, t.uniq] }
  end
end

class Splitter
  include Bud

  state do
    table :in_table, [:lineno] => [:text]
    table :out_table, [:word, :uniq] => [:cnt]
  end
  
  declare
  def rules
    in_table.each do |t|
      t.text.split.each_with_index{ |w,i| out_table << [w, t.lineno.to_s+':'+i.to_s, 1] }
    end
  end

  def do_map(tbl, t)
    t.text.split.each_with_index {|w,i| tbl << [w, t.lineno.to_s+':'+i.to_s, 1]}
  end
end

source = ARGV[0].split(':')
ip = source[0]
port = source[1].to_i
file = ARGV[1]
splitter = Splitter.new(:ip => ip, :port => port+10)
program = SimpleMapper.new(file, splitter, :ip => ip, :port => port)
# XXX: update to use run_bg
r = Thread.new {program.run}
sleep 40
