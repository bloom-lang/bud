# simple mapreduce
# run "ruby simple_map.rb localhost:127.0.0.1"
require 'rubygems'
require 'bud'
require 'zlib'

class SimpleMapper < Bud

  def initialize(ip, port, mapper)
    super ip, port
    @mapper = mapper
  end
  
  def state
    file_reader :nodelist, 'mr_reducelist.txt'
    file_reader :input, 'ulysses_short.txt'
    scratch     :map_out, ['key', 'uniq', 'value']
    table       :kvs, ['key', 'uniq'], ['value', 'hashed']
    table       :kvs_addrs, ['key', 'uniq'], ['value', 'addr']
    channel     :reducers, 0, ['addr', 'key', 'value']
    scratch     :all, ['key', 'value']
    scratch     :nodecnt, ['cnt']
  end

  def declaration
    strata[0] = rules {
      nodecnt <= nodelist.group([], count)
    }
    
    strata[1] = rules {
#      @mapper.in_table <= input.map{ |t| t }
#      @mapper.tick
#      map_out <= @mapper.out_table.map{ |t| t }
      input.each { |i| @mapper.do_map(map_out, i) }
        
      kvs <= join([map_out, nodecnt]).map do |mo,cnt|
        [mo.key, mo.uniq, mo.value, Zlib.crc32(mo.key) % cnt.cnt]
      end
      
      kvs_addrs <= join([kvs, nodelist], [kvs.hashed, nodelist.lineno]).map do |k, n|
        [k.key, k.uniq, k.value, n.text]
      end
    }
    
    strata[2] = rules {
      reducers <+ kvs_addrs.map{ |t| [t.addr, t.key, t.uniq] }
    }
  end
end

class Splitter < Bud
  def state
    table :in_table, ['lineno'], ['text']
    table :out_table, ['word', 'uniq'], ['cnt']
  end
  
  def declaration
    strata[1] = rules {
      in_table.each do |t|
        t.text.split.each_with_index{ |w,i| out_table << [w, t.lineno.to_s+':'+i.to_s, 1] }
      end
    }
  end

  def do_map(tbl, t)
    t.text.split.each_with_index {|w,i| tbl << [w, t.lineno.to_s+':'+i.to_s, 1]}
  end
end

splitter = Splitter.new('127.0.0.1', 12347)
program = SimpleMapper.new('127.0.0.1', 12345, splitter)
r = Thread.new {program.run}
sleep 10
