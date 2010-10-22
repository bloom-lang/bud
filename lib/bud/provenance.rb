require 'rubygems'

require 'bud/sane_r2r'
require 'bud/rewrite'
require 'parse_tree'

class Bud
  # stringy provenance implementation: store a derivation tree at tuple granularity.
  # later, pretty-print it.

  # the UDA for this particular provenance implementation
  class ProvConcatenate < Agg
    def init(val)
      [val]
    end
    def trans(state, val)
      retval = state
      retval << val
    end
    def final(state)
      "agg[#{@budtime}](" + state.join(";") + ")"
      ["agg", state]
    end
  end

  def prov_agg(x)
    [ProvConcatenate.new, x]
  end

  def each_p(arr)
    # there is presumably a pretty, ruby-idiomatic way to do this.  but...
    arr.each_with_index do |o, i|
      yield [o, arr[i+1]] if (i % 2) == 0
    end
  end


  # the scalar UDF for this particular provenance implementation
  def prov_cat(rule, *args)
    stms = []
    each_p(args) do |name, tuple|
      print "name=#{name}, tuple = #{tuple.inspect}\n"
      if tuple.prov.nil?
        stms << [name, tuple]
      else
        stms << tuple.prov
      end  
    end
    return ["r#{rule}", stms]
  end

  # pretty printing stuff
  def tabs(cnt)
    str = ""
    (0..cnt).each do |i|
      str = str + "  "
    end
    return str
  end 

  def whence_p(data, lvl)
    # string stuff for now...
    #print "DATA: #{data.inspect}\n"
    #print "OP: #{data[0]}\n"
    op = data[0]
    if op == "agg"
      print tabs(lvl) + "AGGREGATE (\n"
      data[1].each_with_index do |datum, i|
        whence_p(datum, lvl + 1)
      end
      print tabs(lvl) + ") - AGGREGATE\n"
    elsif op =~ /^r(\d+)\z/
      rid = $1
      if @shredded_rules[rid.to_i] and !@printedrules[rid.to_i]
        print tabs(lvl) + "RULE #{rid}: { #{@shredded_rules[rid.to_i][5]} }\n"
        @printedrules[rid.to_i] = true
      else
        print tabs(lvl) + "RULE #{rid} :\n"
      end
      data[1].each do |datum|
        whence_p(datum, lvl + 1)
      end
      print tabs(lvl) + " - RULE #{rid}\n"
    else
      # must be a ground atom: relation name and contents:
      print tabs(lvl) + "#{data[0]}(#{data[1].join(",")})\n"
    end
  end

  def whence(datum)
    @printedrules = {}
    copy = datum.clone
    prov = copy.pop
    print "WHENCE(#{copy.inspect}) ?\n\n"
    whence_p(prov, 0)
  end

end


