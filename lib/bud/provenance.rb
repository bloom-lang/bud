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
    end
  end

  def prov_agg(x)
    [ProvConcatenate.new, x]
  end


  # the scalar UDF for this particular provenance implementation
  def prov_cat(rule, *args)
    return "r#{rule}[#{@budtime}](" + args.join(": ") + ")"
    args.each do |sg|
      d.append(sg)
    end
    return d
  end


  # pretty printing stuff
  def tabs(cnt)
    str = ""
    (0..cnt).each do |i|
      str = str + "  "
    end
    return str
  end 

  def eat_outer_parens(str)
    ret =  str.sub(/^\s*\(/, "").sub(/\)\s*\z/, "")
    #print "from str #{str} I got ret #{ret}\n"
    return ret
  end

  def whence_p(data, lvl)
    # string stuff for now...
    if data =~ /^\s*([a-zA-Z0-9]+)\[(\d*)\](\(.+)/
      rule = $1; tm = $2; rem = $3
      if rule == "agg"
        rem.split(";").each do |spl|
          print tabs(lvl) + "AGG contrib: (#{spl.class}) : #{spl.inspect}\n"
          rest = eat_outer_parens(spl)
          whence_p(rest, lvl + 1)
        end
      else
        items = rem.split(":")
        first = items.shift
        rest = eat_outer_parens(items.join(":"))
        print tabs(lvl) + "rule #{rule} at time #{tm} applied rest #{first} \n"
        #print "call whence on remainder *#{rest}*\n"
        whence_p(rest, lvl + 1)
      end
    else 
      #print "no match(#{lvl}) #{data}\n"
    end 
  end

  def whence(datum)
    print "WHENCE(#{datum.inspect}) ?\n\n"
    prov = datum[datum.length - 1]
    whence_p(prov, 0)
  end

end


