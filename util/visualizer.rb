require 'rubygems'
require 'tokyocabinet'
require 'bud'
include TokyoCabinet

BUD_TC_DIR = "#{ARGV[0]}/bud_"

class VizHelper < Bud
  def state
    table :full_info, [:bud_time, :table, :row]
    scratch :cardinalities, [:bud_time, :table] => [:cnt]
    scratch :times, [:bud_time]
  end

  def initialize(strata, tabinf, cycle, depends, dir)
    @t_strata = strata
    @t_tabinf = tabinf
    @t_cycle = cycle
    @t_depends = depends
    @dir = dir
    super()
  end

  declare 
  def counting
    cardinalities <= full_info.group([full_info.bud_time, full_info.table], count)
    times <= full_info.map{|f| [f.bud_time]}
  end

  def summarize(dir)
    table_io = {}
    times.each do |time|
      write_html(dir, time.bud_time)
    end
    cardinalities.each do |card|
      puts "CARD: #{card.inspect}"
      table_io["#{card.table}_#{card.bud_time}"] = start_table(dir, card.table, card.bud_time)
    end
    full_info.each do |info|
      puts "write out #{info.table} at #{info.bud_time}: #{info.row}"
      write_table_content(table_io["#{info.table}_#{info.bud_time}"], info.row)
    end

    table_io.each_value do |tab|
      end_table(tab)
    end

    # fix: nested loops
    times.sort.each do |time|
      card_info = {}
      cardinalities.each do |card|
        if card.bud_time == time.bud_time
          puts "info #{card.table} is #{card.cnt}"
          card_info[card.table] = card.cnt
        end
      end
      
      @t_strata.each{|s| puts "strata: #{s.inspect}" } 
      gv = GraphGen.new(@t_strata, @t_tabinf, @t_cycle, "#{@dir}/tm_#{time.bud_time}", time.bud_time, 3, @dir, false, nil, card_info)
      gv.process(@t_depends)
      gv.finish
        
    end
    
  end

  def write_html(dir, tm)
    puts "tm is #{tm} as #{tm.class}"
    nm = "tm_#{tm}"
    prev = "tm_#{tm-1}"
    nxt = "tm_#{tm+1}"
    fout = File.new("#{dir}/#{nm}.html", "w")
    ##fout.puts "<center><h1>#{@bud_instance.class} #{time_node_header()}</h1><center>"
    fout.puts "<embed src=\"#{ENV['PWD']}/#{dir}/#{nm}_expanded.svg\" width=\"100%\" height=\"75%\" type=\"image/svg+xml\" pluginspage=\"http://www.adobe.com/svg/viewer/install/\" />"
    fout.puts "<hr><h2><a href=\"#{ENV['PWD']}/#{dir}/#{prev}.html\">prev</a>"
    fout.puts "<a href=\"#{ENV['PWD']}/#{dir}/#{nxt}.html\">next</a>"
    fout.close
  end

  def start_table(dir, tab, time)
    fout = File.new("#{dir}/#{tab}_#{time}.html", "w")
    #fout.puts "<h1>#{tab} #{time_node_header()}</h1>"
    fout.puts "<table border=1>"
    #fout.puts "<tr>" + data.schema.map{|s| "<th> #{s} </th>"}.join(" ") + "<tr>"
    return fout
  end
  
  def end_table(stream)
    stream.puts "</table>"
    stream.close
  end

  def write_table_content(stream, row)
    stream.puts "<tr>"
    stream.puts row.map{|c| "<td>#{c.to_s}</td>"}.join(" ")
    stream.puts "</tr>"
  end
end

def deserialize_table(tab, strict)
  # oy.  meta only
  ret = []
  puts "DS #{tab}"
  tab.each_pair do |k, v|
    key = Marshal.load(k)
    time = key.shift
    raise "non-zero budtime.  sure this is metadata?" if time != 0 and strict
    #ret[key] = Marshal.load(v)
    tup = key
    Marshal.load(v).each{|v| tup << v }
    ret << tup
  end
  return ret
end

@tables = {}

Dir.new(BUD_TC_DIR).entries.each do |file|
  next if file =~ /^\./
  puts "FILE is #{file}"
  hdb = TokyoCabinet::HDB.new
  ret = hdb.open("#{BUD_TC_DIR}/#{file}", HDB::OREADER)
  raise "db not found" unless ret
  @tables[file] = hdb
end


# let's try to do a visualization
strata = deserialize_table(@tables['t_stratum_log.tch'], true)
tabinf = deserialize_table(@tables['t_table_info_log.tch'], true)
puts "try cyc"
cycle = deserialize_table(@tables['t_cycle_log.tch'], true)
depends = deserialize_table(@tables['t_depends_log.tch'], true)

gv = GraphGen.new(strata, tabinf, cycle, "OUTPUT", -1, 1, "plotter_out")
gv.process(depends)
gv.finish


strata.each{|s| puts "STRAT: #{s.inspect}" } 
vh = VizHelper.new(strata, tabinf, cycle, depends, ARGV[0])

@tables.each_pair do |name, contents|
  name = name.gsub("_log.tch", "")
  puts "TAB #{name}"
  contents.each_pair do |k, v|
    key = Marshal.load(k)
    time = key[0]
    row = key
    Marshal.load(v).each{|val| row << val } 
    vh.full_info << [time, name, row]
  end
end

vh.tick
vh.summarize(ARGV[0])
