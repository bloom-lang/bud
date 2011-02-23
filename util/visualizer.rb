require 'rubygems'
require 'tokyocabinet'
require 'bud'
include TokyoCabinet

# prototype offline viz

BUD_TC_DIR = "#{ARGV[0]}/bud_"


class VizHelper
  include Bud

  state {
    table :full_info, [:bud_time, :table, :row]
    scratch :cardinalities, [:bud_time, :table] => [:cnt]
    scratch :times, [:bud_time]
  }

  def initialize(strata, tabinf, cycle, depends, rules, dir)
    @t_strata = strata
    @t_tabinf = tabinf
    @t_cycle = cycle
    @t_depends = depends
    @dir = dir
    @t_rules = rules
    super()
  end

  declare
  def counting
    cardinalities <= full_info.group([full_info.bud_time, full_info.table], count)
    times <= full_info.map{|f| [f.bud_time]}
  end

  def summarize(dir, schema)
    puts "SUMMARIZE"
    table_io = {}
    timeseries = {}
    cardinalities.sort{|a, b| a[0] <=> b[0]}.each do |card|
      table_io["#{card.table}_#{card.bud_time}"] = start_table(dir, card.table, card.bud_time, schema[card.table])
      unless timeseries[card.table]
        timeseries[card.table] = []
      end
      timeseries[card.table] << card.cnt
    end
    full_info.each do |info|
      write_table_content(table_io["#{info.table}_#{info.bud_time}"], info.row)
    end

    ts2 = {}
    timeseries.each_pair do |k, v|
      #taburl = gchart.(:size => '100x100', :data => v, :axis_with_labels => 'x, y')
      puts "TS[#{k}] = #{v.inspect}"
      #taburl = gchart.line(:data => v, :axis_with_labels => 'x, y')
      #url = URI::parse(taburl)
      #puts "TABURL = #{taburl}"
      #response = Net::HTTP.get_response(url)
      #fn = "#{@dir}/#{k}_timeseries.png"
      #fout = File.new(fn, "w")
      #fout.write response.body
      #fout.close
      fn = v
      puts "GOT #{fn}"
      #ts2[k] = "#{ENV['PWD']}/#{fn}"
      ts2[k] = v
    end

    sum = GraphGen.new(@t_strata, @t_tabinf, @t_cycle, "#{@dir}/summary", -1, 3, @dir, false, nil, ts2)
    sum.process(@t_depends)
    sum.dump(@t_rules)
    sum.finish

    table_io.each_value do |tab|
      end_table(tab)
    end

    # fix: nested loops
    times.sort.each do |time|
      card_info = {}
      cardinalities.each do |card|
        if card.bud_time == time.bud_time
          card_info[card.table] = card.cnt
        end
      end

      gv = GraphGen.new(@t_strata, @t_tabinf, @t_cycle, "#{@dir}/tm_#{time.bud_time}", time.bud_time, 3, @dir, false, nil, card_info)
      gv.process(@t_depends)
      gv.dump(@t_rules)
      gv.finish

    end

  end

  def start_table(dir, tab, time, schema)
    fout = File.new("#{dir}/#{tab}_#{time}.html", "w")
    #fout.puts "<h1>#{tab} #{time_node_header()}</h1>"
    fout.puts "<html><title>#{tab} @ #{time}</title>"
    fout.puts "<table border=1>"
    fout.puts "<tr>" + schema.map{|s| "<th> #{s} </th>"}.join(" ") + "<tr>"
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
  tab.each_pair do |k, v|
    #key = Marshal.load(k)
    key = MessagePack.unpack(k)
    time = key.shift
    raise "non-zero budtime.  sure this is metadata?" if time != 0 and strict
    tup = key
    #Marshal.load(v).each{|v| tup << v }
    MessagePack.unpack(v).each{|val| tup << val }
    ret << tup
  end
  tab.close
  return ret
end

def usage
  puts "Usage:"
  puts "Running a Bud program with option :visualize => 3 will cause a TC directory TC_dir to be created (Class_ObjectId_Port)"
  puts "> ruby visualize.rb TC_dir"
  puts "This will create a series of svg files in TC_dir, the root of which will be named tm_0_expanded.svg.  Open in a browser.\n"
  puts "e.g."
  puts "> ruby test/tc_carts.rb"
  puts "> ruby visualize.rb BCS_2159661360_"
  puts "> open -a /Applications/Google\ Chrome.app/ BCS_2159661360_/tm_0_expanded.svg"
  exit
end

# begin "main"

usage unless ARGV[0]

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
strata = deserialize_table(@tables['t_stratum_vizlog.tch'], true)
tabinf = deserialize_table(@tables['t_table_info_vizlog.tch'], true)
tabscm = deserialize_table(@tables['t_table_schema_vizlog.tch'], true)
cycle = deserialize_table(@tables['t_cycle_vizlog.tch'], true)
depends = deserialize_table(@tables['t_depends_vizlog.tch'], true)
rules = deserialize_table(@tables['t_rules_vizlog.tch'], true)


schminf = {}
tabscm.each do |ts|
  tab = ts[0].to_s
  unless schminf[tab]
    schminf[tab] = []
  end
  schminf[tab][ts[2]] = ts[1]
end

gv = GraphGen.new(strata, tabinf, cycle, "OUTPUT", -1, 1, "plotter_out")
gv.process(depends)
gv.finish


vh = VizHelper.new(strata, tabinf, cycle, depends, rules, ARGV[0])

@tables.each_pair do |name, contents|
  name = name.gsub("_vizlog.tch", "")
  contents.each_pair do |k, v|
    key = MessagePack.unpack(k)
    time = key[0]
    row = key
    MessagePack.unpack(v).each{ |val| row << val }
    if name == "t_table_info.tch" or name == "t_table_schema.tch"
      vh.full_info << [0, name, row]
    else
      vh.full_info << [time, name, row]
    end
  end
end

vh.tick
vh.summarize(ARGV[0], schminf)
