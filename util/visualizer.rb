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
    table_io = {}
    cardinalities.each do |card|
      table_io["#{card.table}_#{card.bud_time}"] = start_table(dir, card.table, card.bud_time, schema[card.table])
    end
    full_info.each do |info|
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
    key = Marshal.load(k)
    time = key.shift
    raise "non-zero budtime.  sure this is metadata?" if time != 0 and strict
    tup = key
    Marshal.load(v).each{|v| tup << v }
    ret << tup
  end
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
    key = Marshal.load(k)
    time = key[0]
    row = key
    Marshal.load(v).each{|val| row << val }
    vh.full_info << [time, name, row]
  end
end

vh.tick
vh.summarize(ARGV[0], schminf)
