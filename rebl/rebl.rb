require 'readline'
require 'rubygems'
require 'bud'

TABLE_TYPES = ["table", "scratch"]
BUILTIN_TABLES = [:stdio, :t_depends, :periodics_tbl, :t_cycle, :localtick,
                  :t_provides, :t_rules, :t_depends_tc, :t_stratum]
HISTFILE = "~/.rebl_history"
MAXHISTSIZE = 100

# set up everything
class ReblClass
  include Bud
end

def reinstantiate
  # new anonymous subclass
  @rebl_class = Class.new(ReblClass)

  if not @rules.empty?
    @rebl_class.class_eval("bloom :rebl_rules do\n" +
                           @rules.sort.map {|_,r| r}.join("\n") + "\nend")
  end

  if not @state.empty?
    @rebl_class.class_eval("state do\n" + @state.values.join("\n") + "\nend")
  end

  # instantiate it
  @old_inst = @rebl_class_inst
  @rebl_class_inst = @rebl_class.new

  # copy the tables over
  if @old_inst
    @rebl_class_inst.tables.merge!(@old_inst.tables.reject do |k,v|
                                     BUILTIN_TABLES.include? k
                                   end)
  end
end

def setup_history
  # permament history; code lifted from irb
  begin
    histfile = File::expand_path(HISTFILE)
    if File::exists?(histfile)
      lines = IO::readlines(histfile).collect { |line| line.chomp }
      Readline::HISTORY.push(*lines)
    end
    Kernel::at_exit do
      lines = Readline::HISTORY.to_a.reverse.uniq.reverse
      lines = lines[-MAXHISTSIZE, MAXHISTSIZE] if lines.nitems > MAXHISTSIZE
      File::open(histfile, File::WRONLY|File::CREAT|File::TRUNC) { |io| io.puts lines.join("\n") }
    end
  rescue Exception
    puts "Error when configuring permanent history: #{$!}"
  end
end

# main
@rules = {}
@ruleid = 0
@state = {}
@stateid = 0
@new_table = nil
reinstantiate
setup_history

loop do
  begin
    line = Readline::readline('rebl> ').lstrip.rstrip
    Readline::HISTORY.push(line)
    split_line = line.split(" ")

    # command
    if line[0..0] == "/" then
      split_line[0].slice! 0
      case split_line[0]
      when "tick" then @rebl_class_inst.tick
      when "lsrules" then puts @rules.inspect
      when "rmrule"
        @rules.delete(Integer(split_line[1]))
        reinstantiate
      when "lscollections"
        puts @rebl_class_inst.tables.keys.find_all{ |tn| not BUILTIN_TABLES.include? tn}.inspect
      when "dump" then @rebl_class_inst.instance_eval("#{split_line[1]}.dump")
      else puts "invalid command"
      end
      next
    end

    # collection
    if TABLE_TYPES.include? split_line[0]
      @state[@stateid += 1] = line
      begin
        reinstantiate
      rescue Exception
        @state.delete(@stateid)
        raise
      end
    else # assume it's a rule
      @rules[@ruleid += 1] = line
      begin
        reinstantiate
      rescue Exception
        @rules.delete(@ruleid)
        raise
      end
    end
    next

  rescue SystemExit, Interrupt
    abort("\nrebellion quashed")
  rescue Exception
    puts "exception: #{$!}"
  end
end
