# XXX: still not catching some exceptions
# XXX: parsing is horrible
# XXX: catch signals
require 'readline'
require 'bud'

# set up everything
bud_class = Class.new(Bud)
bud_class_instance = bud_class.new('localhost', 0, 'enforce_rewrite' => true)

# 3 wasted lines on an accessor
def bud_class_instance.add_declaration(method_sym)
  @declarations << method_sym
end

bud_class_instance.instance_eval("def print_state\nend")
bud_class_instance.instance_eval("def state\nend")

table_names = []
state_exprs = []

i = 0

loop do
  line = Readline::readline('> ').lstrip
  Readline::HISTORY.push(line)

  if line[0..0] == "/" then
    if line[1..4] == "tick" then
      bud_class_instance.tick
      bud_class_instance.print_state
    end
    next
  end

  # XXX: expand to other types of tables
  if line[0..5] == "table " then
    begin
      bud_class_instance.instance_eval(line)
    rescue Exception => exc
      puts "Uh oh, an error while trying to create a table: (#{$!})\n"
      next
    end

    state_exprs << line

    # 100% hacky parsing
    table_name = line.split(" ")[1]
    table_name.slice!(0)
    table_name = table_name.chop
    table_names << table_name

    # remove state & print_state
    class << bud_class_instance
      remove_method(:print_state)
      remove_method(:state)
    end
    
    # add them back with new state
    def_state = "def state\n"
    state_exprs.each {|s| def_state += s + "\n"}
    bud_class_instance.instance_eval(def_state + "end")

    def_print_state = "def print_state\n"
    table_names.each{|t| def_print_state += "puts \"" + t + ":\n\"\n" + t + ".each {|t| puts t.inspect}\n"}
    bud_class_instance.instance_eval(def_print_state + "end")

  else
    bud_class_instance.instance_eval("def rule" + i.to_s + "\n\t" + line + "\nend")
    bud_class_instance.add_declaration(("rule" + i.to_s).to_sym)
    i += 1
    begin
      bud_class_instance.safe_rewrite
    rescue
      puts "Uh oh, an error while trying to add a rule: (#{$!})\n"
      # XXX: not sure if this is necessary
      # revert changes:   
      class << bud_class
        remove_method(rule + i.to_s)
      end
    end
  end

end
