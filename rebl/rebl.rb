# XXX: still not catching some exceptions
# XXX: parsing is horrible
# XXX: catch signals
# XXX: separate tick from print
# XXX: allow rules or oneshot queries to be added "at the current time"
require 'readline'
require 'bud'

# set up everything
bud_class = Class.new(Bud)
bud_class_instance = bud_class.new('localhost', 0, 'enforce_rewrite' => true)
bud_class_instance.instance_eval("@declarations << :rules")

def bud_class_instance.safe_instance_eval(str)
  begin
    self.instance_eval(str)
  rescue Exception => exc
    puts "#{$!}"
    return false
  end
  return true
end

bud_class_instance.instance_eval("def print_state\nend")
bud_class_instance.instance_eval("def state\nend")

table_names = []
state_exprs = []
rules = []

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
    next if not bud_class_instance.safe_instance_eval(line)
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
    next if not bud_class_instance.safe_instance_eval("def state\n" + state_exprs.join("\n") + "\nend")

    def_print_state = "def print_state\n"
    table_names.each{|t| def_print_state += "puts \"" + t + ":\n\"\n" + t + ".each {|t| puts t.inspect}\n"}
    next if not bud_class_instance.safe_instance_eval(def_print_state + "end")

  else

    # do a dry run of inserting the block with the new rule, and a safe_rewrite
    rules << line
    if bud_class_instance.safe_instance_eval("def rules\n\t" + rules.join("\n") + "\nend")
      begin
        # even if our code passes the dry run, it might still fail on tick
        bud_class_instance.safe_rewrite
      rescue Exception => exc
        puts "#{$!}"
        class << bud_class
          remove_method(:rules)
        end
        # revert
        rules.pop()
        bud_class_instance.safe_instance_eval("def rules\n\t" + rules.join("\n") + "\nend")
        next
      end
    else
      rules.pop()
      next
    end

  end
end
