require 'rubygems'
require 'ruby_parser'
require 'bud/errors'

module Source
  $cached_file_info = Struct.new(:curr_file, :lines, :last_state_bloom_line).new
  $ruby_parser = RubyParser.new

  #Reads the block corresponding to the location (string of the form "file:line_num").
  #Returns an ast for the block
  def Source.read_block(location)
    raise Bud::Error, "Source must be present in a file; cannot read interactive shell or eval block" if location.start_with? '('
    location =~ /^(.*):(\d+)/
    filename, num = $1, $2.to_i
    raise Bud::BudError, "Couldn't determine filename from backtrace" if filename.nil?
    lines = cache(filename, num)
    # Note: num is 1-based.

    src_asts = [] # array of SrcAsts to be returned

    stmt = ""   # collection of lines that form one complete ruby statement
    endok = true #
    ast = nil
    lines[num .. -1].each do |l|
      break if endok and l =~ /^\s*[}]|end/
      stmt += l
      begin
        ast = $ruby_parser.parse stmt
        endok = true
      rescue => ex
        #        puts "Syntax Error on #{l}: #{ex}"
        endok = false
        ast = nil
      end
    end
    ast
  end

  def Source.cache(filename, num)  # returns array of lines
    if $cached_file_info.curr_file == filename
      retval = $cached_file_info.lines
      if $cached_file_info.last_state_bloom_line == num
        # have no use for the cached info any more. reset it.
        $cached_file_info.lines = []
        $cached_file_info.curr_file = ""
        $cached_file_info.last_state_bloom_line = -1
      end
    else
      $cached_file_info.last_state_bloom_line = -1
      $cached_file_info.curr_file = filename
      $cached_file_info.lines = []
      retval = []
      File.open(filename, "r").each_with_index {|line, i|
        retval << line
        if line =~ /^ *(bloom|state)/
          $cached_file_info.last_state_bloom_line = i
        end
      }
      $cached_file_info.lines = retval
    end
    retval # array of lines.
  end

  # Tok is string tokenizer that extracts a substring matching the
  # supplied regex, and internally advances past the matched substring.
  # Leading white space is ignored.
  # tok = Tok.new("foo 123")
  # x = tok =~ /\w+/  # => x == 'foo'
  # y = tok =~ /\d+/  # => y = '123'
  class Tok
    attr_accessor :str, :group
    def initialize(str)
      @str = str
      @group = nil
    end

    # match regex at beginning of string, and advance. Return matched token
    def =~(regex)
      s = @str
      skiplen = 0
      if s =~ /^\s*/
        skiplen = $&.length
        s = s[skiplen .. -1]
      end
      if (s =~ regex) == 0
        # Regexp.last_match is local to this thread and method; squirrel
        # it away for use in tok.[]
        @group = Regexp.last_match
        skiplen += $&.length
        @str = @str[skiplen .. -1]
        return $&
      else
        nil
      end
    end

    # get the nth subgroup match
    # t = Tok.new("a1122b"); t =~ /a(1+)(2+)b/ ; #=> t[0] =  a1122b; t[1] = 11; t[2] = 22
    def [](n)
      @group ? @group[n] : nil
    end
    def pushBack(str)
      @str = str + @str
    end

    def to_s; @str; end
  end
end