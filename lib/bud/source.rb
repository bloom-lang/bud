require 'rubygems'
require 'ruby_parser'
require 'bud/errors'

module Source
  $cached_file_info = Struct.new(:curr_file, :lines, :last_state_bloom_line).new

  # Reads the block corresponding to the location (string of the form
  # "file:line_num").  Returns an ast for the block.
  def Source.read_block(location)
    raise Bud::IllegalSourceError, "source must be present in a file; cannot read interactive shell or eval block" if location.start_with? '('
    location =~ /^(.*):(\d+)/
    filename, num = $1, $2.to_i
    raise Bud::IllegalSourceError, "couldn't determine filename from backtrace" if filename.nil?
    lines = cache(filename, num)
    # Note: num is 1-based.

    src_asts = [] # array of SrcAsts to be returned
    ruby_parser = RubyParser.new

    stmt = ""   # collection of lines that form one complete ruby statement
    endok = true #
    ast = nil
    lines[num .. -1].each do |l|
      next if l =~ /^\s*#/
      break if endok and l =~ /^\s*([}]|end)/
      stmt += l + "\n"
      begin
        ast = ruby_parser.parse stmt
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
    retval # array of lines
  end
end
