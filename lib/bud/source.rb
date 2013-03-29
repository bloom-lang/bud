require 'rubygems'

module Source
  $cached_file_info = Struct.new(:curr_file, :lines, :last_state_bloom_line).new

  # Reads the block corresponding to the location (string of the form
  # "file:line_num").  Returns an ast for the block.
  def Source.read_block(location)
    if location.start_with? '('
      raise Bud::IllegalSourceError, "source must be present in a file; cannot read interactive shell or eval block"
    end
    location =~ /^(.*):(\d+)/
    filename, num = $1, $2.to_i
    if filename.nil?
      raise Bud::IllegalSourceError, "couldn't determine filename from backtrace"
    end
    lines = cache(filename, num)
    # Note: num is 1-based.

    # for_current_ruby might object if the current Ruby version is not supported
    # by RubyParser; bravely try to continue on regardless
    parser = RubyParser.for_current_ruby rescue RubyParser.new
    stmt = ""       # collection of lines that form one complete Ruby statement
    ast = nil
    lines[num .. -1].each do |l|
      next if l =~ /^\s*#/
      if l =~ /^\s*([}]|end)/
        # We found some syntax that looks like it might terminate the Ruby
        # statement. Hence, try to parse it; if we don't find a syntax error,
        # we're done.
        begin
          ast = parser.parse stmt
          break
        rescue
          ast = nil
        end
      end
      stmt += l + "\n"
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
