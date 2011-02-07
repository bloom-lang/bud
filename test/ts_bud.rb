require 'rubygems'
require 'bud'
require 'test/unit'

require 'tc_aggs'
require 'tc_channel'
require 'tc_collections'
# require 'tc_conflicts'
require 'tc_delta'
# require 'tc_evaluation'
require 'tc_inheritance'
require 'tc_joins'
require 'tc_meta'
require 'tc_nest'
require 'tc_timer'
require 'tc_vars'
require 'tc_wc1'
# require 'tc_wc2'

if defined? Bud::HAVE_TOKYO_CABINET
  puts "Running TC tests"
  require 'tc_tc'
else
  puts "Skipping TC tests"
end
