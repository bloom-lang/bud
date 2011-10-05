require 'test_common'

# In "quick mode", don't bother running some of the more expensive tests
if ARGV.first and ARGV.first.downcase == "quick"
  $quick_mode = true
end

require 'tc_aggs'
require 'tc_attr_rewrite'
require 'tc_bust'
require 'tc_callback'
require 'tc_channel'
require 'tc_collections'
require 'tc_dbm'
require 'tc_delta'
require 'tc_errors'
require 'tc_execmodes' unless $quick_mode
require 'tc_exists'
require 'tc_forkdeploy' unless $quick_mode
require 'tc_halt'
require 'tc_inheritance'
require 'tc_interface'
require 'tc_joins'
require 'tc_mapvariants'
require 'tc_meta'
require 'tc_metrics'
require 'tc_module'
require 'tc_nest'
require 'tc_rebl'
require 'tc_schemafree'
require 'tc_semistructured'
require 'tc_temp'
require 'tc_terminal'
require 'tc_threaddeploy' unless $quick_mode
require 'tc_timer'
require 'tc_wc'
require 'tc_with'

if defined? Bud::HAVE_TOKYOCABINET
  require 'tc_tc'
end
