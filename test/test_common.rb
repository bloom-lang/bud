# Prefer Bud from local source tree to any version in RubyGems
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")
$:.unshift "."
require 'bud'

require 'rubygems'
gem 'minitest'  # Use the rubygems version of MT, not builtin (if on 1.9)
require 'minitest/autorun'
