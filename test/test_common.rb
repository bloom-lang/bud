require 'rubygems'

if ENV["COVERAGE"]
  require 'simplecov'
  SimpleCov.command_name 'minitest'
  SimpleCov.root '../'
  SimpleCov.start
end

# Prefer Bud from local source tree to any version in RubyGems
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")
$:.unshift "."
require 'bud'

gem 'minitest'  # Use the rubygems version of MT, not builtin (if on 1.9)
require 'minitest/autorun'
