# Prefer Bud from local source tree to any version in RubyGems
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")
$:.unshift "."

require 'bud'
require 'test/unit'
require 'rubygems'
