# Prefer Bud from local source tree to any version in RubyGems
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")

require 'bud'
require 'test/unit'
require 'rubygems'

