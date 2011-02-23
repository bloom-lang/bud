require 'rubygems'
require 'bud'
require 'deployer'

source = ARGV[0].split(':')
ip = source[0]
port = source[1]
program = MetaRecv.new(:ip => ip, :port => port)
program.run
