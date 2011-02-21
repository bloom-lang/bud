require 'rubygems'
require 'bud'
require 'deployer'

class GenericBud
  include Bud
end

class MetaRecv
  include Bud
  include Deployer
end

source = ARGV[0].split(':')
ip = source[0]
port = source[1]
program = MetaRecv.new(:enforce_rewrite => true, :ip => ip, :port => port)
program.run
