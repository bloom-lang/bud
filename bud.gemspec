$LOAD_PATH.unshift 'lib'
require 'bud/version'

Gem::Specification.new do |s|
  s.name = "bud"
  s.version = Bud::VERSION
  s.authors = ["Peter Alvaro", "Neil Conway", "Joseph M. Hellerstein", "William R. Marczak", "Sriram Srinivasan"]
  s.email = ["bloomdevs@gmail.com"]
  s.summary = "A prototype Bloom DSL for distributed programming."
  s.homepage = "http://www.bloom-lang.org"
  s.description = "A prototype of the Bloom distributed programming language as a Ruby DSL."
  s.license = "BSD"
  s.has_rdoc = true
  s.required_ruby_version = '>= 1.9.3'
  s.rubyforge_project = 'bloom-lang'

  s.files = Dir['lib/**/*'] + Dir['bin/*'] + Dir['docs/**/*'] + Dir['examples/**/*'] + %w[README.md LICENSE History.txt Rakefile]
  s.executables = %w[rebl budplot budvis budtimelines budlabel]
  s.default_executable = 'rebl'

  s.add_dependency 'backports', '= 3.8.0'
  s.add_dependency 'eventmachine', '= 1.2.5'
  s.add_dependency 'fastercsv', '= 1.5.5'
  s.add_dependency 'getopt', '= 1.4.3'
  s.add_dependency 'msgpack', '= 1.1.0'
  s.add_dependency 'ruby-graphviz', '= 1.2.3'
  s.add_dependency 'ruby2ruby', '= 2.4.0'
  s.add_dependency 'ruby_parser', '= 3.10.1'
  s.add_dependency 'superators19', '= 0.9.3'
  s.add_dependency 'syntax', '= 1.2.2'
  s.add_dependency 'uuid', '= 2.3.8'

  s.add_development_dependency 'minitest', '= 2.5.1'

  # Optional dependencies -- if we can't find these libraries, certain features
  # will be disabled.
  # s.add_dependency 'zookeeper', '>= 1.3.0'
end
