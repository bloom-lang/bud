Gem::Specification.new do |s|
  s.name = "bud"
  s.version = "0.9.7"
  s.authors = ["Peter Alvaro", "Neil Conway", "Joseph M. Hellerstein", "William R. Marczak", "Sriram Srinivasan"]
  s.email = ["bloomdevs@gmail.com"]
  s.summary = "A prototype Bloom DSL for distributed programming."
  s.homepage = "http://www.bloom-lang.org"
  s.description = "A prototype of the Bloom distributed programming language as a Ruby DSL."
  s.license = "BSD"
  s.has_rdoc = true
  s.required_ruby_version = '>= 1.8.7'
  s.rubyforge_project = 'bloom-lang'

  s.files = Dir['lib/**/*'] + Dir['bin/*'] + Dir['docs/**/*'] + Dir['examples/**/*'] + %w[README.md LICENSE History.txt]
  s.executables = %w[rebl budplot budvis budtimelines budlabel]
  s.default_executable = 'rebl'

  s.add_dependency 'eventmachine'
  s.add_dependency 'fastercsv'
  s.add_dependency 'getopt'
  s.add_dependency 'msgpack'
  s.add_dependency 'ruby-graphviz'
  s.add_dependency 'ruby2ruby', '>= 2.0.1'
  s.add_dependency 'ruby_parser', '>= 3.1.0'
  s.add_dependency 'superators19'
  s.add_dependency 'syntax'
  s.add_dependency 'uuid'

  s.add_development_dependency 'minitest'

  # Optional dependencies -- if we can't find these libraries, certain features
  # will be disabled.
  # s.add_dependency 'zookeeper', '>= 1.3.0'
end
