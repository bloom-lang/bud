Gem::Specification.new do |s|
  s.name = "bud"
  s.version = "0.0.1"
  s.date = "2010-07-19"
  s.authors = ["Peter Alvaro", "Neil Conway", "Joseph M. Hellerstein", "William R. Marczak"]
  s.email = ["palvaro@cs.berkeley.edu", "nrc@cs.berkeley.edu", "hellerstein@berkeley.edu", "wrm@cs.berkeley.edu"]
  s.summary = "A prototype Bloom DSL for distributed programming."
  s.homepage = "http://www.bloom-lang.org"
  s.description = "A prototype of the Bloom distributed programming language, as a Ruby DSL."
  s.license = "BSD"
  s.has_rdoc = true

  s.files = Dir.glob("lib/**/*") + %w[README LICENSE]
  s.executables = %w[rebl budplot budvis]
  s.default_executable = 'rebl'

  s.add_dependency 'backports'
  s.add_dependency 'eventmachine'
  s.add_dependency 'gchart'
  s.add_dependency 'msgpack'
  s.add_dependency 'ParseTree'
  s.add_dependency 'ruby-graphviz'
  s.add_dependency 'ruby2ruby'
  s.add_dependency 'sexp_path'
  s.add_dependency 'superators'
  s.add_dependency 'syntax'
  s.add_dependency 'uuid'

  # Optional dependencies -- if we can't find these libraries, certain features
  # will be disabled.
  # s.add_dependency 'tokyocabinet'
  # s.add_dependency 'zookeeper'
end
