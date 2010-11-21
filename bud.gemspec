Gem::Specification.new do |s|
  s.name = %q{bud}
  s.version = "0.0.1"
  s.date = %q{2010-07-19}
  s.authors = ["Joseph M. Hellerstein"]
  s.email = %q{jmh@berkeley.edu}
  s.summary = %q{Provides a prototype Bloom-like sublanguage in Ruby.}
  s.homepage = %q{http://bud.cs.berkeley.edu/}
  s.description = %q{This gem provides a prototype Bloom-like declarative distributed sublanguage for Ruby.}

  s.files = ["README", "Changelog", "LICENSE", "lib/bud.rb"]
  s.files += Dir.entries("lib/bud").select{|f| f.include? ".rb"}.map{|f| "lib/bud/" + f}

  s.add_dependency 'msgpack'
  s.add_dependency 'eventmachine'
  s.add_dependency 'superators'
  s.add_dependency 'ParseTree'
  s.add_dependency 'sexp_path'
  s.add_dependency 'ruby2ruby'
  s.add_dependency 'anise'
  s.add_dependency 'ruby-graphviz'
  s.add_dependency 'backports'
end
