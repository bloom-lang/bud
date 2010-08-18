Gem::Specification.new do |s|
  s.name = %q{bud}
  s.version = "0.0.1"
  s.date = %q{2010-07-19}
  s.authors = ["Joseph M. Hellerstein"]
  s.email = %q{jmh@berkeley.edu}
  s.summary = %q{Provides a prototype Bloom-like sublanguage in Ruby.}
  s.homepage = %q{http://bud.cs.berkeley.edu/}
  s.description = %q{This gem provides a prototype Bloom-like declarative distributed sublanguage for Ruby.}
  s.files = [ "README", "Changelog", "LICENSE", "lib/bud.rb", "lib/bud/aggs.rb", "lib/bud/collections.rb", "lib/bud/errors.rb", "lib/bud/events.rb", "lib/bud/parse_bud.rb", "lib/bud/strat.rb", "lib/bud/sane_r2r.rb", "lib/bud/forward_parse.rb" ]
  s.add_dependency 'msgpack'
  s.add_dependency 'eventmachine'
  s.add_dependency 'superators'
end
