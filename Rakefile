require 'rake'
require 'rake/testtask'

PRJ = "bud"

def version
  @version ||= begin
    $LOAD_PATH.unshift 'lib'
    require 'bud/version'
    warn "Bud::VERSION not a string" unless Bud::VERSION.kind_of? String
    Bud::VERSION
  end
end

def tag
  @tag ||= "v#{version}"
end

desc "Run all tests"
task :test => "test:unit"

TESTS = FileList["test/tc_*.rb"]
SLOW_TESTS = %w{ test/tc_execmodes.rb }

namespace :test do
  desc "Run unit tests"
  Rake::TestTask.new :unit do |t|
    t.libs << "lib"
    t.ruby_opts = %w{ -C test }
    t.test_files = TESTS.sub('test/', '')
      ### it would be better to make each tc_*.rb not depend on pwd
  end

  desc "Run quick unit tests"
  Rake::TestTask.new :quick do |t|
    t.libs << "lib"
    t.ruby_opts = %w{ -C test }
    t.test_files = TESTS.exclude(*SLOW_TESTS).sub('test/', '')
  end

  desc "Run quick non-zk unit tests"
  Rake::TestTask.new :quick_no_zk do |t|
    t.libs << "lib"
    t.ruby_opts = %w{ -C test }
    t.test_files = TESTS.
      exclude('test/tc_zookeeper.rb').
      exclude(*SLOW_TESTS).
      sub('test/', '')
  end
end

desc "Commit, tag, and push repo; build and push gem"
task :release => "release:is_new_version" do
  require 'tempfile'
  
  sh "gem build #{PRJ}.gemspec"

  file = Tempfile.new "template"
  begin
    file.puts "release #{version}"
    file.close
    sh "git commit --allow-empty -a -v -t #{file.path}"
  ensure
    file.close unless file.closed?
    file.unlink
  end

  sh "git tag #{tag}"
  sh "git push"
  sh "git push --tags"
  
  sh "gem push #{tag}.gem"
end

namespace :release do
  desc "Diff to latest release"
  task :diff do
    latest = `git describe --abbrev=0 --tags --match 'v*'`.chomp
    sh "git diff #{latest}"
  end

  desc "Log to latest release"
  task :log do
    latest = `git describe --abbrev=0 --tags --match 'v*'`.chomp
    sh "git log #{latest}.."
  end

  task :is_new_version do
    abort "#{tag} exists; update version!" unless `git tag -l #{tag}`.empty?
  end
end
