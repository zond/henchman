# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = "henchman"
  s.version     = "0.0.2"
  s.authors     = ["Martin Bruse"]
  s.email       = ["martin@oort.se"]
  s.homepage    = "https://github.com/zond/henchman"
  s.summary     = %q{A maximally simple amqp wrapper}
  s.description = %q{A maximally simple amqp wrapper}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency 'amqp'
  s.add_dependency 'em-synchrony'
  s.add_dependency 'multi_json'

end
