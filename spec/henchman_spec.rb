
dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift dir + '/../lib'

require 'henchman'

require 'rspec'

describe Henchman do
  
  it 'should consume published jobs' do
    EM.synchrony do
      val = rand(1 << 32)
      found = nil
      deferrable = EM::DefaultDeferrable.new
      Henchman.consume("test.queue") do
        if message["val"] == val
          found = val
          deferrable.set_deferred_status :succeeded
        end
      end
      Henchman.publish("test.queue", :val => val)
      EM::Synchrony.sync deferrable
      found.should == val
      EM.stop
    end
  end

end

