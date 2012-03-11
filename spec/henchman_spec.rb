
dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift dir + '/../lib'

require 'henchman'

require 'rspec'

describe Henchman do
  
  around :each do |example|
    EM.synchrony do
      example.run
      Henchman.stop!
      EM.stop
    end
  end

  it 'should consume published jobs' do
    val = rand(1 << 32)
    found = nil
    deferrable = EM::DefaultDeferrable.new
    Henchman.consume("test.queue") do
      if message["val"] == val
        found = val
        deferrable.set_deferred_status :succeeded
      end
      nil
    end
    Henchman.publish("test.queue", :val => val)
    EM::Synchrony.sync deferrable
    found.should == val
  end

  it 'should forward consumed jobs if they ask for it' do
    val = rand(1 << 32)
    found = nil
    deferrable = EM::DefaultDeferrable.new
    Henchman.consume("test.queue2") do
      if message["val"] == val
        found = val
        deferrable.set_deferred_status :succeeded
      end
      nil
    end
    Henchman.consume("test.queue") do
      if message["val"] == val
        {"next_queue" => "test.queue2"}
      else
        nil
      end
    end
    Henchman.publish("test.queue", :val => val, :bajs => "hepp")
    EM::Synchrony.sync deferrable
    found.should == val
  end

  it 'should let many consumers consume off the same queue'

  it 'should let many producers produce to the same queue'

end

