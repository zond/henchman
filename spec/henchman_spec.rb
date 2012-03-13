
dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift dir + '/../lib'

require 'henchman'

require 'rspec'

describe Henchman do

  context 'without amqp running' do

    it 'allows testing of workers' do
      val = rand(1 << 32)
      found = nil
      worker = Henchman::Worker.new("test.queue") do
        if message["val"] == val
          found = val
        end
      end
      worker.call("val" => val)
      found.should == val
    end

  end
  
  context 'with amqp running' do

    around :each do |example|
      EM.synchrony do
        example.run
        Henchman.stop!
        EM.stop
      end
    end

    it 'should consume jobs' do
      val = rand(1 << 32)
      found = nil
      deferrable = EM::DefaultDeferrable.new
      Henchman::Worker.new("test.queue") do
        if message["val"] == val
          found = val
          deferrable.set_deferred_status :succeeded
        end
        nil
      end.consume!
      Henchman.enqueue("test.queue", :val => val)
      EM::Synchrony.sync deferrable
      found.should == val
    end

    it 'should forward consumed jobs if they ask for it' do
      val = rand(1 << 32)
      found = nil
      deferrable = EM::DefaultDeferrable.new
      Henchman::Worker.new("test.queue2") do
        if message["val"] == val
          found = val
          deferrable.set_deferred_status :succeeded
        end
        nil
      end.consume!
      Henchman::Worker.new("test.queue") do
        if message["val"] == val
          enqueue("test.queue2", "val" => val)
        else
          nil
        end
      end.consume!
      Henchman.enqueue("test.queue", :val => val, :bajs => "hepp")
      EM::Synchrony.sync deferrable
      found.should == val
    end

    it 'should be able to unsubscribe' do
      val = rand(1 << 32)
      found = 0
      deferrable = EM::DefaultDeferrable.new
      Henchman::Worker.new("test.queue") do
        if message["val"] == val
          found += 1
          unsubscribe!
          deferrable.set_deferred_status :succeeded
        end
        nil
      end.consume!
      Henchman.enqueue("test.queue", :val => val)
      Henchman.enqueue("test.queue", :val => val)
      EM::Synchrony.sync deferrable
      EM::Synchrony.sleep 0.2
      found.should == 1
    end
    
    it 'handles errors with a global error handler' do
      val = rand(1 << 32)
      error = nil
      deferrable = EM::DefaultDeferrable.new
      Henchman::Worker.new("test.queue") do
        if message["val"] == val
          raise "error!"
        end
        nil
      end.consume!
      Henchman.error do
        if exception.message == "error!"
          error = exception
          deferrable.set_deferred_status :succeeded
        end
      end
      Henchman.enqueue("test.queue", :val => val)
      EM::Synchrony.sync deferrable
      error.message.should == "error!"
    end

    it 'should let many consumers consume off the same queue' do
      consumers = Set.new
      found = 0
      val = rand(1 << 32)
      deferrable = EM::DefaultDeferrable.new
      Henchman::Worker.new("test.queue") do
        if message["val"] == val
          consumers << "1"
          found += 1
          deferrable.set_deferred_status :succeeded if found == 10
        end
        nil
      end.consume!
      Henchman::Worker.new("test.queue") do
        if message["val"] == val
          consumers << "2"
          found += 1
          deferrable.set_deferred_status :succeeded if found == 10
        end
        nil
      end.consume!
      Henchman::Worker.new("test.queue") do
        if message["val"] == val
          consumers << "3"
          found += 1
          deferrable.set_deferred_status :succeeded if found == 10
        end
        nil
      end.consume!
      10.times do
        Henchman.enqueue("test.queue", :val => val)
      end
      EM::Synchrony.sync deferrable
      consumers.should == Set.new(["1", "2", "3"])
      found.should == 10
    end

    it 'should let many consumers consume off the same fanout' do
      consumers = Set.new
      found = 0
      val = rand(1 << 32)
      deferrable = EM::DefaultDeferrable.new
      Henchman::Worker.new("test.exchange") do
        if message["val"] == val
          consumers << "1"
          found += 1
          deferrable.set_deferred_status :succeeded if found == 30
        end
        nil
      end.subscribe!
      Henchman::Worker.new("test.exchange") do
        if message["val"] == val
          consumers << "2"
          found += 1
          deferrable.set_deferred_status :succeeded if found == 30
        end
        nil
      end.subscribe!
      Henchman::Worker.new("test.exchange") do
        if message["val"] == val
          consumers << "3"
          found += 1
          deferrable.set_deferred_status :succeeded if found == 30
        end
        nil
      end.subscribe!
      10.times do |n|
        Henchman.publish("test.exchange", :val => val, :n => n)
      end
      EM::Synchrony.sync deferrable
      consumers.should == Set.new(["1", "2", "3"])
      found.should == 30
    end

  end

end

