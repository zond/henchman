
require 'em-synchrony'
require 'amqp'
require 'yajl'

require 'henchman/worker'

module Henchman

  extend self

  @@connection = nil
  @@channel = nil

  def amqp_url
    ENV["AMQP_URL"] || "amqp://localhost/"
  end

  def amqp_options
    uri = URI.parse(amqp_url)
    {
      :vhost => uri.path,
      :host => uri.host,
      :user => uri.user || "guest",
      :port => uri.port || 5672,
      :pass => uri.password || "guest"
    }
  rescue Object => e
    raise "invalid AMQP_URL: #{uri.inspect} (#{e})"
  end
  
  def queue_options
    {
      :durable => true,
      :auto_delete => true
    }
  end

  def channel_options
    {
      :prefetch => 1,
      :auto_recovery => true
    }
  end

  def subscribe_options
    {
      :ack => true
    }
  end

  def stop!
    with_channel do |channel|
      channel.close
    end
    @@channel = nil
    with_connection do |connection|
      connection.close
    end
    @@connection = nil
  end

  def with_connection(&block)
    @@connection = AMQP.connect(amqp_options) if @@connection.nil? || @@connection.status == :closed
    @@connection.on_open do 
      yield @@connection
    end
  end

  def with_channel(&block)
    with_connection do |connection|
      @@channel = AMQP::Channel.new(connection, channel_options) if @@channel.nil? || @@channel.status == :closed
      @@channel.once_open do 
        yield @@channel
      end
    end
  end

  def with_direct_exchange(&block)
    with_channel do |channel|
      channel.direct(AMQ::Protocol::EMPTY_STRING, queue_options, &block)
    end
  end

  def with_queue(queue_name, &block)
    with_channel do |channel|
      channel.queue(queue_name, queue_options) do |queue|
        yield queue
      end
    end
  end

  def publish(queue_name, message)
    EM::Synchrony.sync(apublish(queue_name, message))
  end

  def apublish(queue_name, message)
    deferrable = EM::DefaultDeferrable.new
    with_direct_exchange do |exchange|
      exchange.publish(Yajl::Encoder.encode(message), :routing_key => queue_name) do 
        deferrable.set_deferred_status :succeeded
      end
    end
    deferrable
  end

  def consume(queue_name, &block) 
    worker = Worker.new(queue_name, &block)
    with_queue(queue_name) do |queue|
      consumer = AMQP::Consumer.new(@@channel, 
                                    queue, 
                                    queue.generate_consumer_tag(queue_name), # consumer_tag
                                    false, # exclusive
                                    false) # no_ack
      worker.consumer = consumer
      consumer.on_delivery do |headers, message|
        unless @@channel.connection.closing?
          task = worker.task
          begin
            task.headers = headers
            task.message = Yajl::Parser.parse(message)
            task.call
          rescue Exception => e
            begin
              task.handle_error(e)
            rescue Exception => e
              STDERR.puts e
              STDERR.puts e.backtrace.join("\n")
            end
          ensure
            task.ack!
          end
        end
      end
      consumer.consume
    end
  end

end
