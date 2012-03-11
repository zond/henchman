
require 'em-synchrony'
require 'amqp'
require 'yajl'

require 'henchman/worker'

#
# Thin wrapper around AMQP
#
module Henchman

  extend self

  @@connection = nil
  @@channel = nil

  #
  # Will return a URL to the AMQP broker to use. Will get this from the <code>ENV</code> variable <code>AMQP_URL</code> if present.
  #
  # @return [String] a URL to an AMQP broker.
  #
  def amqp_url
    ENV["AMQP_URL"] || "amqp://localhost/"
  end

  #
  # Will return the default options when connecting to the AMQP broker.
  #
  # Uses the URL from {#amqp_url} to construct these options.
  #
  # @return [Hash] a {::Hash} of options to AMQP.connect.
  #
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

  #
  # Will return the default options to use when creating queues and exchanges.
  #
  # If you change the returned {::Hash} the changes will persist in this instance, so use this to configure stuff.
  #
  # @return [Hash] a {::Hash} of options to use when creating exchanges and queues.
  #
  def queue_options
    @queue_options ||= {
      :durable => true,
      :auto_delete => true
    }
  end

  #
  # Will return the default options to use when creating channels.
  #
  # If you change the returned {::Hash} the changes will persist in this instance, so use this to configure stuff.
  #
  # @return [Hash] a {::Hash} of options to use when creating channels.
  #
  def channel_options
    @channel_options ||= {
      :prefetch => 1,
      :auto_recovery => true
    }
  end
  
  #
  # Will stop and deactivate {::Henchman}.
  #
  def stop!
    with_channel do |channel|
      channel.close
    end
    @@channel = nil
    with_connection do |connection|
      connection.close
    end
    @@connection = nil
    AMQP.stop
  end

  #
  # Will yield an open and ready connection.
  #
  # @param [Proc] block a {::Proc} to yield an open and ready connection to.
  #
  def with_connection(&block)
    @@connection = AMQP.connect(amqp_options) if @@connection.nil? || @@connection.status == :closed
    @@connection.on_open do 
      yield @@connection
    end
  end

  #
  # Will yield an open and ready channel.
  #
  # @param [Proc] block a {::Proc} to yield an open and ready channel to.
  #
  def with_channel(&block)
    with_connection do |connection|
      @@channel = AMQP::Channel.new(connection, channel_options) if @@channel.nil? || @@channel.status == :closed
      @@channel.once_open do 
        yield @@channel
      end
    end
  end

  #
  # Will yield an open and ready direct exchange.
  #
  # @param [Proc] block a {::Proc} to yield an open and ready direct exchange to.
  #
  def with_direct_exchange(&block)
    with_channel do |channel|
      channel.direct(AMQ::Protocol::EMPTY_STRING, queue_options, &block)
    end
  end

  #
  # Will yield an open and ready queue.
  #
  # @param [Proc] block a {::Proc} to yield an open and ready queue to.
  #
  def with_queue(queue_name, &block)
    with_channel do |channel|
      channel.queue(queue_name, queue_options) do |queue|
        yield queue
      end
    end
  end

  #
  # Publish a message synchronously.
  #
  # @param [String] queue_name the name of the queue to publish to.
  # @param [Object] message the message to publish
  #
  def publish(queue_name, message)
    EM::Synchrony.sync(apublish(queue_name, message))
  end

  #
  # Publish a message asynchronously.
  #
  # @param (see #publish)
  #
  # @return [EM::Deferrable] a deferrable that will succeed when the publishing is done.
  #
  def apublish(queue_name, message)
    deferrable = EM::DefaultDeferrable.new
    with_direct_exchange do |exchange|
      exchange.publish(Yajl::Encoder.encode(message), :routing_key => queue_name) do 
        deferrable.set_deferred_status :succeeded
      end
    end
    deferrable
  end
  
  #
  # Shorthand for <code>EM.synchrony do consume(queue_name, &block) end</code>.
  #
  def start(queue_name, &block)
    EM.synchrony do
      consume(queue_name, &block)
    end
  end

  #
  # Consume messages synchronously.
  #
  # @param [String] queue_name the name of the queue to consume messages from.
  # @param [Proc] block the block to call for each message. The block will be 
  #   <code>instance_eval</code>ed inside a {::Henchman::Worker::Task} and have 
  #   access to all the particulars of the message through that instance.
  #
  # @return [Henchman::Worker] a {::Henchman::Worker} that will execute {::Henchman::Worker::Tasks} for
  #   each received message.
  #
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
        if queue.channel.status == :opened
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
    worker
  end

end
