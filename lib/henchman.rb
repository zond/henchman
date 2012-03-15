
require 'em-synchrony'
require 'amqp'
require 'multi_json'

require 'henchman/worker'

#
# Thin wrapper around AMQP
#
module Henchman

  extend self

  @@connection = nil
  @@channel = nil
  @@error_handler = Proc.new do
    STDERR.puts("consume(#{queue_name.inspect}, #{headers.inspect}, #{message.inspect}): #{exception.message}")
    STDERR.puts(exception.backtrace.join("\n"))
  end
  @@logger = Proc.new do |msg|
    puts msg
  end

  #
  # Define a log handler.
  #
  # @param [Proc] block the block that handles log messages.
  #
  def logger(&block)
    @@logger = block
  end
  
  #
  # Log a message.
  #
  # @param [String] msg the message to log.
  #
  def log(msg)
  end

  #
  # @return [Proc] the error handler
  #
  def self.error_handler
    @@error_handler
  end
  
  #
  # Define an error handler.
  #
  # @param [Proc] block the block that handles errors.
  #
  def error(&block)
    @@error_handler = block
  end
  
  #
  # Will return a URL to the AMQP broker to use. Will get this from the <code>ENV</code> variable <code>AMQP_URL</code> if present.
  #
  # @return [String] a URL to an AMQP broker.
  #
  def amqp_url
    @amqp_url ||= (ENV["AMQP_URL"] || "amqp://localhost/")
  end

  #
  # @return [String] the AMQP broker url to use.
  #
  def amqp_url=(url)
    @amqp_url = url
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
  # Will return the default options to use when creating queues.
  #
  # If you change the returned {::Hash} the changes will persist in this instance, so use this to configure stuff.
  #
  # @return [Hash] a {::Hash} of options to use when creating queues.
  #
  def queue_options
    @queue_options ||= {
      :durable => true,
      :auto_delete => true
    }
  end

  #
  # Will return the default options to use when creating exchanges.
  #
  # If you change the returned {::Hash} the changes will persist in this instance, so use this to configure stuff.
  #
  # @return [Hash] a {::Hash} of options to use when creating exchanges.
  #
  def exchange_options
    @exchange_options ||= {
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
    @@connection.on_tcp_connection_loss do
      log("#{self} reconnecting")
      @@connection.reconnect
    end
    @@connection.on_recovery do
      log("#{self} reconnected!")
    end 
    @@connection.on_error do |connection, connection_close|
      raise "#{connection}: #{connection_close.reply_text}"
    end
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
      @@channel.on_error do |channel, channel_close|
        log("#{self} reinitializing #{channel} due to #{channel_close}")
        channel.reuse
      end
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
      channel.direct(AMQ::Protocol::EMPTY_STRING, exchange_options, &block)
    end
  end

  #
  # Will yield an open and ready fanout exchange.
  #
  # @param [String] exchange_name the name of the exchange to create or find.
  # @param [Proc] block a {::Proc} to yield an open and ready fanout exchange to.
  #
  def with_fanout_exchange(exchange_name, &block)
    with_channel do |channel|
      channel.fanout(exchange_name, exchange_options, &block)
    end
  end

  #
  # Will yield an open and ready queue bound to an open and ready fanout exchange.
  #
  # @param [String] exchange_name the name of the exchange to create or find
  # @param [Proc] block the {::Proc} to yield an open and ready queue bound to the found exchange to.
  #
  def with_fanout_queue(exchange_name, &block)
    with_channel do |channel|
      with_fanout_exchange(exchange_name) do |exchange|
        channel.queue do |queue|
          queue.bind(exchange) do
            yield queue
          end
        end
      end
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
  # Enqueue a message synchronously.
  #
  # @param [String] queue_name the name of the queue to enqueue on.
  # @param [Object] message the message to enqueue.
  #
  def enqueue(queue_name, message)
    EM::Synchrony.sync(aenqueue(queue_name, message))
  end

  #
  # Enqueue a message asynchronously.
  #
  # @param (see #publish)
  #
  # @return [EM::Deferrable] a deferrable that will succeed when the publishing is done.
  #
  def aenqueue(queue_name, message)
    deferrable = EM::DefaultDeferrable.new
    with_direct_exchange do |exchange|
      exchange.publish(MultiJson.encode(message), :routing_key => queue_name) do
        deferrable.set_deferred_status :succeeded
      end
    end
    deferrable
  end

  #
  # Publish a a message to multiple consumers synchronously.
  #
  # @param [String] exchange_name the name of the exchange to publish on.
  # @param [Object] message the object to publish
  #
  def publish(exchange_name, message)
    EM::Synchrony.sync(apublish(exchange_name, message))
  end
  
  #
  # Publish a message to multiple consumers asynchronously.
  #
  # @param (see #publish)
  #
  # @return [EM::Deferrable] a deferrable that will succeed when the publishing is done.
  #
  def apublish(exchange_name, message)
    deferrable = EM::DefaultDeferrable.new
    with_fanout_exchange(exchange_name) do |exchange|
      exchange.publish(MultiJson.encode(message)) do
        deferrable.set_deferred_status :succeeded
      end
    end
    deferrable
  end

end

