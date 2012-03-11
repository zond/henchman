
require 'em-synchrony'
require 'amqp'
require 'yajl'

module Henchman

  class Execution
    attr_accessor :headers
    attr_accessor :message
    def initialize(headers, message)
      @headers = headers
      @message = message
    end
  end

  extend self

  @@connection = nil
  @@channel = nil
  @@error_handler = Proc.new do |queue_name, headers, message, exception|
    STDERR.puts("consume(#{queue_name.inspect}, #{headers.inspect}, #{message.inspect}): #{exception.message}")
    STDERR.puts(exception.backtrace.join("\n"))
  end

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

  def error_handler=(&block)
    @@error_handler = block
  end

  def handle_error(queue_name, headers, message, exception)
    @@error_handler.call(queue_name, headers, message, exception)
  end

  def forward(result, argument)
    queue_name = result.delete("next_queue")
    Fiber.new do
      publish(queue_name, argument.merge(result))
    end.resume
  end
  
  def consume(queue_name, &block) 
    with_queue(queue_name) do |queue|
      queue.subscribe(subscribe_options) do |headers, message|
        unless @@channel.connection.closing?
          begin
            argument = Yajl::Parser.parse(message)
            execution = Execution.new(headers, argument)
            result = execution.instance_eval(&block)
            forward(result, argument) if result.is_a?(Hash) && argument.is_a?(Hash)
          rescue Exception => e
            handle_error(queue_name, headers, message, e)
          ensure
            headers.ack
          end
        end
      end
    end
  end

end
