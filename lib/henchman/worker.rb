
module Henchman

  #
  # A class that handles incoming messages.
  #
  class Worker

    #
    # The handling of an incoming message.
    #
    class Task

      #
      # [AMQP::Header] The metadata of the message.
      #
      attr_accessor :headers

      #
      # [Object] The message itself
      attr_accessor :message

      #
      # [Henchman::Worker] the {::Henchman::Worker} this {::Henchman::Worker::Task} belongs to.
      #
      attr_accessor :worker

      #
      # [Exception] any {::Exception} this {::Henchman::Worker::Task} has fallen victim to.
      #
      attr_accessor :exception

      #
      # [Object] the result of executing this {::Henchman::Worker::Task}.
      #
      attr_accessor :result

      #
      # Create a {::Henchman::Worker::Task} for a given {::Henchman::Worker}.
      #
      # @param [Henchman::Worker] worker the {::Henchman::Worker} creating this {::Henchman::Worker::Task}.
      #
      def initialize(worker)
        @worker = worker
      end

      #
      # Handle an exception for a {::Henchman::Worker::Task}.
      #
      # @param [Exception] exception the {::Exception} that happened.
      #
      def handle_error(exception)
        @exception = exception
        @result = instance_eval(&(worker.error_handler))
      end

      #
      # Call this {::Henchman::Worker::Task}.
      #
      def call
        @result = instance_eval(&(worker.block))
        forward
      end

      #
      # Potentially forward the results of this {::Henchman::Worker::Task} to another queue.
      #
      # If the result of the {::Henchman::Worker::Task} is a {::Hash} that includes
      # <code>"next_queue"</code> the message to this {::Henchman::Worker::Task} will be merged
      # with the result of this {::Henchman::Worker::Task} and sent to the value of the 
      # <code>"next_queue"</code> key.
      #
      def forward
        if result.is_a?(Hash) && message.is_a?(Hash)
          queue_name = result.delete("next_queue")
          Fiber.new do
            Henchman.publish(queue_name, message.merge(result))
          end.resume
        end
      end

      #
      # Acknowledge this message.
      #
      def ack!
        headers.ack
      end

      #
      # Unsubscribe the {::Henchman::Worker} of this {::Henchman::Worker::Task} from the queue it subscribes to.
      # 
      def unsubscribe!
        worker.unsubscribe!
      end
    end

    #
    # [String] the name of the queue this {::Henchman::Worker} listens to.
    #
    attr_accessor :queue_name

    #
    # [Proc] the {::Proc} handling errors for this {::Henchman::Worker}.
    #
    attr_accessor :error_handler

    #
    # [AMQP::Consumer] the consumer feeding this {::Henchman::Worker} with messages.
    #
    attr_accessor :consumer

    #
    # [Proc] the {::Proc} handling the messages for this {::Henchman::Worker}.
    #
    attr_accessor :block

    #
    # @param [String] queue_name the name of the queue this worker listens to.
    # @param [Proc] block the {::Proc} that will handle the messages for this {::Henchman::Worker}.
    #
    def initialize(queue_name, &block)
      @block = block
      @queue_name = queue_name
      @error_handler = Proc.new do |exception|
        STDERR.puts("consume(#{queue_name.inspect}, #{headers.inspect}, #{message.inspect}): #{exception.message}")
        STDERR.puts(exception.backtrace.join("\n"))
      end
    end

    #
    # @return [Henchman::Worker::Task] a {::Henchman::Worker::Task} for this {::Henchman::Worker}.
    #
    def task
      Task.new(self)
    end

    #
    # Give this {::Henchman::Worker} a custom error handler.
    #
    # @param [Proc] block the {::Proc} to use for errors in this {::Henchman::Worker}. 
    #   The {::Proc} will be <code>instance_eval</code>ed inside a {::Henchman::Worker::Task}
    #   and have access to all the particulars of the message causing the {::Exception} through
    #   that instance.
    #
    def error(&block)
      @error_handler = block
    end

    #
    # Unsubscribe this {::Henchman::Worker} from its queue.
    #
    def unsubscribe!
      deferrable = EM::DefaultDeferrable.new
      consumer.cancel do
        deferrable.set_deferred_status :succeeded
      end
      Fiber.new do
        EM::Synchrony.sync deferrable
      end.resume
    end
  end

end
