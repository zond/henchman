
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
        @result = instance_eval(&(Henchman.error_handler))
      end

      #
      # Call this {::Henchman::Worker::Task}.
      #
      def call
        @result = instance_eval(&(worker.block))
      end

      #
      # Enqueue something on another queue.
      #
      # @param [String] queue_name the name of the queue on which to publish.
      # @param [Object] message the message to publish-
      #
      def enqueue(queue_name, message)
        Fiber.new do
          Henchman.enqueue(queue_name, message)
        end.resume
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
      @error_handler = nil
    end

    #
    # @return [Henchman::Worker::Task] a {::Henchman::Worker::Task} for this {::Henchman::Worker}.
    #
    def task
      Task.new(self)
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
