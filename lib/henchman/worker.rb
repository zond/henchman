
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
      # @param [AMQP::Header] header the {::AMQP::Header} being handled.
      # @param [Object] message the {::Object} being handled.
      #
      def initialize(worker, headers, message)
        @worker = worker
        @headers = headers
        @message = message
      end

      #
      # Call this {::Henchman::Worker::Task}.
      #
      def call
        begin
          @result = instance_eval(&(worker.block))
        rescue Exception => e
          @exception = e
          @result = instance_eval(&(Henchman.error_handler))
        ensure
          headers.ack
        end
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
      # Unsubscribe the {::Henchman::Worker} of this {::Henchman::Worker::Task} from the queue it subscribes to.
      # 
      def unsubscribe!
        worker.unsubscribe!
      end
    end

    @@workers = {}

    #
    # @return [Array<Henchman::Worker>] the {::Henchman::Workers} created in this ruby.
    #
    def self.workers
      @@workers
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
    # [Symbol] the type of exchange this {::Henchman::Worker} listens to.
    #
    attr_accessor :exchange_type
    
    #
    # @param [String] queue_name the name of the queue this worker listens to.
    # @param [Symbol] exchange_type the type of exchange this worker will connect its queue to.
    # @param [Proc] block the {::Proc} that will handle the messages for this {::Henchman::Worker}.
    #
    def initialize(queue_name, exchange_type, &block)
      @block = block
      @exchange_type = exchange_type
      @queue_name = queue_name
      @@workers[queue_name] ||= []
      @@workers[queue_name] << self
    end

    #
    # Make this {::Henchman::Worker} subscribe to its queue.
    #
    def subscribe!
      deferrable = EM::DefaultDeferrable.new
      if exchange_type == :direct
        Henchman.with_queue(queue_name) do |queue|
          Henchman.subscribe(queue, self, deferrable)
        end
      elsif exchange_type == :fanout
        Henchman.with_fanout_queue(queue_name) do |queue|
          Henchman.subscribe(queue, self, deferrable)
        end
      end
      EM::Synchrony.sync deferrable
    end

    #
    # Call this worker with some data.
    #
    # @param [AMQP::Header] headers the headers to handle.
    # @param [Object] message the message to handle.
    #
    # @return [Henchman::Worker::Task] a {::Henchman::Worker::Task} for this {::Henchman::Worker}.
    #
    def call(headers, message)
      task = Task.new(self, headers, message)
      begin
        task.call
      ensure
        route = (headers.header[:headers] || {})["route"]
        if !route.nil? && !task.result.nil?
          route_parts = route.split(/,/)
          next_queue_name, method_name = route_parts.shift.split(/:/)
          if method_name.nil?
            if exchange_type == :direct
              method_name = :enqueue
            elsif exchange_type == :fanout
              method_name = :publish
            end
          end
          Fiber.new do
            Henchman.send(method_name, [next_queue_name] + route_parts, task.result)
          end.resume
        end
      end
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
