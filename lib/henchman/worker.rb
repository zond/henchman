
module Henchman

  class Worker
    class Task
      attr_accessor :headers
      attr_accessor :message
      attr_accessor :worker
      attr_accessor :exception
      attr_accessor :result
      def initialize(worker)
        @worker = worker
      end
      def handle_error(exception)
        @exception = exception
        @result = instance_eval(&(worker.error_handler))
      end
      def call
        @result = instance_eval(&(worker.block))
        forward
      end
      def forward
        if result.is_a?(Hash) && message.is_a?(Hash)
          queue_name = result.delete("next_queue")
          Fiber.new do
            Henchman.publish(queue_name, message.merge(result))
          end.resume
        end
      end
      def unsubscribe!
        worker.unsubscribe!
      end
    end
    attr_accessor :queue_name
    attr_accessor :error_handler
    attr_accessor :consumer
    attr_accessor :block
    def initialize(queue_name, consumer, &block)
      @block = block
      @queue_name = queue_name
      @consumer = consumer
      @error_handler = Proc.new do |exception|
        STDERR.puts("consume(#{queue_name.inspect}, #{headers.inspect}, #{message.inspect}): #{exception.message}")
        STDERR.puts(exception.backtrace.join("\n"))
      end
    end
    def task
      Task.new(self)
    end
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
