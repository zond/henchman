# henchman

A thin wrapper around [amqp](https://github.com/ruby-amqp/amqp).

## Installation

### Ruby

We use Ruby 1.9.3. 

To install and run on your local machine use [RVM](https://rvm.beginrescueend.com/). 
For Mac machines make sure you compile with gcc-4.2 (because the compiler from Xcode doesn't compile Ruby 1.9.3 properly). 
Download and install gcc from https://github.com/kennethreitz/osx-gcc-installer 

    $ gem install rvm
    $ rvm install 1.9.3

And for Macs

    $ rvm install 1.9.3 --with-gcc=gcc-4.2

### Rubygems

Use [Bundler](http://gembundler.com/) to install the gems needed by Herdis

    $ bundle install

### RabbitMQ

henchman naturally needs [RabbitMQ](http://www.rabbitmq.com/) to run. Install it and run it with default options.

## Using

### Queues

To enqueue jobs that will only be consumed by a single consumer, you 

    Henchman.enqueue("test", {:time => Time.now.to_s})

within an EM.synchrony block.

To consume jobs enqueued this way

    Henchman.job("test") do
      puts message.inspect
      puts headers
    end.subscribe!

The `script/enqueue` and `script/consume` scripts provide a test case as simple as possible.

If you want a global error handler for the all consumers in your Ruby environment

    Henchman.error do
      global_error_handler(exception)
    end

### Broadcasts

To publish jobs that will be consumed by every consumer listening to your exchange, you

    Henchman.publish("testpub", {:time => Time.now.to_s})

within an EM.synchrony block.

To consume jobs published this way

    Henchman.receiver("test") do
      puts message.inspect
      puts headers
    end.subscribe!

The `script/publish` and `script/receive` scripts provide a test case as simple as possible.

Error handling is done the exact same way as with the single consumer case.

## Test suite

    $ rake

## Console

To run an eventmachine-friendly console to test your servers from IRB

    $ bundle exec em-console
