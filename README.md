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

Within an EM.synchrony block

    Henchman.enqueue("test", {:time => Time.now.to_s})

or

    Henchman.start("test") do
      puts message.inspect
      puts headers
    end

The `script/enqueue` and `script/consume` scripts provide a test case as simple as possible.

If you want error handling, you can just provide an error handling block with the return value of `consume`

    Henchman.start("test") do
      clever_message_handler(message)
    end.error do
      clever_error_handler(exception)
    end

## Test suite

    $ rake

## Console

To run an eventmachine-friendly console to test your servers from IRB

    $ bundle exec em-console
