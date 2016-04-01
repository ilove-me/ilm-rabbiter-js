require 'singleton'
require 'bunny'
require 'thread/pool'

module Ilm
  module Rabbiter

    class Rabbiter
      include Singleton

      def self.connector
        @connector ||= Connector.new
      end

      def self.publisher
        @publisher ||= Publisher.new
      end

      def self.callbacks
        @callbacks ||= Callbacks.new
      end

      def self.aux
        @aux_obj ||= Aux.new
      end


      class MessageBuilder

        def self.default(data)
          return {data: data}.to_json
        end

        def self.success(data)
          return {success: true, data: data}.to_json
        end

        def self.error(err)
          return {
              success: false,
              error: err
          }.to_json
        end
      end


      class Callbacks

        def condition_variables_map
          @condition_variables_map ||= {}
        end

        def response_map
          @response_map ||= {}
        end


        def wait_condition(key, mutex)
          cv = ConditionVariable.new

          puts "======== WAIT CONDITION #{key} - #{cv}"

          condition_variables_map[key] = cv
          cv.wait(mutex)
        end

        def signal_condition(key)
          conditionVariable = condition_variables_map[key]

          puts "======== SIGNAL CONDITION #{key}  - #{conditionVariable}"

          conditionVariable.signal
          condition_variables_map.delete(key)
        end

        def has_condition(key)
          condition_variables_map.has_key?(key)
        end


        def set_response(key, response)
          response_map[key] = response
        end

        def get_response(key)
          rsp = JSON.parse(response_map[key], :symbolize_names => true)
          response_map.delete(key)

          if rsp[:success]
            return rsp[:data]
          else
            raise Exception.new(rsp[:error])
          end

        end

      end


      class Aux
        def convert_hash_keys(value)
          case value
            when Array
              value.map { |v| convert_hash_keys(v) }
            # or `value.map(&method(:convert_hash_keys))`
            when Hash
              Hash[value.map { |k, v| [underscore_key(k), convert_hash_keys(v)] }]
            else
              value
          end
        end

        def underscore_key(k)
          k.to_s.underscore.to_sym
          # Or, if you're not in Rails:
          # to_snake_case(k.to_s).to_sym
        end
      end


      class Publisher


        def mutex
          @mutex ||= Mutex.new
        end

        def on_response=(call)
          @response_callback = call
        end

        def on_response
          @response_callback
        end


        def receive_msg(body, properties)
          begin

            #ActionDispatch::Reloader.cleanup!
            #ActionDispatch::Reloader.prepare!

            message_id = properties[:message_id]
            correlation_id = properties[:correlation_id]
            reply_to = properties[:replyTo]

            context = properties[:headers]

            puts "\n\n\n--------\nRECEIVED ON #{message_id} --- REPLY TO #{reply_to} WITH CORID #{correlation_id}"

            if correlation_id && Ilm::Rabbiter::Rabbiter.callbacks.has_condition(correlation_id)
              Ilm::Rabbiter::Rabbiter.callbacks.set_response(correlation_id, body)
              mutex.synchronize { Ilm::Rabbiter::Rabbiter.callbacks.signal_condition(correlation_id) }
              return
            end

            if message_id

              rsp = on_response.(properties, body)

              puts "Controller Response #{rsp}\n\n"

              #when responding it should be asynchronous
              Ilm::Rabbiter::Rabbiter.publisher.send_msg(properties[:reply_to], nil, MessageBuilder.success(rsp), correlation_id, context)
            end


          rescue Exception => e
            puts "\n\nERROR CONTROLLER RESPONSE : #{e}\n\n"
            puts e.backtrace

            #when responding it should be asynchronous
            Ilm::Rabbiter::Rabbiter.publisher.send_msg(properties[:reply_to], nil, MessageBuilder.error(e), correlation_id, context)

          end
        end


        def send_msg(to_queue, message_id, msg, correlation_id = nil, user_id = nil, sync = true)

          send_opts = {
              reply_to: Ilm::Rabbiter::Rabbiter.connector.response_queue_name,
              message_id: message_id,
              routing_key: to_queue,
              correlation_id: correlation_id || "#{rand}",
              headers: {
                  user_id: user_id
              }
          }

          puts "\n\n\n--------\nSENDING TO #{send_opts[:routing_key]} WITH ACTION #{message_id} --- REPLY TO #{send_opts[:reply_to]} WITH CORID #{send_opts[:correlation_id]}"

          #@@channel.basic_publish(msg.to_s, ENV['RABBIT_EXCHANGE'], msg_id || send_queue, send_opts)
          rsp = Ilm::Rabbiter::Rabbiter.connector.exchange.publish(msg.to_s, send_opts)

          if message_id and sync #se tiver um message ID é porque fez um pedido e tem de esperar por ele, dada a natureza sincrona do ruby
            mutex.synchronize { Ilm::Rabbiter::Rabbiter.callbacks.wait_condition(send_opts[:correlation_id], mutex) }

            rsp = Ilm::Rabbiter::Rabbiter.callbacks.get_response(send_opts[:correlation_id])
          end

          rsp
        end

      end

      class Connector

        attr_accessor :response_queue_id

        @@options = {
            connection: {
                user: ENV['RABBIT_USER'],
                pass: ENV['RABBIT_PASS'],
                host: ENV['RABBIT_HOST'],
                port: ENV['RABBIT_PORT']
            },
            queue_name: (ENV['RABBIT_QUEUE'] || "") + (ENV['RABBIT_SUFIX'] ? "_" + ENV['RABBIT_SUFIX'].to_s : ""),
            exchange_name: ENV['RABBIT_EXCHANGE'] || "",
            prefetch: 0,
            durable: false,
            auto_delete: true,
            async: false
        }

        @@connection = nil
        @@channel = nil
        @@exchange = nil

        def exchange
          @@exchange
        end

        def channel
          @@channel
        end

        def connection_uri
          @@options[:connection]
        end

        def queue_name(priority = response_queue_id)
          @@options[:queue_name] + '_' + priority.to_s;
        end

        def response_queue_name
          @response_queue_name ||= @@options[:queue_name] + "_" + (rand*100000000).to_i.to_s
        end


        def thread_pool
          @thread_pool ||= Thread.pool( ENV["DB_POOL"] || ENV['MAX_THREADS'] || 20)
        end

        def reload_modules_from_disk(controller_key, all_models = true)
          #reload from disk if not in production

          if !Rails.env.production?
            $".delete_if { |s| s.include?(controller_key) }
            require Rails.root.join('app/controllers/', controller_key).to_s

            Dir.glob(Rails.root.join('app/models/**/*.rb')).each do |x|

              $".delete_if { |s| s.include?(x) }
              require x
            end

            Dir.glob(Rails.root.join('app/serializers/**/*.rb')).each do |x|

              $".delete_if { |s| s.include?(x) }
              require x
            end

          end
        end


        def create

          #NONE NAO FAZ NADA
          return if ENV['RABBIT_TYPE'] == "none"

          #create connection
          begin
            @@connection = Bunny.new(connection_uri)
            @@connection.start
          rescue Bunny::TCPConnectionFailed => e
            puts "Connection failed"
          end


          begin

            #open channel
            @@channel = @@connection.create_channel

            @@channel.prefetch(@@options[:prefetch])

            @@exchange = @@channel.direct(ENV['RABBIT_EXCHANGE'])


            #bind queues
            if ENV['RABBIT_TYPE'] == "publisher"
              queues_names = [response_queue_name]
            else
              queues_names = [queue_name("normal"), queue_name("high"), queue_name("low"), response_queue_name]
            end

            #default callback message_id = "controller#action"
            on_response_callback = -> properties, body do

              #call controller by name
              callKey = properties[:message_id].split("#")
              actionKey = callKey[1]

              controller_key = 'api/v1/' + callKey[0] + '_controller'


              #TODO: CHECK IF THE CONTROLLER EXISTS

              puts "Controller Key: #{controller_key}"
              controller = controller_key.classify.safe_constantize.new

              params = Ilm::Rabbiter::Rabbiter.aux.convert_hash_keys(JSON.parse(body, :symbolize_names => true))

              controller.send("context=", properties[:headers])
              controller.send("jsonapi_params=", params)
              return controller.send(actionKey)

            end

            Ilm::Rabbiter::Rabbiter.publisher.on_response = on_response_callback

            #create queues
            queues_names.each do |queue_name|

              queue = @@channel.queue(queue_name, {durable: @@options[:durable], auto_delete: @@options[:auto_delete], routing_key: queue_name})

              queue.bind(ENV['RABBIT_EXCHANGE'], routing_key: queue_name)

              queue.subscribe do |delivery_info, properties, body|
                thread_pool.process do
                  Ilm::Rabbiter::Rabbiter.publisher.receive_msg(body, properties)
                end
              end

            end


              puts "Rabbiter sucessfully loaded for service #{@@options[:queue_name]}!"

          rescue Bunny::PreconditionFailed => e
            puts "Channel-level exception! Code: #{e.channel_close.reply_code}, message: #{e.channel_close.reply_text}".squish

            delete_connection
          end

        end


        def delete_connection
          [queue_name("normal"), queue_name("high"), queue_name("low"), response_queue_name].each do |priority|
            @@channel.queue_delete(queue_name(priority))
          end

          @@channel.close
        end

      end
    end

  end
end