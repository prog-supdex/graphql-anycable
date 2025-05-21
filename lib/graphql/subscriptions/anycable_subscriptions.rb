# frozen_string_literal: true

require "anycable"
require "graphql/subscriptions"
require "graphql/anycable/errors"

# rubocop: disable Metrics/AbcSize, Metrics/LineLength, Metrics/MethodLength

# A subscriptions implementation that sends data as AnyCable broadcastings.
#
# Since AnyCable is aimed to be compatible with ActionCable, this adapter
# may be used as (practically) drop-in replacement to ActionCable adapter
# shipped with graphql-ruby.
#
# @example Adding AnyCableSubscriptions to your schema
#   MySchema = GraphQL::Schema.define do
#     use GraphQL::Subscriptions::AnyCableSubscriptions
#   end
#
# @example Implementing a channel for GraphQL Subscriptions
#   class GraphqlChannel < ApplicationCable::Channel
#     def execute(data)
#       query = data["query"]
#       variables = ensure_hash(data["variables"])
#       operation_name = data["operationName"]
#       context = {
#         current_user: current_user,
#         # Make sure the channel is in the context
#         channel: self,
#       }
#
#       result = MySchema.execute({
#         query: query,
#         context: context,
#         variables: variables,
#         operation_name: operation_name
#       })
#
#       payload = {
#         result: result.subscription? ? {data: nil} : result.to_h,
#         more: result.subscription?,
#       }
#
#       transmit(payload)
#     end
#
#     def unsubscribed
#       MySchema.subscriptions.delete_channel_subscriptions(self)
#     end
#   end
#
module GraphQL
  class Subscriptions
    class AnyCableSubscriptions < GraphQL::Subscriptions
      extend Forwardable

      def_delegators :"GraphQL::AnyCable", :with_redis, :config
      def_delegators :"::AnyCable", :broadcast

      SUBSCRIPTION_PREFIX = "subscription:"  # HASH: Stores subscription data: query, context, …
      FINGERPRINTS_PREFIX = "fingerprints:"  # ZSET: To get fingerprints by topic
      SUBSCRIPTIONS_PREFIX = "subscriptions:" # SET:  To get subscriptions by fingerprint
      CHANNEL_PREFIX = "channel:"       # SET:  Auxiliary structure for whole channel's subscriptions cleanup

      # @param serializer [<#dump(obj), #load(string)] Used for serializing messages before handing them to `.broadcast(msg)`
      def initialize(serializer: Serialize, **rest)
        @serializer = serializer
        super
      end

      # An event was triggered.
      # Re-evaluate all subscribed queries and push the data over ActionCable.
      def execute_all(event, object)
        fingerprints = with_redis { |redis| redis.zrange(redis_key(FINGERPRINTS_PREFIX) + event.topic, 0, -1) }
        return if fingerprints.empty?

        fingerprint_subscription_ids = with_redis do |redis|
          fingerprints.zip(
            redis.pipelined do |pipeline|
              fingerprints.map do |fingerprint|
                pipeline.smembers(redis_key(SUBSCRIPTIONS_PREFIX) + fingerprint)
              end
            end
          ).to_h
        end

        fingerprint_subscription_ids.each do |fingerprint, subscription_ids|
          execute_grouped(fingerprint, subscription_ids, event, object)
        end

        # Call to +trigger+ returns this. Convenient for playing in console
        fingerprint_subscription_ids.map { |k, v| [k, v.size] }.to_h
      end

      # The fingerprint has told us that this response should be shared by all subscribers,
      # so just run it once, then deliver the result to every subscriber
      def execute_grouped(fingerprint, subscription_ids, event, object)
        return if subscription_ids.empty?

        result = nil

        # Iterate through all subscriptions to find the subscription which:
        # 1. still exists in Redis
        # 2. got a result when updated with the event
        # This protects in cases where a subscription could expire between checking a subscription exists and
        # update execution
        # We need only one working subscription, because the result will be shared with all subscribers
        with_redis do |redis|
          subscription_ids.each do |sid|
            next unless redis.exists?(redis_key(SUBSCRIPTION_PREFIX) + sid)

            result = execute_update(sid, event, object)

            break if result
          end
        end

        return unless result

        # Having calculated the result _once_, send the same payload to all subscribers
        deliver(redis_key(SUBSCRIPTIONS_PREFIX) + fingerprint, result)
      end

      # Disable this method as there is no fingerprint (it can be retrieved from subscription though)
      def execute(subscription_id, event, object)
        raise NotImplementedError, "Use execute_all method instead of execute to get actual event fingerprint"
      end

      # This subscription was re-evaluated.
      # Send it to the specific stream where this client was waiting.
      # @param strean_key [String]
      # @param result [#to_h] result to send to clients
      def deliver(stream_key, result)
        payload = {result: result.to_h, more: true}.to_json
        broadcast(stream_key, payload)
      end

      # Save query to "storage" (in redis)
      def write_subscription(query, events)
        context = query.context.to_h
        subscription_id = context.delete(:subscription_id) || build_id
        channel = context.delete(:channel)

        raise GraphQL::AnyCable::ChannelConfigurationError unless channel

        # Store subscription_id in the channel state to cleanup on disconnect
        write_subscription_id(channel, subscription_id)

        events.each do |event|
          channel.stream_from(redis_key(SUBSCRIPTIONS_PREFIX) + event.fingerprint)
        end

        data = {
          query_string: query.query_string,
          variables: query.provided_variables.to_json,
          context: @serializer.dump(context.to_h),
          operation_name: query.operation_name.to_s,
          events: events.map { |e| [e.topic, e.fingerprint] }.to_h.to_json
        }

        with_redis do |redis|
          redis.multi do |pipeline|
            pipeline.sadd(redis_key(CHANNEL_PREFIX) + subscription_id, [subscription_id])
            pipeline.mapped_hmset(redis_key(SUBSCRIPTION_PREFIX) + subscription_id, data)
            events.each do |event|
              pipeline.zincrby(redis_key(FINGERPRINTS_PREFIX) + event.topic, 1, event.fingerprint)
              pipeline.sadd(redis_key(SUBSCRIPTIONS_PREFIX) + event.fingerprint, [subscription_id])
            end
            next unless config.subscription_expiration_seconds
            pipeline.expire(redis_key(CHANNEL_PREFIX) + subscription_id, config.subscription_expiration_seconds)
            pipeline.expire(redis_key(SUBSCRIPTION_PREFIX) + subscription_id, config.subscription_expiration_seconds)
          end
        end
      end

      # Return the query from "storage" (in redis)
      def read_subscription(subscription_id)
        with_redis do |redis|
          redis.mapped_hmget(
            "#{redis_key(SUBSCRIPTION_PREFIX)}#{subscription_id}",
            :query_string, :variables, :context, :operation_name
          ).then do |subscription|
            # Redis returns hash with all nils for missing key
            return nil if subscription.values.all?(&:nil?)
            # query_string is a required field for executing a subscription, so we should be sure that it exists
            return nil if subscription[:query_string].nil?

            subscription[:context] = @serializer.load(subscription[:context])
            subscription[:variables] = JSON.parse(subscription[:variables])
            subscription[:operation_name] = nil if subscription[:operation_name].to_s.strip == ""

            subscription
          end
        end
      end

      # The channel was closed, forget about it and its subscriptions
      def delete_channel_subscriptions(channel)
        raise(ArgumentError, "Please pass channel instance to #{__method__} in your #unsubscribed method") if channel.is_a?(String)

        channel_id = read_subscription_id(channel)

        # Missing in case disconnect happens before #execute
        return unless channel_id

        with_redis do |redis|
          redis.smembers(redis_key(CHANNEL_PREFIX) + channel_id).each do |subscription_id|
            delete_subscription(subscription_id, redis: redis)
          end
          redis.del(redis_key(CHANNEL_PREFIX) + channel_id)
        end
      end

      def delete_subscription(subscription_id, redis: AnyCable.redis)
        events = redis.hget(redis_key(SUBSCRIPTION_PREFIX) + subscription_id, :events)
        events = events ? JSON.parse(events) : {}
        fingerprint_subscriptions = {}
        redis.pipelined do |pipeline|
          events.each do |topic, fingerprint|
            pipeline.srem(redis_key(SUBSCRIPTIONS_PREFIX) + fingerprint, subscription_id)
            score = pipeline.zincrby(redis_key(FINGERPRINTS_PREFIX) + topic, -1, fingerprint)
            fingerprint_subscriptions[redis_key(FINGERPRINTS_PREFIX) + topic] = score
          end
          # Delete subscription itself
          pipeline.del(redis_key(SUBSCRIPTION_PREFIX) + subscription_id)
        end
        # Clean up fingerprints that doesn't have any subscriptions left
        redis.pipelined do |pipeline|
          fingerprint_subscriptions.each do |key, score|
            pipeline.zremrangebyscore(key, "-inf", "0") if score.value.zero?
          end
        end
      end

      private

      def read_subscription_id(channel)
        return channel.instance_variable_get(:@__sid__) if channel.instance_variable_defined?(:@__sid__)

        istate = fetch_channel_istate(channel)

        return unless istate

        channel.instance_variable_set(:@__sid__, istate["sid"])
      end

      def write_subscription_id(channel, val)
        channel.connection.anycable_socket.istate["sid"] = val
        channel.instance_variable_set(:@__sid__, val)
      end

      def fetch_channel_istate(channel)
        # For Rails integration
        return channel.__istate__ if channel.respond_to?(:__istate__)

        return unless channel.connection.socket.istate

        if channel.connection.socket.istate[channel.identifier]
          JSON.parse(channel.connection.socket.istate[channel.identifier])
        else
          channel.connection.socket.istate
        end
      end

      def redis_key(prefix)
        "#{config.redis_prefix}-#{prefix}"
      end
    end
  end
end
# rubocop: enable Metrics/AbcSize, Metrics/LineLength, Metrics/MethodLength
