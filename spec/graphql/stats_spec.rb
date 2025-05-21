# frozen_string_literal: true

RSpec.describe GraphQL::AnyCable::Stats do
  describe "#collect" do
    let(:created_query) do
      <<~GRAPHQL
        subscription ProductCreatedSubscription {
          productCreated { id title }
        }
      GRAPHQL
    end

    let(:updated_query) do
      <<~GRAPHQL
        subscription ProductUpdatedSubscription {
          productUpdated { id }
        }
      GRAPHQL
    end

    let(:channel) do
      socket = double("Socket", istate: AnyCable::Socket::State.new({}))
      connection = double("Connection", anycable_socket: socket)
      double("Channel", id: "legacy_id", params: {"channelId" => "legacy_id"}, stream_from: nil, connection: connection)
    end

    let(:subscription_id) do
      "some-truly-random-number"
    end

    before do
      AnycableSchema.execute(
        query: created_query,
        context: {channel: channel, subscription_id: "#{subscription_id}-created"},
        variables: {},
        operation_name: "ProductCreatedSubscription"
      )

      AnycableSchema.execute(
        query: updated_query,
        context: {channel: channel, subscription_id: "#{subscription_id}-updated"},
        variables: {},
        operation_name: "ProductUpdatedSubscription"
      )
    end

    context "when include_subscriptions is false" do
      let(:expected_result) do
        {total: {subscription: 2, fingerprints: 2, subscriptions: 2, channel: 2}}
      end

      it "returns total stat" do
        expect(subject.collect).to eq(expected_result)
      end
    end

    context "when include_subscriptions is true" do
      subject { described_class.new(include_subscriptions: true) }

      let(:expected_result) do
        {
          total: {subscription: 2, fingerprints: 2, subscriptions: 2, channel: 2},
          subscriptions: {
            "productCreated" => 1,
            "productUpdated" => 1
          }
        }
      end

      it "returns total stat with grouped subscription stats" do
        expect(subject.collect).to eq(expected_result)
      end
    end
  end
end
