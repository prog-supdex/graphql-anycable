# frozen_string_literal: true

RSpec.describe "Broadcasting" do
  def subscribe(query)
    BroadcastSchema.execute(
      query: query,
      context: {channel: channel},
      variables: {},
      operation_name: "SomeSubscription"
    )
  end

  let(:channel) do
    socket = double("Socket", istate: AnyCable::Socket::State.new({}))
    connection = double("Connection", anycable_socket: socket)
    double("Channel", connection: connection)
  end

  let(:object) do
    double("Post", id: 1, title: "Broadcasting…", actions: %w[Edit Delete])
  end

  let(:query) do
    <<~GRAPHQL.strip
      subscription SomeSubscription { postCreated{ id title } }
    GRAPHQL
  end

  before do
    allow(channel).to receive(:stream_from)
    allow(AnyCable).to receive(:broadcast)
  end

  context "when all clients asks for broadcastable fields only" do
    let(:query) do
      <<~GRAPHQL.strip
        subscription SomeSubscription { postCreated{ id title } }
      GRAPHQL
    end

    it "uses broadcasting to resolve query only once" do
      2.times { subscribe(query) }
      BroadcastSchema.subscriptions.trigger(:post_created, {}, object)
      expect(object).to have_received(:title).once
      expect(AnyCable).to have_received(:broadcast).once
    end
  end

  context "when all clients asks for non-broadcastable fields" do
    let(:query) do
      <<~GRAPHQL.strip
        subscription SomeSubscription { postCreated{ id title actions } }
      GRAPHQL
    end

    it "resolves query for every client" do
      2.times { subscribe(query) }
      BroadcastSchema.subscriptions.trigger(:post_created, {}, object)
      expect(object).to have_received(:title).twice
      expect(AnyCable).to have_received(:broadcast).twice
    end
  end

  context "when one of subscriptions got expired" do
    let(:query) do
      <<~GRAPHQL.strip
        subscription SomeSubscription { postCreated{ id title } }
      GRAPHQL
    end

    let(:redis) { $redis }

    it "doesn't fail" do
      3.times { subscribe(query) }
      redis.keys("graphql-subscription:*").last.tap(&redis.method(:del))
      expect(redis.keys("graphql-subscription:*").size).to eq(2)
      expect { BroadcastSchema.subscriptions.trigger(:post_created, {}, object) }.not_to raise_error
      expect(object).to have_received(:title).once
      expect(AnyCable).to have_received(:broadcast).once
    end
  end

  context "when handling race conditions with subscription deleted between checks" do
    let(:query) do
      <<~GRAPHQL.strip
        subscription SomeSubscription { postCreated{ id } }
      GRAPHQL
    end

    let(:redis) { $redis }
    let(:object) { double("Post", id: 1, title: "Racing") }
    let(:fingerprint) { ":postCreated:/SomeSubscription/race-condition-test/0/signature456=" }
    let(:subscriptions) { BroadcastSchema.subscriptions }

    before do
      allow_any_instance_of(GraphQL::Subscriptions::Event).to receive(:fingerprint).and_return(fingerprint)

      3.times { subscribe(query) }

      @subscription_ids = redis.smembers("graphql-subscriptions:#{fingerprint}")
      expect(@subscription_ids.size).to eq(3)

      # Emulate removing a subscription like race condition
      allow(subscriptions).to receive(:read_subscription).and_wrap_original do |original, sid|
        # Remove first subscription after `checking existing`, but before the read_subscription
        if sid == @subscription_ids.first
          redis.del("graphql-subscription:#{sid}")

          nil
        else
          original.call(sid)
        end
      end

      allow(AnyCable).to receive(:broadcast)
    end

    it "handles subscription deleted between exists? check and read_subscription" do
      subscriptions.execute_grouped(
        fingerprint,
        @subscription_ids,
        GraphQL::Subscriptions::Event.new(
          name: "postCreated",
          arguments: {},
          field: BroadcastSchema.subscription.fields["postCreated"],
          scope: nil,
          context: {}
        ),
        object
      )

      # We must get broadcast here, because if the first subscription expired, we should process the rest of subscriptions
      expect(AnyCable).to have_received(:broadcast).once
      expect(AnyCable).to have_received(:broadcast).with("graphql-subscriptions:#{fingerprint}", anything)
    end

    it "returns without broadcasting when all subscriptions were deleted between checks" do
      # read_subscription always returns nil
      allow(subscriptions).to receive(:read_subscription).and_return(nil)

      subscriptions.execute_grouped(
        fingerprint,
        @subscription_ids,
        GraphQL::Subscriptions::Event.new(
          name: "postCreated",
          arguments: {},
          field: BroadcastSchema.subscription.fields["postCreated"],
          scope: nil,
          context: {}
        ),
        object
      )

      expect(AnyCable).not_to have_received(:broadcast)
    end
  end
end
