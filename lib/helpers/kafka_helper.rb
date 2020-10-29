require 'kafka'

module KafkaHelper
  class Client
    def self.client
      @client ||= Kafka.new(seed_brokers: "qa81.indopus.in:9092")
    end

    def self.async_produce(message:, topic:)
      kafka_producer = client.async_producer
      kafka_producer.produce(message.to_json, topic: topic)
      kafka_producer.deliver_messages
      kafka_producer.shutdown
    end

    def self.produce(message:, topic:, logger:)
      if logger.present?
        @logger.info "Producing Message :- #{topic}"
        @logger.info message.to_json
      end
      kafka_producer = client.producer
      kafka_producer.produce(message.to_json, topic: topic)
      kafka_producer.deliver_messages
      kafka_producer.shutdown
    end
  end
end