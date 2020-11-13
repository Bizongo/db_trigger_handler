require 'kafka'

module KafkaHelper
  class Client
    def self.client(kafka_broker)
      @client ||= Kafka.new(seed_brokers: kafka_broker)
    end

    def self.produce(message:, topic:, logger:, kafka_broker:)
      if logger.present?
        logger.info "Producing Message :- #{topic}"
        logger.info message.to_json
      end
      kafka_producer = client(kafka_broker).producer
      kafka_producer.produce(message.to_json, topic: topic)
      kafka_producer.deliver_messages
      kafka_producer.shutdown
    end
  end
end