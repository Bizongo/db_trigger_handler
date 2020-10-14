require "db_trigger_handler/version"
require "helpers/sql"

module DbTriggerHandler
  include SQL

  class << self
    def init(connection)
      return if connection.blank?
      @connection = connection
      subscribe
      listen
    end

    private
    def subscribe
      notification_channels.each do |channel|
        SQL.subscribe_channel(@connection, channel)
      end
    end

    def listen
      Thread.new do
        loop do
          @connection.raw_connection.wait_for_notify do |event, id, data|
            pp "MessageReceived :- #{event}, #{id}, #{data}"
          end
        end
      end
    end

    def notification_channels
      %w[SHIPMENT_CREATED SHIPMENT_CHANGED DPIR_CHANGED TRIGGER_FAILED]
    end
  end
end
