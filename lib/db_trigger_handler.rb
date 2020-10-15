require "db_trigger_handler/version"
require "helpers/sql"
require 'handlers/shipment_handler'

module DbTriggerHandler
  include SQL
  include ShipmentHandler

  class << self
    def init(active_record_base)
      return if active_record_base.blank?
      @active_record_base = active_record_base
      execute
    end

    private
    def execute
      Thread.new do
        begin
          @active_record_base.connection_pool.with_connection do |connection|
            @connection = connection
            subscribe
            listen
          end
        ensure
          @active_record_base.clear_active_connections!
        end
      end
    end

    def subscribe
      notification_channels.each do |channel|
        SQL.subscribe_channel(@connection, channel)
      end
    end

    def listen
      begin
        loop do
          @connection.raw_connection.wait_for_notify do |event, id, data|
            case event
            when 'shipment_create'
              ShipmentHandler.shipment_create_handler(@connection, data)
            when 'shipment_updated'
              ShipmentHandler.shipment_create_handler(@connection, data)
            end
          end
        end
      ensure
        unsubscribe
      end
    end

    def unsubscribe
      notification_channels.each do |channel|
        SQL.unsubscribe_channel(@connection, channel)
      end
    end

    def notification_channels
      %w[shipment_created shipment_updated dpir_updated trigger_failed]
    end
  end
end
