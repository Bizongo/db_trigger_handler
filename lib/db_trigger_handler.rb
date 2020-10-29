require "db_trigger_handler/version"
require "helpers/sql"
require 'handlers/shipment_handler'
require 'handlers/dpir_handler'

module DbTriggerHandler
  include SQL
  include ShipmentHandler
  include DpirHandler

  class << self
    def init(active_record_base, logger)
      return if active_record_base.blank?
      @active_record_base = active_record_base
      logger.info "DbTriggerGemInfo :- init"
      execute
    end

    private
    def execute
      Thread.new do
        logger.info "DbTriggerGemInfo :- init thread"
        begin
          @active_record_base.connection_pool.with_connection do |connection|
            @connection = connection
            subscribe
            listen
          end
        ensure
          logger.info "DbTriggerGemInfo :- exit thread"
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
          logger.info "DbTriggerGemInfo :- in loop"
          @connection.raw_connection.wait_for_notify do |event, id, data|
            case event
            when 'shipment_created'
              ShipmentHandler.shipment_create_handler(@connection, data)
            when 'shipment_cancelled'
              ShipmentHandler.shipment_cancelled(@connection, data)
            when 'shipment_updated'
              ShipmentHandler.shipment_updated(@connection, data)
            when 'shipment_dpir_changed'
              ShipmentHandler.shipment_dpir_transaction_handler(@connection, data)
            when 'dpir_updated'
              DpirHandler.handle_dpir_change(@connection, data)
            end
          end
        end
      ensure
        logger.info "DbTriggerGemInfo :- exit thread"
        unsubscribe
      end
    end

    def unsubscribe
      notification_channels.each do |channel|
        SQL.unsubscribe_channel(@connection, channel)
      end
    end

    def notification_channels
      %w[shipment_created shipment_dpir_changed shipment_cancelled shipment_updated dpir_updated]
    end
  end
end
