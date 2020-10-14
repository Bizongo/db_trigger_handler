require "db_trigger_handler/version"

module DbTriggerHandler
  class << self
    def init
      Rails.logger.info "Base :- #{base}"
      Rails.logger.info "Base :- #{notification_channels}"
    end

    private
    def notification_channels
      %w[SHIPMENT_CREATED SHIPMENT_CHANGED DPIR_CHANGED]
    end
  end
end
