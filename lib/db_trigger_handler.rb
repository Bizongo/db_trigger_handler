require "db_trigger_handler/version"

module DbTriggerHandler
  included do
    before_action :set_event_listener
  end

  def set_event_listener(base)
    Rails.logger.info "Base :- #{base}"
    Rails.logger.info "Base :- #{notification_channels}"
  end

  private
  def notification_channels
    %w[SHIPMENT_CREATED SHIPMENT_CHANGED DPIR_CHANGED]
  end
end
