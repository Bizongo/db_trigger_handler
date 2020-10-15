require 'helpers/sql'

module ShipmentHandler
  include SQL

  class << self
    def shipment_create_handler(connection, data)
      parsed_data = JSON.parse data
      shipment_create_data = SQL.get_all_shipment_info(connection, parsed_data['id'])
      pp shipment_create_data
    end
  end
end
