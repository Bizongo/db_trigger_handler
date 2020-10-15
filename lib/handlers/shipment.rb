module Shipment
  class << self
    def shipment_create_handler(connection, data)
      parsed_data = JSON.parse data
      pp "Message :- #{parsed_data[:id]}, #{parsed_data['id']}"
    end
  end
end
