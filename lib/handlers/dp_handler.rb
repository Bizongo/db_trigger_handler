require 'helpers/sql'
require 'helpers/kafka_helper'
require 'helpers/invoice_creation_helper'

module DpHandler
  include SQL
  include KafkaHelper
  include InvoiceCreationHelper

  class << self
    def handle_dp_updates(connection, data, logger, kafka_broker)
      parsed_data = JSON.parse data
      @type = parsed_data['type']
      if @type == 'BUYER_DETAILS_UPDATE'
        update_billing_address(connection, parsed_data, logger, kafka_broker)
      else @type == "DESTINATION_CHANGE"
      update_shipping_address(connection, parsed_data, logger, kafka_broker)
      end
    end

    private

    def update_shipping_address(connection, data, logger, kafka_broker)
      result = SQL.get_destination_address(connection, data['id'])
      shipment = SQL.get_shipment_from_dp(connection, data['id'])
      KafkaHelper::Client.produce(message: {
          id: shipment['buyer_invoice_id'],
          ship_to_details: get_address_object(result['destination_address_snapshot']),
      }, topic: 'shipment_updated', logger: logger, kafka_broker: kafka_broker)
    end

    def update_billing_address(connection, data, logger, kafka_broker)
      result = SQL.get_billing_address(connection, data['id'])
      buyer_company_snapshot = JSON.parse result['buyer_company_snapshot']
      old_buyer_company_snapshot = JSON.parse data['old']
      state_code_new = buyer_company_snapshot['billing_address']['gstin'][0..1]
      state_code_old = old_buyer_company_snapshot['gstin'][0..1]
      if state_code_new == state_code_old
        shipment = SQL.get_shipment_from_dp(connection, data['id'])
        KafkaHelper::Client.produce(message: {
            id: shipment['buyer_invoice_id'],
            buyer_details: get_buyer_details(buyer_company_snapshot)
        }, topic: 'shipment_updated', logger: logger, kafka_broker: kafka_broker)
      end
    end

    def get_buyer_details buyer_company_snapshot
      address = buyer_company_snapshot['billing_address']
      {
          name: address['full_name'],
          company_name: address['company_name'],
          gstin: address['gstin'],
          contact_number: address['mobile_number'],
          address: {
              street_address: "#{address['street_address']} #{address['city']} #{address['state']}",
              pincode: address['pincode'],
              state: address['state'],
              country: address['country'],
              state_code: address['gstin_state_code']
          }
      }
    end

    def get_address_object data
      data = JSON.parse data
      {
          name: data['full_name'],
          company_name: data['company_name'],
          street_address: "#{data['street_address']} #{data['city']} - #{data['pincode']}",
          pincode: data['pincode'],
          state: data['state'],
          country: data['country'],
          gstin: data['gstin'],
          mobile: data['mobile_number'],
          state_code: data['gstin_state_code']
      }
    end

  end

end

