require 'helpers/sql'

module ShipmentHandler
  include SQL

  class << self
    def shipment_create_handler(connection, data)
      parsed_data = JSON.parse data
      shipment_create_data = SQL.get_all_shipment_info(connection, parsed_data['id'])
      pp "Shipment Data :- #{shipment_create_data[:dispatch_plan]}"
      if [0,2,4].include? shipment_create_data[:dispatch_plan]['dispatch_mode']
        # Create Invoice For seller_to_buyer, warehouse_to_warehouse, warehouse_to_buyer
        create_invoice shipment_create_data
      end
    end

    private

    def create_invoice data
      invoice_creation_data = {
        invoice_date: Date.today.strftime("%Y-%m-%d"),
        # entity_reference_number:,
        file: "",
        account_type: "BUYER",
        # account_name:,
        # pan: get_pan(data['dispatch_plan']),
        # centre_reference_id:,
        amount: data[:shipment]['total_buyer_invoice_amount'].to_f - data[:shipment]['total_buyer_service_charge'].to_f,
        line_item_details: get_line_item_details(data),
        buyer_details: get_buyer_company_details(data),
        ship_to_details: get_address_object(data[:dispatch_plan]['destination_address_snapshot']),
        dispatch_from_details: get_address_object(data[:dispatch_plan]['origin_address_snapshot']),
        # supporting_document_details:,
        delivery_amount: data[:shipment]['total_buyer_service_charge'],
        shipment_id: data[:shipment]['id']
      }
      pp invoice_creation_data.inspect
    end

    def get_line_item_details data
      line_item_details = []
      data[:dispatch_plan_item_relations].each do |dpir|
        product_details = JSON.parse dpir['product_details']
        if [0,2].include? data[:dispatch_plan]['dispatch_mode']
          price_per_unit = product_details['order_price_per_unit']
          gst_percentage = product_details['order_item_gst']
        else
          price_per_unit = product_details['price_per_unit']
          gst_percentage = product_details['child_item_gst']
        end
        line_item_details << {
            item_name: product_details['product_name'],
            hsn: product_details['hsn_number'],
            quantity: dpir['shipped_quantity'],
            price_per_unit: price_per_unit,
            tax_percentage: gst_percentage,
            amount_without_tax: dpir['total_buyer_amount_without_tax'],
            dispatch_plan_item_relation_id: dpir['id']
        }
      end
      line_item_details
    end

    def get_buyer_company_details data
      address = data[:dispatch_plan]['destination_address_snapshot']
      if [0,2].include? data[:dispatch_plan]['dispatch_mode']
        buyer_company_snapshot = JSON.parse data[:dispatch_plan]['buyer_company_snapshot']
        address = buyer_company_snapshot['billing_address']
      end
      address = JSON.parse address
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
