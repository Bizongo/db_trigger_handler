require 'helpers/sql'
require 'helpers/kafka_helper'

module ShipmentHandler
  include SQL
  include KafkaHelper

  class << self
    @lead_plus_account_pan_mapping = {
        54 => 'AAECH3221K'
    }

    def shipment_create_handler(connection, data)
      parsed_data = JSON.parse data
      shipment_create_data = SQL.get_all_shipment_info(connection, parsed_data['id'])
      if [0,2,4].include? shipment_create_data[:dispatch_plan]['dispatch_mode']
        # Create Invoice For seller_to_buyer, warehouse_to_warehouse, warehouse_to_buyer
        KafkaHelper::Client.produce(message: create_invoice(shipment_create_data),
                                    topic: "shipment_created")
      elsif [3,6].include? shipment_create_data[:dispatch_plan]['dispatch_mode']
        # Create Invoice for buyer_to_warehouse, buyer_to_seller
        forward_shipment = SQL.get_shipment(connection, shipment_create_data[:shipment]['forward_shipment_id']);
        message = create_invoice(shipment_create_data)
        message.merge!({
          invoice_id_for_note: forward_shipment['buyer_invoice_id'],
          type: 'CREDIT_NOTE',
          buyer_details: message[:seller_details],
          seller_details: message[:buyer_details]
        })
        KafkaHelper::Client.produce(message: message, topic: "shipment_created")
      end
    end

    def shipment_cancelled(connection, data)
      parse_data = JSON.parse data
      shipment = SQL.get_shipment(connection, parse_data['id'])
      # Cancel Invoice if shipment is cancelled or deleted
      if shipment['buyer_invoice_id'].present?
        pp cancel_invoice(shipment['buyer_invoice_id'])
      end
      if shipment['seller_invoice_id'].present?
        pp cancel_invoice(shipment['seller_invoice_id'])
      end
    end

    def shipment_updated(connection, data)
      parse_data = JSON.parse data
      shipment = SQL.get_shipment(connection, parse_data['id'])
      pp "Invoice Update"
      if shipment['status'] == 3
        if shipment['seller_invoice_id'].present?
          update_invoice_data = {status: 'CANCELLED', id: shipment['seller_invoice_id']}
          KafkaHelper::Client.produce(message: update_invoice_data, topic: "shipment_updated")
        end
        if shipment['buyer_invoice_id'].present?
          update_invoice_data = {status: 'CANCELLED', id: shipment['buyer_invoice_id']}
          KafkaHelper::Client.produce(message: update_invoice_data, topic: "shipment_updated")
        end
      else
        if shipment['seller_invoice_id'].present?
          update_invoice_data = update_invoice(shipment)
          if shipment['seller_due_data'].present?
            update_invoice_data.merge!({due_date: shipment['seller_due_date'].strftime("%Y-%m-%d")})
          end
        end
        KafkaHelper::Client.produce(message: update_invoice_data, topic: "shipment_updated")
      end
    end

    private

    def update_invoice shipment
      {
          invoice_number: shipment['seller_invoice_number'],
          amount: shipment['total_seller_invoice_amount'].to_f - shipment['actual_charges'].to_f,
          delivery_amount: shipment['actual_charges'].to_f,
          extra_amount: shipment['seller_extra_charges'].to_f,
          id: shipment['seller_invoice_id']
      }
    end

    def cancel_invoice id
      {
          status: 'CANCELLED',
          id: id
      }
    end

    def create_invoice data
      @sku_codes = []
      @account_name = ""
      @entity_reference_number = ""
      @center_id = nil
      @buyer_gstin_state_code = ""
      invoice_creation_data = {
        invoice_date: Date.today.strftime("%Y-%m-%d"),
        file: "",
        account_type: "BUYER",
        pan: get_pan(data[:dispatch_plan]),
        amount: data[:shipment]['total_buyer_invoice_amount'].to_f - data[:shipment]['total_buyer_service_charge'].to_f,
        line_item_details: get_line_item_details(data),
        buyer_details: get_buyer_company_details(data),
        supplier_details: get_seller_comapny_details(data),
        account_name: @account_name,
        entity_reference_number: @entity_reference_number,
        centre_reference_id: @center_id,
        ship_to_details: get_address_object(data[:dispatch_plan]['destination_address_snapshot']),
        dispatch_from_details: get_address_object(data[:dispatch_plan]['origin_address_snapshot']),
        supporting_document_details: get_supporting_document_details(data),
        delivery_amount: data[:shipment]['total_buyer_service_charge'],
        shipment_id: data[:shipment]['id']
      }
      pp "Invoice Creation"
      invoice_creation_data
    end

    def get_line_item_details data
      line_item_details = []
      data[:dispatch_plan_item_relations].each do |dpir|
        product_details = JSON.parse dpir['product_details']
        if [0,2,3,6].include? data[:dispatch_plan]['dispatch_mode']
          price_per_unit = product_details['order_price_per_unit']
          gst_percentage = product_details['order_item_gst']
        else
          price_per_unit = product_details['price_per_unit']
          gst_percentage = product_details['child_item_gst']
        end
        line_item_details << {
            item_name: product_details['product_name'],
            hsn: product_details['hsn_number'],
            quantity: dpir['shipped_quantity'].to_f,
            price_per_unit: price_per_unit,
            tax_percentage: gst_percentage,
            amount_without_tax: dpir['total_buyer_amount_without_tax'].to_f,
            dispatch_plan_item_relation_id: dpir['id']
        }
        @sku_codes << product_details['sku_code']
      end
      line_item_details
    end

    def get_buyer_company_details data
      address = data[:dispatch_plan]['destination_address_snapshot']
      address = JSON.parse address
      @center_id = address['center_id']
      if [0,2,3,6].include? data[:dispatch_plan]['dispatch_mode']
        buyer_company_snapshot = JSON.parse data[:dispatch_plan]['buyer_company_snapshot']
        @entity_reference_number = buyer_company_snapshot['purchase_order_no']
        @center_id = buyer_company_snapshot['center_id']
        address = buyer_company_snapshot['billing_address']
      end
      @account_name = address['company_name']
      @buyer_gstin_state_code = address['gstin_state_code']
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

    def get_seller_comapny_details data
      seller_company = data[:dispatch_plan]['seller_company_snapshot']
      seller_company = JSON.parse seller_company
      {
          name: seller_company['seller_company_name'],
          company_name: seller_company['seller_company_name'],
          gstin: data[:transition_address]['gstin'],
          email_id: seller_company['seller_primary_contact']['email'],
          contact_number: seller_company['seller_primary_contact']['mobile'],
          address: {
              street_address: "#{data[:transition_address]['street_address']} #{data[:transition_address]['city']} - #{data[:transition_address]['pincode']}",
              pincode: data[:transition_address]['pincode'],
              state: data[:transition_address]['state'],
              country:  data[:transition_address]['country'],
              state_code: data[:transition_address]['gstin_state_code']
          }
      }
    end

    def get_if_igst_required data
      seller_gstin_state_code = data[:transition_address]['gstin_state_code']
      buyer_gstin_state_code = @buyer_gstin_state_code
      return seller_gstin_state_code != buyer_gstin_state_code
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

    def get_supporting_document_details data
      stock_transfer = [4].include? data[:dispatch_plan]['dispatch_mode']
      product_details = JSON.parse data[:dispatch_plan_item_relations].first['product_details']
      {
          sku_codes: @sku_codes,
          stock_transfer: stock_transfer,
          international_shipment: data[:shipment]['international_shipment'],
          include_tax: product_details['include_tax'],
          currency_symbol: product_details['currency'],
          invoice_using_igst: get_if_igst_required(data)
      }
    end

    def get_pan data
      pan = ""
      buyer_company_snapshot = JSON.parse data['buyer_company_snapshot']
      destination_address = JSON.parse data['destination_address_snapshot']
      account_id = buyer_company_snapshot['billing_address']['lead_plus_account_id']
      billing_address = buyer_company_snapshot['billing_address']
      if account_id.present? && @lead_plus_account_pan_mapping[account_id.to_i].present?
        pan = @lead_plus_account_pan_mapping[account_id.to_i]
      elsif billing_address.present? && billing_address['gstin'].present?
        pan = billing_address['gstin'].gsub(/\s+/, "").squish.upcase[2..11]
      elsif billing_address.present? && billing_address['pan'].present?
        pan = billing_address['pan'].gsub(/\s+/, "").squish.upcase[2..11]
      elsif destination_address.present? && destination_address['gstin'].present?
        pan = destination_address['gstin'].gsub(/\s+/, "").squish.upcase[2..11]
      end
      pan
    end
  end
end
