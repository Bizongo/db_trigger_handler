require 'helpers/sql'

module ShipmentHandler
  include SQL

  class << self
    @lead_plus_account_pan_mapping = {
        54 => 'AAECH3221K'
    }

    def shipment_create_handler(connection, data)
      parsed_data = JSON.parse data
      shipment_create_data = SQL.get_all_shipment_info(connection, parsed_data['id'])
      if [0,2,4].include? shipment_create_data[:dispatch_plan]['dispatch_mode']
        # Create Invoice For seller_to_buyer, warehouse_to_warehouse, warehouse_to_buyer
        create_invoice shipment_create_data
      end
    end

    private

    def create_invoice data
      @sku_codes = []
      @account_name = ""
      @entity_reference_number = ""
      @center_id = nil
      invoice_creation_data = {
        invoice_date: Date.today.strftime("%Y-%m-%d"),
        file: "",
        account_type: "BUYER",
        pan: get_pan(data['dispatch_plan']),
        amount: data[:shipment]['total_buyer_invoice_amount'].to_f - data[:shipment]['total_buyer_service_charge'].to_f,
        line_item_details: get_line_item_details(data),
        buyer_details: get_buyer_company_details(data),
        account_name: @account_name,
        entity_reference_number: @entity_reference_number,
        centre_reference_id: @center_id,
        ship_to_details: get_address_object(data[:dispatch_plan]['destination_address_snapshot']),
        dispatch_from_details: get_address_object(data[:dispatch_plan]['origin_address_snapshot']),
        supporting_document_details: get_supporting_document_details(data),
        delivery_amount: data[:shipment]['total_buyer_service_charge'],
        shipment_id: data[:shipment]['id']
      }
      pp invoice_creation_data.to_json
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
        @sku_codes << product_details['sku_code']
      end
      line_item_details
    end

    def get_buyer_company_details data
      address = data[:dispatch_plan]['destination_address_snapshot']
      address = JSON.parse address
      @center_id = address['center_id']
      if [0,2].include? data[:dispatch_plan]['dispatch_mode']
        buyer_company_snapshot = JSON.parse data[:dispatch_plan]['buyer_company_snapshot']
        @entity_reference_number = buyer_company_snapshot['purchase_order_no']
        @center_id = buyer_company_snapshot['center_id']
        address = buyer_company_snapshot['billing_address']
      end
      @account_name = address['company_name']
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
          name: seller_company['name'],
          company_name: seller_company['name'],
          gstin: data[:transition_address]['gstin'],
          email_id: seller_company['primary_contact']['email'],
          contact_number: seller_company['promary_contact']['mobile'],
          address: {
              street_address: "#{data[:transition_address]['street_address']} #{data[:transition_address]['city']} - #{data[:transition_address]['pincode']}",
              pincode: data[:transition_address]['pincode'],
              state: data[:transition_address]['state'],
              country:  data[:transition_address]['country'],
              state_code: data[:transition_address]['gstin_state_code']
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

    def get_supporting_document_details data
      stock_transfer = [4].include? data[:dispatch_plan]['dispatch_mode']
      product_details = JSON.parse data[:dispatch_plan_item_relations].first['product_details']
      {
          sku_codes: @sku_codes,
          stock_transfer: stock_transfer,
          international_shipment: data[:shipment]['international_shipment'],
          include_tax: product_details['include_tax'],
          currency_symbol: product_details['currency']
      }
    end

    def get_pan data
      pan = ""
      buyer_company_snapshot = JSON.parse data[:dispatch_plan]['buyer_company_snapshot']
      destination_address = JSON.parse data[:dispatch_plan]['destination_address_snapshot']
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
