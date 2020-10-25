require 'helpers/sql'
require 'helpers/kafka_helper'

module DpirHandler
  include SQL
  include KafkaHelper

  class << self
    @lead_plus_account_pan_mapping = {
        54 => 'AAECH3221K'
    }

    def handle_dpir_change(connection, data)
      parsed_data = JSON.parse data
      dpir_update_data = SQL.get_all_dpir_info(connection, parsed_data['id'])
      if [0,2].include? dpir_update_data[:dispatch_plan]['dispatch_mode']
        @note_type = 'CREDIT_NOTE'
        @type = parsed_data['type']
        case parsed_data['type']
        when 'SHIPPED_QUANTITY_CHANGE'
          shipped_quantity_change(dpir_update_data, parsed_data['old'].to_f)
        when 'RETURNED_QUANTITY_CHANGE'
          returned_quantity_change(dpir_update_data, parsed_data['old'].to_f)
        when 'LOST_QUANTITY_CHANGE'
          lost_quantity_change(dpir_update_data, parsed_data['old'].to_f)
        when 'PRICE_PER_UNIT_CHANGE'
          price_per_unit_change(dpir_update_data, parsed_data['old'].to_f)
        when 'GST_CHANGE'
          gst_percentage_change(dpir_update_data, parsed_data['old'].to_f)
        end
      end
    end

    private

    def get_line_item_details(dpir, old)
      product_details = JSON.parse dpir['product_details']
      price_per_unit = product_details['order_price_per_unit']
      gst_percentage = product_details['order_item_gst']
      quantity = dpir['shipped_quantity']
      case @type
      when 'SHIPPED_QUANTITY_CHANGE'
        quantity = (dpir['shipped_quantity'].to_f-old.to_f).abs
      when 'RETURNED_QUANTITY_CHANGE'
        quantity = (dpir['returned_quantity'].to_f-old.to_f).abs
      when 'LOST_QUANTITY_CHANGE'
        quantity = (dpir['lost_quantity'].to_f-old.to_f).abs
      when 'PRICE_PER_UNIT_CHANGE'
        price_per_unit = (price_per_unit.to_f-old.to_f).abs
      when 'GST_CHANGE'
        gst_percentage = (gst_percentage.to_f-old.to_f).abs
      end
      amount_without_tax = quantity.to_f * price_per_unit.to_f
      @amount = quantity * price_per_unit * (1+(gst_percentage/100))
      [{
          item_name: product_details['product_name'],
          hsn: product_details['hsn_number'],
          dispatch_plan_item_relation_id: dpir['id'],
          quantity: quantity,
          price_per_unit: price_per_unit,
          gst_percentage: gst_percentage,
          amount_without_tax: amount_without_tax
      }]
    end

    def shipped_quantity_change(dpir_update_data, old_shipped_quantity)
      creation_data = common_create_invoice_data(dpir_update_data, old_shipped_quantity)
                          .merge!({
        type: @note_type
      })
      pp creation_data
    end

    def returned_quantity_change(dpir_update_data, old_returned_quantity)
      creation_data = common_create_invoice_data(dpir_update_data, old_returned_quantity)
                          .merge!({
        type: @note_type
      })
      pp creation_data
    end

    def lost_quantity_change(dpir_update_data, old_lost_quantity)
      creation_data = common_create_invoice_data(dpir_update_data, old_lost_quantity)
                          .merge!({
        type: @note_type
      })
      pp creation_data
    end

    def price_per_unit_change(dpir_update_data, old_ppu)
      creation_data = common_create_invoice_data(dpir_update_data, old_ppu)
                          .merge!({
                                      type: @note_type
                                  })
      pp creation_data
    end

    def gst_percentage_change(dpir_update_data, old_gst)
      creation_data = common_create_invoice_data(dpir_update_data, old_gst)
                          .merge!({
        type: @note_type
      })
      pp creation_data
    end

    def common_create_invoice_data(data, old)
      @sku_codes = []
      @account_name = ""
      @entity_reference_number = ""
      @center_id = nil
      @buyer_gstin_state_code = ""
      @amount = 0
      {
          invoice_date: Date.today.strftime("%Y-%m-%d"),
          file: "",
          account_type: "BUYER",
          pan: get_pan(data[:dispatch_plan]),
          buyer_details: get_buyer_company_details(data),
          supplier_details: get_seller_comapny_details(data),
          account_name: @account_name,
          entity_reference_number: @entity_reference_number,
          centre_reference_id: @center_id,
          ship_to_details: get_address_object(data[:dispatch_plan]['destination_address_snapshot']),
          dispatch_from_details: get_address_object(data[:dispatch_plan]['origin_address_snapshot']),
          supporting_document_details: get_supporting_document_details(data),
          shipment_id: data[:shipment]['id'],
          line_item_details: get_line_item_details(data[:dispatch_plan_item_relation], old),
          amount: @amount
      }
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
      product_details = JSON.parse data[:dispatch_plan_item_relation]['product_details']
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
