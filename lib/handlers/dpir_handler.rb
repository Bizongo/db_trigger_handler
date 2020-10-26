require 'helpers/sql'
require 'helpers/kafka_helper'
require 'helpers/invoice_creation_helper'

module DpirHandler
  include SQL
  include KafkaHelper
  include InvoiceCreationHelper

  class << self
    def handle_dpir_change(connection, data)
      parsed_data = JSON.parse data
      dpir_update_data = SQL.get_all_dpir_info(connection, parsed_data['id'])
      if [0,2].include? dpir_update_data[:dispatch_plan]['dispatch_mode']
        @note_type = 'CREDIT_NOTE'
        @type = parsed_data['type']
        common_data = InvoiceCreationHelper.common_create_invoice_data dpir_update_data
        creation_data = add_information(dpir_update_data, common_data, parsed_data['old'])
        pp "dpir changed"
        pp creation_data.to_json
      end
    end

    private

    def add_information(data, common_data, old)
      common_data.merge!({
        line_item_details: get_line_item_details(data[:dispatch_plan_item_relation], old),
        amount: @amount,
        type: @note_type,
        supporting_document_details: get_supporting_document_details(data)
      })
    end

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
  end
end
