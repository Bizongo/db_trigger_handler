require 'helpers/sql'
require 'helpers/kafka_helper'
require 'helpers/invoice_creation_helper'

module DpirHandler
  include SQL
  include KafkaHelper
  include InvoiceCreationHelper

  class << self
    def handle_dpir_change(connection, data, logger)
      parsed_data = JSON.parse data
      dpir_update_data = SQL.get_all_dpir_info(connection, parsed_data['id'])
      if [0,2].include? dpir_update_data[:dispatch_plan]['dispatch_mode']
        @note_type = 'CREDIT_NOTE'
        @note_sub_type = ''
        @type = parsed_data['type']
        common_data = InvoiceCreationHelper.common_create_invoice_data dpir_update_data
        creation_data = add_information(dpir_update_data, common_data, parsed_data['old'])
        KafkaHelper::Client.produce(message: creation_data, topic: 'shipment_created', logger: logger)
      end
    end

    private

    def add_information(data, common_data, old)
      common_data.merge!({
        line_item_details: get_line_item_details(data[:dispatch_plan_item_relation], old),
        amount: @amount,
        type: @note_type + @note_sub_type,
        supporting_document_details: get_supporting_document_details(data),
        invoice_id_for_note: data[:shipment]['buyer_invoice_id']
      })
    end

    def get_line_item_details(dpir, old)
      product_details = JSON.parse dpir['product_details']
      price_per_unit = product_details['order_price_per_unit']
      gst_percentage = product_details['order_item_gst']
      quantity =
          dpir['shipped_quantity'].to_f - dpir['returned_quantity'].to_f - dpir['lost_quantity'].to_f
      ppu_difference = 0
      gst_difference = 0
      case @type
      when 'SHIPPED_QUANTITY_CHANGE'
        quantity = (dpir['shipped_quantity'].to_f-old.to_f).abs
        get_note_type(dpir['shipped_quantity'], old)
      when 'RETURNED_QUANTITY_CHANGE'
        quantity = (dpir['returned_quantity'].to_f-old.to_f).abs
        get_note_type(old, dpir['returned_quantity'])
      when 'LOST_QUANTITY_CHANGE'
        quantity = (dpir['lost_quantity'].to_f-old.to_f).abs
        get_note_type(old, dpir['lost_quantity'])
      when 'PRICE_PER_UNIT_CHANGE'
        new_price_per_unit = price_per_unit.to_f
        price_per_unit = (price_per_unit.to_f-old.to_f).abs
        ppu_difference = price_per_unit
        get_note_type(new_price_per_unit, old)
        @note_sub_type = '_RATE_CHANGE_SYSTEM'
      when 'GST_CHANGE'
        new_gst_percentage = gst_percentage.to_f
        gst_percentage = (gst_percentage.to_f-old.to_f).abs
        gst_difference = gst_difference
        get_note_type(new_gst_percentage, old)
        @note_sub_type = '_TAX_CHANGE_SYSTEM'
      end
      amount_without_tax = quantity.to_f * price_per_unit.to_f
      @amount = quantity * price_per_unit * ( (@type=='GST_CHANGE'? 0:1)+(gst_percentage.to_f/100))
      line_item_data = {
          item_name: product_details['product_name'],
          hsn: product_details['hsn_number'],
          dispatch_plan_item_relation_id: dpir['id'],
          quantity: quantity,
          price_per_unit: price_per_unit,
          tax_percentage: gst_percentage,
          amount_without_tax: amount_without_tax,
          ppu_difference: ppu_difference,
          gst_difference: gst_difference,
       }
      if ['GST_CHANGE', 'PRICE_PER_UNIT_CHANGE'].include?(@type)
        line_item_data.merge!({
          new_rate_per_unit: product_details['order_price_per_unit'],
          new_gst_percentage: product_details['order_item_gst'],
          old_rate_per_unit: @type == 'PRICE_PER_UNIT_CHANGE' ? old.to_f : product_details['order_price_per_unit'],
          old_gst_percentage: @type == 'GST_CHANGE' ? old.to_f : product_details['order_item_gst']
        })
      end
      [line_item_data]
    end

    def get_note_type(new, old)
      if new.to_f < old.to_f
        @note_type = 'CREDIT_NOTE'
      else
        @note_type = 'DEBIT_NOTE'
      end
    end

    def get_comment
      case @note_sub_type
      when ''
        return ''
      when  '_RATE_CHANGE_SYSTEM'
        return 'Rate change for product'
      when '_TAX_CHANGE_SYSTEM'
        return 'Tax changed for product'
      else
        return ''
      end
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
          invoice_using_igst: InvoiceCreationHelper.get_if_igst_required(data),
          comment: get_comment
      }
    end
  end
end
