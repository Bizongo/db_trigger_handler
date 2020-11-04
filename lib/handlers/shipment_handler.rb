require 'helpers/sql'
require 'helpers/kafka_helper'
require 'helpers/invoice_creation_helper'

module ShipmentHandler
  include SQL
  include KafkaHelper
  include InvoiceCreationHelper

  class << self
    def shipment_create_handler(connection, data, logger)
      parsed_data = JSON.parse data
      shipment_create_data = SQL.get_all_shipment_info(connection, parsed_data['id'])
      if [0,2,4].include? shipment_create_data[:dispatch_plan]['dispatch_mode']
        # Create Invoice For seller_to_buyer, warehouse_to_warehouse, warehouse_to_buyer
        KafkaHelper::Client.produce(message: create_invoice(shipment_create_data),
                                    topic: "shipment_created", logger: logger)
      elsif [3,6].include? shipment_create_data[:dispatch_plan]['dispatch_mode']
        # Create Invoice for buyer_to_warehouse, buyer_to_seller (non lost returns)
        forward_shipment = SQL.get_shipment(connection, shipment_create_data[:shipment]['forward_shipment_id']);
        actions = SQL.get_shipment_actions_by_id(connection, shipment_create_data[:shipment]['id'], 29)
        if actions.blank?
          message = create_invoice(shipment_create_data)
          message.merge!({
            invoice_id_for_note: forward_shipment['buyer_invoice_id'],
            type: 'CREDIT_NOTE'
          })
          KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger)
        end
      end
    end

    def shipment_dpir_transaction_handler(connection, data, logger)
      parsed_data = JSON.parse data
      shipment_lost_data = SQL.get_lost_shipment_info(connection, parsed_data['id'])
      if [0,2,4].include? shipment_lost_data[:dispatch_plan]['dispatch_mode']
        message = create_lost_shipment_credit_note(shipment_lost_data, parsed_data['is_debit_note'].present?)
        message.merge!({
          invoice_id_for_note: shipment_lost_data[:shipment]['buyer_invoice_id'],
          supporting_document_details: get_supporting_document_details(shipment_lost_data),
          type: parsed_data['is_debit_note'].present? ? 'DEBIT_NOTE' : 'CREDIT_NOTE'
        })
        KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger)
      end
    end

    def shipment_updated(connection, data, logger)
      parse_data = JSON.parse data
      shipment = SQL.get_shipment(connection, parse_data['id'])
      if shipment['status'] == 3
        if shipment['seller_invoice_id'].present?
          update_invoice_data = {status: 'CANCELLED', id: shipment['seller_invoice_id']}
          KafkaHelper::Client.produce(message: update_invoice_data, topic: "shipment_updated", logger: logger)
        end
        if shipment['buyer_invoice_id'].present?
          datetime = shipment['created_at'].to_datetime
          if Time.now - datetime < 24.hours
            update_invoice_data = {status: 'CANCELLED', id: shipment['buyer_invoice_id']}
            KafkaHelper::Client.produce(message: update_invoice_data, topic: "shipment_updated", logger: logger)
          else
            generate_cancel_credit_note(connection, shipment['id'], logger)
          end
        end
        if shipment['buyer_invoice_id'].blank? && shipment['seller_invoice_id'].blank?
          generate_return_cancel_debit_note(connection, shipment['id'], logger)
        end
      else
        if shipment['seller_invoice_id'].present?
          update_invoice_data = update_invoice(shipment)
          if shipment['seller_due_data'].present?
            update_invoice_data.merge!({due_date: shipment['seller_due_date'].strftime("%Y-%m-%d")})
          end
          KafkaHelper::Client.produce(message: update_invoice_data, topic: "shipment_updated", logger: logger)
        end
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

    def create_invoice data
      common_data = InvoiceCreationHelper.common_create_invoice_data(data)
      invoice_creation_data = common_data.merge!({
        amount: data[:shipment]['total_buyer_invoice_amount'].to_f - data[:shipment]['total_buyer_service_charge'].to_f,
        line_item_details: get_line_item_details(data),
        delivery_amount: data[:shipment]['total_buyer_service_charge'],
        supporting_document_details: get_supporting_document_details(data),
      })
      invoice_creation_data
    end

    def create_lost_shipment_credit_note(data, is_debit_note = false)
      common_data = InvoiceCreationHelper.common_create_invoice_data(data)
      invoice_creation_data = common_data.merge!({
        line_item_details: get_line_item_details(data, true, is_debit_note),
        amount: @amount
      })
      invoice_creation_data
    end

    def get_line_item_details(data, is_lost = false, is_debit_note = false)
      @amount = 0
      @sku_codes = []
      line_item_details = []
      lost_data = []
      if is_lost && is_debit_note
        lost_data = JSON.parse data[:shipment]['items_change_snapshot']
      end
      data[:dispatch_plan_item_relations].each do |dpir|
        product_details = JSON.parse dpir['product_details']
        if [0,2,3,6].include? data[:dispatch_plan]['dispatch_mode']
          price_per_unit = product_details['order_price_per_unit']
          gst_percentage = product_details['order_item_gst']
        else
          price_per_unit = product_details['price_per_unit']
          gst_percentage = product_details['child_item_gst']
        end
        quantity = is_lost ? dpir['lost_quantity'].to_f : dpir['shipped_quantity'].to_f
        amount_without_tax = quantity.to_f * price_per_unit.to_f
        if is_lost && is_debit_note && !lost_data.blank?
          lost_quantity = quantity
          lost_data.each do |datum|
            if dpir['id'] == datum['id']
              lost_quantity = (datum['lost_quantity'].to_f - lost_quantity.to_f).abs
            end
          end
          quantity = lost_quantity.to_f
        end
        line_item_details << {
            item_name: product_details['product_name'],
            hsn: product_details['hsn_number'],
            quantity: quantity,
            price_per_unit: price_per_unit,
            tax_percentage: gst_percentage,
            amount_without_tax:amount_without_tax.to_f,
            dispatch_plan_item_relation_id: dpir['id']
        }
        @amount += quantity * price_per_unit * (1+(gst_percentage.to_f/100))
        @sku_codes << product_details['sku_code']
      end
      line_item_details
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
          invoice_using_igst: InvoiceCreationHelper.get_if_igst_required(data)
      }
    end

    def generate_return_cancel_debit_note(connection, id, logger)
      dn_create_data = SQL.get_all_shipment_info(connection, id)
      if [3,6].include? shipment_create_data[:dispatch_plan]['dispatch_mode']
        # Create DN for buyer_to_warehouse, buyer_to_seller cancellation (non lost returns)
        forward_shipment = SQL.get_shipment(connection, dn_create_data[:shipment]['forward_shipment_id']);
        actions = SQL.get_shipment_actions_by_id(connection, dn_create_data[:shipment]['id'], 29)
        if actions.blank?
          message = create_invoice(dn_create_data)
          message.merge!({
                             invoice_id_for_note: forward_shipment['buyer_invoice_id'],
                             type: 'DEBIT_NOTE'
                         })
          KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger)
        end
      end
    end

    def generate_cancel_credit_note(connection, id, logger)
      cn_create_data = SQL.get_all_shipment_info(connection, id)
      if [0,4].include? cn_create_data[:dispatch_plan]['dispatch_mode']
        message = create_invoice(cn_create_data)
        message.merge!({
                           invoice_id_for_note: cn_create_data['buyer_invoice_id'],
                           type: 'CREDIT_NOTE_FOR_INVOICE_NULLIFICATION'
                       })
        KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger)
      end
    end
  end
end
