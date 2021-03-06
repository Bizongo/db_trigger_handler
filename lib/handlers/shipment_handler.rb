require 'helpers/sql'
require 'helpers/kafka_helper'
require 'helpers/invoice_creation_helper'

module ShipmentHandler
  include SQL
  include KafkaHelper
  include InvoiceCreationHelper

  class << self
    def shipment_create_handler(connection, data, logger, kafka_broker)
      parsed_data = JSON.parse data
      shipment_create_data = SQL.get_all_shipment_info(connection, parsed_data['id'])
      if [0,2,4].include? shipment_create_data[:dispatch_plan]['dispatch_mode']
        # Create Invoice For seller_to_buyer, warehouse_to_warehouse, warehouse_to_buyer
        # KafkaHelper::Client.produce(message: create_invoice(shipment_create_data),
        #                             topic: "shipment_created", logger: logger, kafka_broker: kafka_broker)
      elsif [3,6].include? shipment_create_data[:dispatch_plan]['dispatch_mode']
        # Create Invoice for buyer_to_warehouse, buyer_to_seller (non lost returns)
        forward_shipment = SQL.get_shipment(connection, shipment_create_data[:shipment]['forward_shipment_id']);
        actions = SQL.get_shipment_actions_by_id(connection, shipment_create_data[:shipment]['id'], 29)
        if actions.blank? and forward_shipment['delivered_at'].blank?
          @comment = 'Return Created'
          message = create_invoice(shipment_create_data)
          message.merge!({
            invoice_id_for_note: forward_shipment['buyer_invoice_id'],
            type: 'CREDIT_NOTE'
          })
          KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger, kafka_broker: kafka_broker)
        end
      end
    end

    def shipment_dpir_transaction_handler(connection, data, logger, kafka_broker)
      parsed_data = JSON.parse data
      shipment_lost_data = SQL.get_lost_shipment_info(connection, parsed_data['id'], parsed_data['is_debit_note'].present?)
      if [0,2,4].include? shipment_lost_data[:dispatch_plan]['dispatch_mode']
        @comment = 'Lost'
        message = create_lost_shipment_credit_note(shipment_lost_data, parsed_data['is_debit_note'].present?)
        message.merge!({
          invoice_id_for_note: shipment_lost_data[:shipment]['buyer_invoice_id'],
          supporting_document_details: get_supporting_document_details(shipment_lost_data),
          type: parsed_data['is_debit_note'].present? ? 'DEBIT_NOTE' : 'CREDIT_NOTE'
        })
        KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger, kafka_broker: kafka_broker)
      elsif [3,6].include? shipment_lost_data[:dispatch_plan]['dispatch_mode']
        forward_shipment = SQL.get_shipment(connection, shipment_lost_data[:shipment]['forward_shipment_id'])
        if !forward_shipment['delivered_at'].blank?
          @comment = 'Return Lost'
          message = create_lost_shipment_credit_note(shipment_lost_data, parsed_data['is_debit_note'].present?)
          message.merge!({
            invoice_id_for_note: forward_shipment['buyer_invoice_id'],
            supporting_document_details: get_supporting_document_details(shipment_lost_data),
            type: 'CREDIT_NOTE'
          })
          KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger, kafka_broker: kafka_broker)
        end
      end
    end

    def shipment_updated(connection, data, logger, kafka_broker)
      parse_data = JSON.parse data
      shipment = SQL.get_shipment(connection, parse_data['id'])
      if shipment['status'] == 3
        if shipment['seller_invoice_id'].present?
          update_invoice_data = {status: 'CANCELLED', id: shipment['seller_invoice_id']}
          KafkaHelper::Client.produce(message: update_invoice_data, topic: "shipment_updated", logger: logger, kafka_broker: kafka_broker)
        end
        if shipment['buyer_invoice_id'].present?
          datetime = shipment['created_at'].to_datetime
          if Time.now - datetime < 24.hours
            update_invoice_data = {status: 'CANCELLED', id: shipment['buyer_invoice_id']}
            KafkaHelper::Client.produce(message: update_invoice_data, topic: "shipment_updated", logger: logger, kafka_broker: kafka_broker)
          else
            @comment = 'Shipment Cancelled'
            generate_cancel_credit_note(connection, shipment['id'], logger, kafka_broker)
          end
        end
        if shipment['buyer_invoice_id'].blank? && shipment['seller_invoice_id'].blank?
          @comment = 'Return Cancelled'
          generate_return_cancel_debit_note(connection, shipment['id'], logger, kafka_broker)
        end
      end
    end

    def shipment_delivered(connection, data, logger, kafka_broker)
      parse_data = JSON.parse data
      return_shipment_delivered_data = SQL.get_all_shipment_info(connection, parse_data['id'])
      return unless [3,6].include? return_shipment_delivered_data[:dispatch_plan]['dispatch_mode']
      actions = SQL.get_shipment_actions_by_id(connection, return_shipment_delivered_data[:shipment]['id'], 29)
      forward_shipment = SQL.get_shipment(connection, return_shipment_delivered_data[:shipment]['forward_shipment_id'])
      if return_shipment_delivered_data[:shipment]['status'] == 2 && !forward_shipment['delivered_at'].blank? &&
        actions.blank?
        dpirs = SQL.get_dispatch_plan_item_relations_unchecked(connection, return_shipment_delivered_data[:dispatch_plan]['id'])
        new_dpirs = []
        dpirs.each do |dpir|
          if [3].include? return_shipment_delivered_data[:dispatch_plan]['dispatch_mode']
            dpir['shipped_quantity'] = SQL.get_inwarded_good(connection, dpir['id'])['quantity']
            unless dpir['shipped_quantity'] == 0
              new_dpirs << dpir
            end
          else
            unless dpir['shipped_quantity'] == 0
              new_dpirs << dpir
            end
          end
        end
        return_shipment_delivered_data[:dispatch_plan_item_relations] = new_dpirs

        # Create Invoice for buyer_to_warehouse, buyer_to_seller (non lost returns)
        @comment = 'Return Delivered'
        message = create_invoice(return_shipment_delivered_data)
        message.merge!({
          invoice_id_for_note: forward_shipment['buyer_invoice_id'],
          type: 'CREDIT_NOTE'
        })
        KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger, kafka_broker: kafka_broker)
      end
    end

    private

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
        item_name = product_details['alias_name'].present? ? product_details['alias_name'] : product_details['product_name']
        line_item_details << {
            item_name: item_name,
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
          invoice_using_igst: InvoiceCreationHelper.get_if_igst_required(data),
          comment: @comment.presence || ''
      }
    end

    def generate_return_cancel_debit_note(connection, id, logger, kafka_broker)
      dn_create_data = SQL.get_all_shipment_info(connection, id)
      if [3,6].include? dn_create_data[:dispatch_plan]['dispatch_mode']
        # Create DN for buyer_to_warehouse, buyer_to_seller cancellation (non lost returns)
        forward_shipment = SQL.get_shipment(connection, dn_create_data[:shipment]['forward_shipment_id']);
        actions = SQL.get_shipment_actions_by_id(connection, dn_create_data[:shipment]['id'], 29)
        if actions.blank? && forward_shipment['delivered_at'].blank?
          message = create_invoice(dn_create_data)
          message.merge!({
                             invoice_id_for_note: forward_shipment['buyer_invoice_id'],
                             type: 'DEBIT_NOTE'
                         })
          KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger, kafka_broker: kafka_broker)
        end
      end
    end

    def generate_cancel_credit_note(connection, id, logger, kafka_broker)
      cn_create_data = SQL.get_all_shipment_info(connection, id)
      if [0,2,4].include? cn_create_data[:dispatch_plan]['dispatch_mode']
        message = create_invoice(cn_create_data)
        message.merge!({
                           invoice_id_for_note: cn_create_data[:shipment]['buyer_invoice_id'],
                           type: 'CREDIT_NOTE',
                           sub_type: 'INVOICE_NULLIFICATION'
                       })
        KafkaHelper::Client.produce(message: message, topic: "shipment_created", logger: logger, kafka_broker: kafka_broker)
      end
    end
  end
end
