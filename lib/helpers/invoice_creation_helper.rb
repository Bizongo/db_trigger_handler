module InvoiceCreationHelper
  class << self
    @lead_plus_account_pan_mapping = {
        54 => 'AAECH3221K'
    }

    def common_create_invoice_data(data)
      @sku_codes = []
      @account_name = ""
      @entity_reference_number = ""
      @center_id = nil
      @buyer_gstin_state_code = ""
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
          shipment_id: data[:shipment]['id'],
      }
    end

    private

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
      seller_details = {
          name: seller_company['seller_company_name'],
          company_name: seller_company['seller_company_name'],
          gstin: data[:transition_address]['gstin'],
          address: {
              street_address: "#{data[:transition_address]['street_address']} #{data[:transition_address]['city']} - #{data[:transition_address]['pincode']}",
              pincode: data[:transition_address]['pincode'],
              state: data[:transition_address]['state'],
              country:  data[:transition_address]['country'],
              state_code: data[:transition_address]['gstin_state_code']
          }
      }
      seller_details.merge!({
        email_id: seller_company['seller_primary_contact']['email'],
        contact_number: seller_company['seller_primary_contact']['mobile']
      }) if seller_company['seller_primary_contact'].present?
      seller_details
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

    def get_pan data
      pan = ""
      buyer_company_snapshot = JSON.parse data['buyer_company_snapshot']
      destination_address = JSON.parse data['destination_address_snapshot']
      account_id = buyer_company_snapshot['billing_address']['lead_plus_account_id'] if buyer_company_snapshot['billing_address'].present?
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
