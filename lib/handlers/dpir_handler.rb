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
      pp dpir_update_data
    end

    private

    def shipped_quantity_change
    end

    def returned_quantity_change
    end

    def lost_quantity_change
    end

    def price_per_unit_change
    end

    def gst_percentage_change
    end

  end

end
