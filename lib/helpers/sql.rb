module SQL
  class << self
    def subscribe_channel(connection, channel)
      execute_query(connection, "LISTEN #{channel}")
    end

    def unsubscribe_channel(connection, channel)
      execute_query(connection, "UNLISTEN #{channel}")
    end

    def get_all_shipment_info(connection, id)
      shipment = get_shipment(connection,id)
      {
          shipment: shipment,
          dispatch_plan: get_dispatch_plan(connection, shipment['dispatch_plan_id']),
          dispatch_plan_item_relations: get_dispatch_plan_item_relations(connection, shipment['dispatch_plan_id']),
          transition_address: get_transition_address(connection, shipment['transition_address_id'])
      }
    end

    def get_shipment(connection, id)
      execute_query(connection,
                    "select * from supply_chain.shipments where id = #{id}").first
    end

    def get_dispatch_plan(connection, id)
      execute_query(connection,
                    "select * from supply_chain.dispatch_plans where id = #{id}").first
    end

    def get_dispatch_plan_item_relations(connection, dispatch_plan_id)
      execute_query(connection,
                    "select * from supply_chain.dispatch_plan_item_relations"+
                        " where shipped_quantity > 0.0 and dispatch_plan_id = #{dispatch_plan_id}").to_a
    end

    def get_transition_address(connection, id)
      execute_query(connection,
                    "select * from ums.addresses where id = #{id}").first
    end

    private
    def execute_query(connection, query)
      connection.execute(query)
    end
  end
end
