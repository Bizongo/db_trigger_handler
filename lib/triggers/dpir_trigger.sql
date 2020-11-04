-- Function to notify DPIR changes
CREATE OR REPLACE FUNCTION notify_dpir_changes()
  RETURNS trigger AS
$$
DECLARE
  channel VARCHAR(50) := '';
  notify_data jsonb := '{}'::jsonb;
BEGIN
    IF TG_OP ilike('UPDATE')
    THEN
        IF OLD.shipped_quantity is not NULL and OLD.shipped_quantity != 0 and OLD.shipped_quantity is DISTINCT FROM NEW.shipped_quantity
        THEN
            channel := 'dpir_updated';
            notify_data := json_build_object('id', NEW.id, 'old', OLD.shipped_quantity, 'type', 'SHIPPED_QUANTITY_CHANGE');
            PERFORM pg_notify(channel, notify_data::text);
            RETURN NEW;
--         ELSIF OLD.returned_quantity  is not NULL and OLD.returned_quantity != 0 and NEW.returned_quantity != 0 and OLD.returned_quantity is DISTINCT FROM NEW.returned_quantity
--         THEN
--             channel := 'dpir_updated';
--             notify_data := json_build_object('id', NEW.id, 'old', OLD.returned_quantity, 'type', 'RETURNED_QUANTITY_CHANGE');
--             PERFORM pg_notify(channel, notify_data::text);
--             RETURN NEW;
        ELSIF OLD.lost_quantity is not NULL and OLD.lost_quantity != 0 and OLD.lost_quantity is DISTINCT FROM NEW.lost_quantity
        THEN
            channel := 'dpir_updated';
            notify_data := json_build_object('id', NEW.id, 'old', OLD.lost_quantity, 'type', 'LOST_QUANTITY_CHANGE');
            PERFORM pg_notify(channel, notify_data::text);
            RETURN NEW;
            RETURN NEW;
        ELSIF OLD.product_details <> '{}'::jsonb and NEW.product_details <> '{}'::jsonb
        THEN
            IF OLD.product_details->>'order_price_per_unit' is not NULL and OLD.product_details->>'order_price_per_unit' is DISTINCT FROM NEW.product_details->>'order_price_per_unit'
            THEN
                channel := 'dpir_updated';
                notify_data := json_build_object('id', NEW.id, 'old', OLD.product_details->>'order_price_per_unit', 'type', 'PRICE_PER_UNIT_CHANGE');
                PERFORM pg_notify(channel, notify_data::text);
            END IF;
            IF OLD.product_details->>'order_item_gst' is not NULL and OLD.product_details->>'order_item_gst' is DISTINCT FROM NEW.product_details->>'order_item_gst'
            THEN
                channel := 'dpir_updated';
                notify_data := json_build_object('id', NEW.id, 'old', OLD.product_details->>'order_item_gst', 'type', 'GST_CHANGE');
                PERFORM pg_notify(channel, notify_data::text);
            END IF;
            RETURN NEW;
        END IF;
    END IF;
    return NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER dpir_changed AFTER UPDATE
ON supply_chain.dispatch_plan_item_relations
FOR EACH ROW
EXECUTE PROCEDURE notify_dpir_changes();