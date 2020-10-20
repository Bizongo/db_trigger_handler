-- Function to send notifications
CREATE OR REPLACE FUNCTION notify(channel VARCHAR(100), notify_data jsonb DEFAULT '{}'::jsonb)
  RETURNS void AS
$$
BEGIN
    PERFORM pg_notify(channel, notify_data::text);
END;
$$
LANGUAGE 'plpgsql';

-- Function to notify shipment changes
CREATE OR REPLACE FUNCTION notify_shipment_changes()
  RETURNS trigger AS
$$
DECLARE
  channel VARCHAR(50) := '';
  notify_data jsonb := '{}'::jsonb;
BEGIN
  IF TG_OP ilike('INSERT')
  THEN
    channel := 'shipment_created';
    notify_data := json_build_object('id', NEW.id);
    PERFORM pg_notify(channel, notify_data::text);
    RETURN NEW;
  ELSIF TG_OP ilike('UPDATE') and
  (UPDATE(seller_due_date) or UPDATE(total_seller_invoice_amount)
  or UPDATE(actual_charges) or UPDATE(total_buyer_invoice_amount)
  or UPDATE(seller_extra_charges) or UPDATE(seller_invoice_no) or UPDATE(status))
  THEN
    channel := 'shipment_updated';
    notify_data := json_build_object('id', NEW.id);
    PERFORM pg_notify(channel, notify_data::text);
    RETURN NEW;
  ELSIF TG_OP ilike('DELETE')
  THEN
    channel := 'shipment_cancelled';
    notify_data := json_build_object('id', OLD.id);
    PERFORM pg_notify(channel, notify_data::text);
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER shipment_changed AFTER INSERT OR UPDATE OR DELETE
ON supply_chain.shipments
FOR EACH ROW
EXECUTE PROCEDURE notify_shipment_changes();

-- Test sending notification
DO $$
DECLARE
BEGIN
    PERFORM notify('shipment_created', '{}'::jsonb);
END;
$$
LANGUAGE 'plpgsql';