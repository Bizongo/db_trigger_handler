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
  ELSIF TG_OP ilike('UPDATE')
  THEN
    channel := 'shipment_updated';
    notify_data := json_build_object('id', NEW.id);
    PERFORM pg_notify(channel, notify_data::text);
    RETURN NEW;
  ELSIF TG_OP ilike('DELETE')
  THEN
    channel := 'shipment_deleted';
    notify_data := json_build_object('id', OLD.id);
    PERFORM pg_notify(channel, notify_data::text);
    RETURN OLD;
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