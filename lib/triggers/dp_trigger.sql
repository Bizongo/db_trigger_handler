-- Function to notify DPIR changes
CREATE OR REPLACE FUNCTION notify_dp_changes()
  RETURNS trigger AS
$$
DECLARE
  channel VARCHAR(50) := '';
  notify_data jsonb := '{}'::jsonb;
BEGIN
    IF TG_OP ilike('UPDATE')
    THEN
        IF OLD.buyer_company_snapshot is not null and OLD.buyer_company_snapshot <> '{}'::jsonb and NEW.buyer_company_snapshot <> '{}'::jsonb and OLD.buyer_company_snapshot is distinct from NEW.buyer_company_snapshot
        THEN
            IF OLD.buyer_company_snapshot->>'billing_address' is not NULL and OLD.buyer_company_snapshot->>'billing_address' is DISTINCT FROM NEW.buyer_company_snapshot->>'billing_address'
            THEN
                channel := 'dp_updated';
                notify_data := json_build_object('id', NEW.id, 'old', OLD.buyer_company_snapshot->>'billing_address', 'type', 'BUYER_DETAILS_UPDATE');
                PERFORM pg_notify(channel, notify_data::text);
            END IF;
            RETURN NEW;
        ELSIF OLD.destination_address_snapshot is not null and OLD.destination_address_snapshot <> '{}'::jsonb and NEW.destination_address_snapshot <> '{}'::jsonb and OLD.destination_address_snapshot is distinct from NEW.destination_address_snapshot
        THEN
            channel := 'dp_updated';
            notify_data := json_build_object('id', NEW.id, 'type', 'DESTINATION_CHANGE');
            PERFORM pg_notify(channel, notify_data::text);
            RETURN NEW;
        END IF;
    END IF;
    return NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER dp_changed AFTER UPDATE
ON supply_chain.dispatch_plans
FOR EACH ROW
EXECUTE PROCEDURE notify_dp_changes();