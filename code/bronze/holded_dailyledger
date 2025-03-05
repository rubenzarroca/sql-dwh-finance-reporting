create table bronze.holded_dailyledger (
  entrynumber integer not null,
  line integer not null,
  timestamp bigint not null,
  type character varying(50) null,
  description text null,
  docdescription text null,
  account bigint null,
  debit numeric(15, 2) null,
  credit numeric(15, 2) null,
  tags jsonb null,
  checked character varying(3) null,
  dwh_source_system character varying(50) null default 'holded'::character varying,
  dwh_source_entity character varying(50) null default 'dailyledger'::character varying,
  dwh_insert_timestamp timestamp without time zone null default CURRENT_TIMESTAMP,
  dwh_update_timestamp timestamp without time zone null default CURRENT_TIMESTAMP,
  dwh_batch_id character varying(50) null,
  dwh_process_id character varying(50) null,
  dwh_page_number integer null,
  constraint pk_holded_dailyledger primary key (entrynumber, line, "timestamp")
) TABLESPACE pg_default;

create index IF not exists idx_holded_dailyledger_timestamp on bronze.holded_dailyledger using btree ("timestamp") TABLESPACE pg_default;

create index IF not exists idx_holded_dailyledger_account on bronze.holded_dailyledger using btree (account) TABLESPACE pg_default;

create index IF not exists idx_holded_dailyledger_type on bronze.holded_dailyledger using btree (type) TABLESPACE pg_default;
