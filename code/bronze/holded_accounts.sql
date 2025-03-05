create table bronze.holded_accounts (
  id character varying(24) not null,
  color character varying(7) null,
  num bigint null,
  name character varying(255) null,
  "group" character varying(255) null,
  debit numeric(15, 2) null,
  credit numeric(15, 2) null,
  balance numeric(15, 2) null,
  dwh_source_system character varying(50) null default 'holded'::character varying,
  dwh_source_entity character varying(50) null default 'accounts'::character varying,
  dwh_insert_timestamp timestamp without time zone null default CURRENT_TIMESTAMP,
  dwh_update_timestamp timestamp without time zone null default CURRENT_TIMESTAMP,
  dwh_batch_id character varying(50) null,
  dwh_process_id character varying(50) null,
  constraint pk_holded_accounts primary key (id)
) TABLESPACE pg_default;

create index IF not exists idx_holded_accounts_num on bronze.holded_accounts using btree (num) TABLESPACE pg_default;

create index IF not exists idx_holded_accounts_group on bronze.holded_accounts using btree ("group") TABLESPACE pg_default;
