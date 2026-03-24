BEGIN;

CREATE TABLE IF NOT EXISTS business_partners (
    business_partner TEXT PRIMARY KEY,
    customer TEXT UNIQUE,
    business_partner_category TEXT,
    business_partner_full_name TEXT,
    business_partner_grouping TEXT,
    business_partner_name TEXT,
    correspondence_language TEXT,
    created_by_user TEXT,
    creation_date DATE,
    creation_time TIME,
    creation_time_json JSONB,
    first_name TEXT,
    form_of_address TEXT,
    industry TEXT,
    last_change_date DATE,
    last_name TEXT,
    organization_bp_name1 TEXT,
    organization_bp_name2 TEXT,
    business_partner_is_blocked BOOLEAN,
    is_marked_for_archiving BOOLEAN
);

CREATE TABLE IF NOT EXISTS business_partner_addresses (
    business_partner TEXT NOT NULL REFERENCES business_partners(business_partner),
    address_id TEXT NOT NULL,
    validity_start_date DATE NOT NULL,
    validity_end_date DATE,
    address_uuid TEXT,
    address_time_zone TEXT,
    city_name TEXT,
    country TEXT,
    po_box TEXT,
    po_box_deviating_city_name TEXT,
    po_box_deviating_country TEXT,
    po_box_deviating_region TEXT,
    po_box_is_without_number BOOLEAN,
    po_box_lobby_name TEXT,
    po_box_postal_code TEXT,
    postal_code TEXT,
    region TEXT,
    street_name TEXT,
    tax_jurisdiction TEXT,
    transport_zone TEXT,
    PRIMARY KEY (business_partner, address_id, validity_start_date)
);

CREATE TABLE IF NOT EXISTS customer_company_assignments (
    customer TEXT NOT NULL REFERENCES business_partners(customer),
    company_code TEXT NOT NULL,
    accounting_clerk TEXT,
    accounting_clerk_fax_number TEXT,
    accounting_clerk_internet_address TEXT,
    accounting_clerk_phone_number TEXT,
    alternative_payer_account TEXT,
    payment_blocking_reason TEXT,
    payment_methods_list TEXT,
    payment_terms TEXT,
    reconciliation_account TEXT,
    deletion_indicator BOOLEAN,
    customer_account_group TEXT,
    PRIMARY KEY (customer, company_code)
);

CREATE TABLE IF NOT EXISTS customer_sales_area_assignments (
    customer TEXT NOT NULL REFERENCES business_partners(customer),
    sales_organization TEXT NOT NULL,
    distribution_channel TEXT NOT NULL,
    division TEXT NOT NULL,
    billing_is_blocked_for_customer TEXT,
    complete_delivery_is_defined BOOLEAN,
    credit_control_area TEXT,
    currency TEXT,
    customer_payment_terms TEXT,
    delivery_priority TEXT,
    incoterms_classification TEXT,
    incoterms_location1 TEXT,
    sales_group TEXT,
    sales_office TEXT,
    shipping_condition TEXT,
    sls_unlmtd_ovrdeliv_is_allwd BOOLEAN,
    supplying_plant TEXT,
    sales_district TEXT,
    exchange_rate_type TEXT,
    PRIMARY KEY (customer, sales_organization, distribution_channel, division)
);

CREATE TABLE IF NOT EXISTS plants (
    plant TEXT PRIMARY KEY,
    plant_name TEXT,
    valuation_area TEXT,
    plant_customer TEXT,
    plant_supplier TEXT,
    factory_calendar TEXT,
    default_purchasing_organization TEXT,
    sales_organization TEXT,
    address_id TEXT,
    plant_category TEXT,
    distribution_channel TEXT,
    division TEXT,
    language TEXT,
    is_marked_for_archiving BOOLEAN
);

CREATE TABLE IF NOT EXISTS products (
    product TEXT PRIMARY KEY,
    product_type TEXT,
    cross_plant_status TEXT,
    cross_plant_status_validity_date DATE,
    creation_date DATE,
    created_by_user TEXT,
    last_change_date DATE,
    last_change_date_time TIMESTAMP,
    is_marked_for_deletion BOOLEAN,
    product_old_id TEXT,
    gross_weight NUMERIC,
    weight_unit TEXT,
    net_weight NUMERIC,
    product_group TEXT,
    base_unit TEXT,
    division TEXT,
    industry_sector TEXT
);

CREATE TABLE IF NOT EXISTS product_descriptions (
    product TEXT NOT NULL REFERENCES products(product),
    language TEXT NOT NULL,
    product_description TEXT,
    PRIMARY KEY (product, language)
);

CREATE TABLE IF NOT EXISTS product_plants (
    product TEXT NOT NULL REFERENCES products(product),
    plant TEXT NOT NULL REFERENCES plants(plant),
    country_of_origin TEXT,
    region_of_origin TEXT,
    production_invtry_managed_loc TEXT,
    availability_check_type TEXT,
    fiscal_year_variant TEXT,
    profit_center TEXT,
    mrp_type TEXT,
    PRIMARY KEY (product, plant)
);

CREATE TABLE IF NOT EXISTS product_storage_locations (
    product TEXT NOT NULL REFERENCES products(product),
    plant TEXT NOT NULL REFERENCES plants(plant),
    storage_location TEXT NOT NULL,
    physical_inventory_block_ind TEXT,
    date_of_last_posted_cnt_unrstrcd_stk DATE,
    PRIMARY KEY (product, plant, storage_location),
    FOREIGN KEY (product, plant) REFERENCES product_plants(product, plant)
);

CREATE TABLE IF NOT EXISTS sales_order_headers (
    sales_order TEXT PRIMARY KEY,
    sales_order_type TEXT,
    sales_organization TEXT,
    distribution_channel TEXT,
    organization_division TEXT,
    sales_group TEXT,
    sales_office TEXT,
    sold_to_party TEXT REFERENCES business_partners(customer),
    creation_date DATE,
    created_by_user TEXT,
    last_change_date_time TIMESTAMP,
    total_net_amount NUMERIC,
    overall_delivery_status TEXT,
    overall_ord_reltd_billg_status TEXT,
    overall_sd_doc_reference_status TEXT,
    transaction_currency TEXT,
    pricing_date DATE,
    requested_delivery_date DATE,
    header_billing_block_reason TEXT,
    delivery_block_reason TEXT,
    incoterms_classification TEXT,
    incoterms_location1 TEXT,
    customer_payment_terms TEXT,
    total_credit_check_status TEXT
);

CREATE TABLE IF NOT EXISTS sales_order_items (
    sales_order TEXT NOT NULL REFERENCES sales_order_headers(sales_order),
    sales_order_item TEXT NOT NULL,
    sales_order_item_category TEXT,
    material TEXT REFERENCES products(product),
    requested_quantity NUMERIC,
    requested_quantity_unit TEXT,
    transaction_currency TEXT,
    net_amount NUMERIC,
    material_group TEXT,
    production_plant TEXT REFERENCES plants(plant),
    storage_location TEXT,
    sales_document_rjcn_reason TEXT,
    item_billing_block_reason TEXT,
    PRIMARY KEY (sales_order, sales_order_item)
);

CREATE TABLE IF NOT EXISTS sales_order_schedule_lines (
    sales_order TEXT NOT NULL,
    sales_order_item TEXT NOT NULL,
    schedule_line TEXT NOT NULL,
    confirmed_delivery_date DATE,
    order_quantity_unit TEXT,
    confd_order_qty_by_matl_avail_check NUMERIC,
    PRIMARY KEY (sales_order, sales_order_item, schedule_line),
    FOREIGN KEY (sales_order, sales_order_item)
        REFERENCES sales_order_items(sales_order, sales_order_item)
);

CREATE TABLE IF NOT EXISTS outbound_delivery_headers (
    delivery_document TEXT PRIMARY KEY,
    actual_goods_movement_date DATE,
    actual_goods_movement_time TIME,
    actual_goods_movement_time_json JSONB,
    creation_date DATE,
    creation_time TIME,
    creation_time_json JSONB,
    delivery_block_reason TEXT,
    hdr_general_incompletion_status TEXT,
    header_billing_block_reason TEXT,
    last_change_date DATE,
    overall_goods_movement_status TEXT,
    overall_picking_status TEXT,
    overall_proof_of_delivery_status TEXT,
    shipping_point TEXT
);

CREATE TABLE IF NOT EXISTS outbound_delivery_items (
    delivery_document TEXT NOT NULL REFERENCES outbound_delivery_headers(delivery_document),
    delivery_document_item TEXT NOT NULL,
    actual_delivery_quantity NUMERIC,
    batch TEXT,
    delivery_quantity_unit TEXT,
    item_billing_block_reason TEXT,
    last_change_date DATE,
    plant TEXT REFERENCES plants(plant),
    reference_sd_document TEXT,
    reference_sd_document_item TEXT,
    storage_location TEXT,
    PRIMARY KEY (delivery_document, delivery_document_item)
);

CREATE TABLE IF NOT EXISTS billing_document_headers (
    billing_document TEXT PRIMARY KEY,
    billing_document_type TEXT,
    creation_date DATE,
    creation_time TIME,
    creation_time_json JSONB,
    last_change_date_time TIMESTAMP,
    billing_document_date DATE,
    billing_document_is_cancelled BOOLEAN,
    cancelled_billing_document TEXT REFERENCES billing_document_headers(billing_document),
    total_net_amount NUMERIC,
    transaction_currency TEXT,
    company_code TEXT,
    fiscal_year TEXT,
    accounting_document TEXT,
    sold_to_party TEXT REFERENCES business_partners(customer)
);

CREATE TABLE IF NOT EXISTS billing_document_items (
    billing_document TEXT NOT NULL REFERENCES billing_document_headers(billing_document),
    billing_document_item TEXT NOT NULL,
    material TEXT REFERENCES products(product),
    billing_quantity NUMERIC,
    billing_quantity_unit TEXT,
    net_amount NUMERIC,
    transaction_currency TEXT,
    reference_sd_document TEXT,
    reference_sd_document_item TEXT,
    PRIMARY KEY (billing_document, billing_document_item)
);

CREATE TABLE IF NOT EXISTS billing_document_cancellations (
    billing_document TEXT PRIMARY KEY REFERENCES billing_document_headers(billing_document),
    billing_document_type TEXT,
    creation_date DATE,
    creation_time TIME,
    creation_time_json JSONB,
    last_change_date_time TIMESTAMP,
    billing_document_date DATE,
    billing_document_is_cancelled BOOLEAN,
    cancelled_billing_document TEXT REFERENCES billing_document_headers(billing_document),
    total_net_amount NUMERIC,
    transaction_currency TEXT,
    company_code TEXT,
    fiscal_year TEXT,
    accounting_document TEXT,
    sold_to_party TEXT REFERENCES business_partners(customer)
);

CREATE TABLE IF NOT EXISTS journal_entry_items_accounts_receivable (
    company_code TEXT NOT NULL,
    fiscal_year TEXT NOT NULL,
    accounting_document TEXT NOT NULL,
    accounting_document_item TEXT NOT NULL,
    gl_account TEXT,
    reference_document TEXT,
    cost_center TEXT,
    profit_center TEXT,
    transaction_currency TEXT,
    amount_in_transaction_currency NUMERIC,
    company_code_currency TEXT,
    amount_in_company_code_currency NUMERIC,
    posting_date DATE,
    document_date DATE,
    accounting_document_type TEXT,
    assignment_reference TEXT,
    last_change_date_time TIMESTAMP,
    customer TEXT REFERENCES business_partners(customer),
    financial_account_type TEXT,
    clearing_date DATE,
    clearing_accounting_document TEXT,
    clearing_doc_fiscal_year TEXT,
    PRIMARY KEY (company_code, fiscal_year, accounting_document, accounting_document_item)
);

CREATE TABLE IF NOT EXISTS payments_accounts_receivable (
    company_code TEXT NOT NULL,
    fiscal_year TEXT NOT NULL,
    accounting_document TEXT NOT NULL,
    accounting_document_item TEXT NOT NULL,
    clearing_date DATE,
    clearing_accounting_document TEXT,
    clearing_doc_fiscal_year TEXT,
    amount_in_transaction_currency NUMERIC,
    transaction_currency TEXT,
    amount_in_company_code_currency NUMERIC,
    company_code_currency TEXT,
    customer TEXT REFERENCES business_partners(customer),
    invoice_reference TEXT,
    invoice_reference_fiscal_year TEXT,
    sales_document TEXT,
    sales_document_item TEXT,
    posting_date DATE,
    document_date DATE,
    assignment_reference TEXT,
    gl_account TEXT,
    financial_account_type TEXT,
    profit_center TEXT,
    cost_center TEXT,
    PRIMARY KEY (company_code, fiscal_year, accounting_document, accounting_document_item)
);

CREATE INDEX IF NOT EXISTS idx_sales_order_headers_sold_to_party
    ON sales_order_headers (sold_to_party);
CREATE INDEX IF NOT EXISTS idx_sales_order_items_material
    ON sales_order_items (material);
CREATE INDEX IF NOT EXISTS idx_outbound_delivery_items_reference_sd
    ON outbound_delivery_items (reference_sd_document, reference_sd_document_item);
CREATE INDEX IF NOT EXISTS idx_billing_document_items_reference_sd
    ON billing_document_items (reference_sd_document, reference_sd_document_item);
CREATE INDEX IF NOT EXISTS idx_payments_customer
    ON payments_accounts_receivable (customer);
CREATE INDEX IF NOT EXISTS idx_journal_customer
    ON journal_entry_items_accounts_receivable (customer);

CREATE OR REPLACE VIEW customers AS
SELECT
    bp.customer AS customer_id,
    bp.business_partner_full_name AS name,
    bp.business_partner_grouping AS grouping,
    bp.business_partner_is_blocked AS is_blocked,
    bp.is_marked_for_archiving AS is_archived,
    bp.creation_date AS created_date
FROM business_partners bp
WHERE bp.customer IS NOT NULL;

CREATE OR REPLACE VIEW orders AS
SELECT
    so.sales_order AS order_id,
    so.sold_to_party AS customer_id,
    so.sales_order_type AS order_type,
    so.sales_organization AS sales_org,
    so.creation_date AS order_date,
    so.requested_delivery_date,
    so.total_net_amount AS total_amount,
    so.transaction_currency AS currency,
    so.overall_delivery_status AS delivery_status,
    so.overall_sd_doc_reference_status AS process_status
FROM sales_order_headers so;

CREATE OR REPLACE VIEW order_items AS
SELECT
    soi.sales_order AS order_id,
    soi.sales_order_item AS line_no,
    soi.material AS product_id,
    soi.requested_quantity AS quantity,
    soi.requested_quantity_unit AS unit,
    soi.net_amount,
    soi.production_plant AS plant,
    soi.sales_order_item_category AS item_category,
    NULL::TEXT AS delivery_status
FROM sales_order_items soi;

CREATE OR REPLACE VIEW deliveries AS
SELECT DISTINCT
    odh.delivery_document AS delivery_id,
    odi.reference_sd_document AS order_id,
    odh.creation_date AS created_date,
    odh.actual_goods_movement_date AS ship_date,
    odh.overall_picking_status AS picking_status,
    odh.overall_goods_movement_status AS goods_status,
    odh.shipping_point,
    odh.delivery_block_reason AS delivery_block
FROM outbound_delivery_headers odh
LEFT JOIN outbound_delivery_items odi
    ON odi.delivery_document = odh.delivery_document;

CREATE OR REPLACE VIEW invoices AS
SELECT DISTINCT
    bdh.billing_document AS invoice_id,
    bdi.reference_sd_document AS order_id,
    bdh.sold_to_party AS customer_id,
    bdh.billing_document_type AS invoice_type,
    bdh.billing_document_date AS invoice_date,
    bdh.total_net_amount AS total_amount,
    bdh.transaction_currency AS currency,
    bdh.accounting_document AS accounting_doc,
    bdh.billing_document_is_cancelled AS is_cancelled
FROM billing_document_headers bdh
LEFT JOIN billing_document_items bdi
    ON bdi.billing_document = bdh.billing_document;

CREATE OR REPLACE VIEW payments AS
SELECT
    par.accounting_document AS payment_id,
    par.accounting_document_item AS payment_item,
    par.customer AS customer_id,
    par.clearing_date,
    par.posting_date,
    par.amount_in_transaction_currency AS amount,
    par.transaction_currency AS currency,
    par.clearing_accounting_document AS clearing_doc,
    par.gl_account,
    (par.amount_in_transaction_currency > 0) AS is_incoming
FROM payments_accounts_receivable par;

COMMIT;
