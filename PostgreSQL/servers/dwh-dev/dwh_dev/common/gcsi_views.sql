create or replace view common.t_subscriptions as 
( SELECT s.id AS subscription_id,
    s.uuid AS subscription_uuid,
    s.user_id,
    u.customer_edp_number AS custedp,
    s.start_date,
    s.end_date,
    s.cancel_date,
    s.next_review_date,
    s.paid_through_date,
    s.plan_id,
    s.shipping_address_id,
    s.payment_source_id,
    gd.ecometry_company_id AS company,
    gd.ecometry_division_id AS division,
    gd.name AS source_name,
    ser.name AS service_name,
    sh.start_date AS hold_start_date,
    sh.end_date AS hold_end_date,
    'WEB'::character varying(255) AS origin_type,
    es.use AS origin,
    es.offer AS ecometry_offer_code,
    ord.uuid AS order_uuid,
    s.md5,
        CASE
            WHEN ((s.cancel_date IS NOT NULL) AND (s.cancel_date <= now())) THEN
            CASE
                WHEN pr.credit_card_required THEN 'Cancelled'::text
                WHEN cancelled_during_nocc_trial(s.id, s.plan_id, s.cancel_date, s.payment_source_id) THEN 'TrialCancelled'::text
                ELSE 'Cancelled'::text
            END
            WHEN ((s.payment_source_id IS NULL) AND has_payment_source_trial_hold(s.id, s.start_date, s.plan_id)) THEN 'TrialEnded'::text
            WHEN (((s.end_date IS NOT NULL) AND (s.end_date <= now())) OR (gd.deactivation_date <= now())) THEN 'Ended'::text
            WHEN ((sh.initiator)::text = ANY (ARRAY[('ADMINISTRATIVE'::character varying)::text, ('CUSTOMER'::character varying)::text])) THEN 'AdminHold'::text
            WHEN ((sh.initiator)::text = 'PAYMENT'::text) THEN 'Hold'::text
            WHEN (((seg.duration_period IS NOT NULL) AND (now() > s.start_date)) AND (now() < (s.start_date + (seg.duration_period)::interval))) THEN 'Trial'::text
            ELSE 'Active'::text
        END AS status,
    pc.code AS promotion_code,
    (((((cd.link_share_affiliate)::text || '-'::text) || (cd.channel)::text) || '-'::text) || (cd.coupon_code)::text) AS referrer_key,
    cd.link_share_affiliate AS affiliate,
    NULL::text AS affiliate_name,
    cd.channel,
    cd.coupon_code,
    NULL::character varying(64) AS ecometry_cont_order,
    NULL::character varying(64) AS ecometry_enrollment_order,
    s.subsequent_subscription_id,
    gd.deactivation_date AS gaiam_division_deactivation_date,
    go.user_id AS gift_giver_id,
    gi.message AS gift_message
   FROM ((((((((((((((((subscription s
     JOIN users u ON ((s.user_id = u.id)))
     JOIN gaiam_division gd ON ((gd.id = s.source_id)))
     LEFT JOIN t_best_active_hold sh ON ((sh.subscription_id = s.id)))
     LEFT JOIN v_subscription_creation_data cd ON ((cd.subscription_id = s.id)))
     LEFT JOIN payment_source ps ON ((ps.id = s.payment_source_id)))
     LEFT JOIN orders ord ON ((ord.id = s.order_id)))
     LEFT JOIN plan p ON ((p.id = s.plan_id)))
     LEFT JOIN product pr ON ((pr.id = p.id)))
     LEFT JOIN service ser ON ((ser.id = p.service_id)))
     LEFT JOIN order_promotion op ON ((op.order_id = ord.id)))
     LEFT JOIN promotion_code pc ON ((pc.id = op.promotion_code_id)))
     LEFT JOIN ecometry_source es ON ((es.id = gd.ecometry_source_id)))
     LEFT JOIN gift_info gi ON ((gi.gift_order_id = s.order_id)))
     LEFT JOIN orders go ON ((go.id = gi.gift_order_id)))
     LEFT JOIN plan_segments pseg ON (((pseg.plan_id = p.id) AND (pseg.segment_sequence = 0))))
     LEFT JOIN segment seg ON (((seg.id = pseg.segment_id) AND ((seg.segment_type)::text = 'TRIAL'::text))))
  WHERE (s.start_date IS NOT NULL));
