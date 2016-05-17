create table tmp.refund_transactions
-- Direct Paymentech Refund Transactions
(select distinct on (cct1.retry_id)
      se.subscription_id ,
      seg.duration_period as duration,
      case
          when seg.duration_period = 'P1M' then 'Monthly Subscription'
          when seg.duration_period in ('P1Y', 'P12M') then 'Annual Subscription'
          else 'Multi month subscription' end as item,
      cct1.amount as refund_amount,
      st.transaction_date as start_date,
      cct1.transaction_number as tx_ref_num,
      cct2.amount as payment_amount
    from gcsi.gaiam_division gd
       join gcsi.subscription_transaction st on st.division_id = gd.id
       join gcsi.credit_card_transaction cct1 on cct1.id = st.id
       join gcsi.direct_capture_transaction dct on dct.refund_id = st.id
       join gcsi.credit_card_transaction cct2 on cct2.id = dct.id
       join gcsi.subscription_transaction_subscription_events stse on stse.subscription_transaction_id = dct.id
       join gcsi.subscription_event se on stse.subscription_event_id = se.id
       join gcsi.subscription_plan_event spe on spe.id = stse.subscription_event_id
       join gcsi.segment seg on seg.id = spe.segment_id
    where
      st.txn_state = 'SETTLED' and 
      gd.name = 'Gaiam TV'
    order by cct1.retry_id, st.id
UNION
-- PayPal  Refund Transactions
(select  se.subscription_id,
        seg.duration_period as duration,
        case
            when seg.duration_period = 'P1M' then 'Monthly Subscription'
            when seg.duration_period in ('P1Y', 'P12M') then 'Annual Subscription'
            else 'Multi month subscription' end as item,
        ppt.amount as refund_amount,
        st.transaction_date as start_date,
        response.transaction_id::varchar(40) as tx_ref_num,
        ppt2.amount as payment_amount
 from gcsi.gaiam_division gd
   join gcsi.subscription_transaction st on st.division_id = gd.id
   join gcsi.pay_pal_refund_transaction ppr on ppr.id = st.id
   join gcsi.pay_pal_transaction ppt on ppr.id = ppt.id
   join gcsi.pay_pal_response_data response on response.id in (select id from pay_pal_response_data dat join pay_pal_transaction_ppresponses x on x.ppresponses_id = dat.id  where x.pay_pal_transaction_id = ppr.id order by date_received desc limit 1)
   join gcsi.pay_pal_refundable_transaction pprt on pprt.refund_id = ppr.id
   join gcsi.pay_pal_transaction ppt2 on ppt2.id = pprt.id
   join gcsi.subscription_transaction initial_st on initial_st.id = pprt.id and st.txn_state = 'SETTLED'
   join gcsi.subscription_transaction_subscription_events stse on stse.subscription_transaction_id = initial_st.id
   join gcsi.subscription_event se on stse.subscription_event_id = se.id
   join gcsi.subscription_plan_event spe on spe.id = stse.subscription_event_id
   join gcsi.segment seg on seg.id = spe.segment_id
 where st.txn_state = 'SETTLED'
   and gd.name = 'Gaiam TV'
 order by st.id )
)
);
