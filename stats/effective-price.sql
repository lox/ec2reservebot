SELECT Id, FixedPrice/(Duration/60/60) + IFNULL(RecurringCharges_Amount1, UsagePrice) as EffectivePrice FROM offering;
