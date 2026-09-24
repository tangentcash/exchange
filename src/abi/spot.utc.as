namespace dex
{
    enum order_side { buy }
    enum order_policy { deferred_all = 1 }
    string limit_order() { return "uint256 limit_order(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, order_side side, order_policy policy, const real320&in price)"; }
    string withdraw_order() { return "void withdraw_order(pmut@, const uint256&in order_id)"; }
}

void pay_asset(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, const address&in dex_account)
{
    payable value = tx::value();
    uint256 order_id = dex_account.call<uint256>(dex::limit_order(), value, primary_asset, secondary_asset, dex::order_side::buy, dex::order_policy::deferred_all, real320(0.000000000000000001));
    dex_account.call<void>(dex::withdraw_order(), payable(), order_id);
}