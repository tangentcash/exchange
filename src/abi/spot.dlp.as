namespace dex
{
    class pool
    {
        uint256 id;
        uint256 pair_id;
        address account;
        real320 primary_value;
        real320 secondary_value;
        real320 primary_revenue;
        real320 secondary_revenue;
        real320 liquidity;
        real320 price;
        real320 min_price;
        real320 max_price;
        real320 fee_rate;
        real320 exit_fee;
    }
    string deposit_pool() { return "uint256 deposit_pool(pmut@, const uint256&in, const uint256&in, const real320&in, const real320&in, const real320&in, const real320&in)"; }
    string withdraw_pool() { return "void withdraw_pool(pmut@, const uint256&in)"; }
    string pool_of() { return "pool pool_of(pconst@, const uint256&in)"; }
}

namespace dlp
{
    bool repay_liquidity(const uint256&in primary_asset, const uint256&in secondary_asset, bool emit_event)
    {
        pool_ref_event ref;
        ref.primary_asset = primary_asset;
        ref.secondary_asset = secondary_asset;
        ref.batch = true;
        require(pools.has(ref), "delegated pool not found");

        pool_state prev = pools[ref];
        dex::pool next = params.ref.dex_account.static_call<dex::pool>(dex::pool_of(), prev.id);
        params.ref.dex_account.protected_call<void>(dex::withdraw_pool(), payable(), next.id);
        pools.erase(ref);

        uint64 number = block::number();
        pool_size_owner change, global, local;
        real320 fee = real320(1) - next.exit_fee, reward = params.ref.reward_emission * real320(number > prev.block_number ? number - prev.block_number : 0) / real320(math::max<usize>(1, params.ref.permissions.size()));
        change.primary_value = (next.primary_value + next.primary_revenue * fee) - (prev.primary_value + prev.primary_revenue);
        change.secondary_value = (next.secondary_value + next.secondary_revenue * fee) - (prev.secondary_value + prev.secondary_revenue);
        if (change.primary_value.zero_or_nan() && change.secondary_value.zero_or_nan() && reward.zero())
            return false;
        
        array<pool_size_owner> deltas;
        ranging_slice slice = shares.x(ref);
        while (slice.next(local, global.owner))
        {
            local.owner = global.owner;
            global.primary_value += local.primary_value;
            global.secondary_value += local.secondary_value;
            deltas.push(local);
        }
        
        real320 total = global.primary_value * prev.price + global.secondary_value;
        real320 new_primary_total = change.primary_value + global.primary_value;
        real320 new_secondary_total = change.secondary_value + global.secondary_value;
        global.primary_value = change.primary_value = new_primary_total;
        global.secondary_value = change.secondary_value = new_secondary_total;
        usize deltas_size = total.positive() ? deltas.size() : 0;
        for (usize i = 0; i < deltas_size; i++)
        {
            pool_size_owner@ delta = deltas[i];
            real320 share = (delta.primary_value * prev.price + delta.secondary_value) / total;
            delta.primary_value = change.primary_value * share;
            delta.secondary_value = change.secondary_value * share;
            delta.reward_value += reward * share;
            global.primary_value -= delta.primary_value;
            global.secondary_value -= delta.secondary_value;
            if (i == deltas_size - 1)
            {
                delta.primary_value += global.primary_value;
                delta.secondary_value += global.secondary_value;
            }
            shares.insert(ref, delta.owner, delta);
        }
        
        bool update = !deltas.empty();
        if (update && emit_event)
            log::emit(ref);
        return update;
    }
}

class pool_state
{
    uint256 id;
    real320 price;
    real320 primary_value;
    real320 secondary_value;
    real320 primary_revenue;
    real320 secondary_revenue;
    uint64 block_number;
}

class pool_ref
{
    uint256 primary_asset;
    uint256 secondary_asset;
}

class pool_ref_event : pool_ref
{
    address owner;
    bool batch;
}

class pool_size
{
    real320 primary_value;
    real320 secondary_value;
    real320 reward_value;
}

class pool_size_owner : pool_size
{
    address owner;
}

class config
{
    address dex_account;
    address deployer_account;
    real320 reward_emission;
    real320 reward_balance;
    array<pool_ref> permissions;
}

ranging<pool_ref, address, pool_size> shares;
mapping<pool_ref, pool_state> pools;
varying<config> params;

void construct(pmut@, const address&in dex_account)
{
    config new_params;
    new_params.deployer_account = tx::from();
    new_params.dex_account = dex_account;
    reconstruct(null, new_params);
}
void reconstruct(pmut@, const config&in new_params)
{
    require(params.empty() || params.ref.deployer_account == tx::from(), "prev. deployer account must be the tx sender");
    require(!new_params.deployer_account.empty() && !new_params.dex_account.empty(), "deployer/dex accounts must not be empty");
    params = new_params;
    log::event(new_params, null);
}
void reconstruct_deployer(pmut@, const address&in deployer_account)
{
    config new_params = params.ref;
    new_params.deployer_account = deployer_account;
    reconstruct(null, new_params);
}
void reconstruct_reward(pmut@, const real320&in reward_emission)
{
    payable value = tx::value();
    require(value.empty() || (value.size() == 1 && value.has(coin::native())), "must either pay native coin or none");
    config new_params = params.ref;
    new_params.reward_emission = reward_emission;
    new_params.reward_balance += value.of(coin::native());
    reconstruct(null, new_params);
}
void reconstruct_permit(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, bool permitted)
{
    pool_ref new_ref; config new_params = params.ref;
    new_ref.primary_asset = primary_asset;
    new_ref.secondary_asset = secondary_asset;
    new_params.permissions.push(new_ref);
    for (usize i = 0; !permitted && i < new_params.permissions.size(); i++)
    {
        pool_ref@ ref = new_params.permissions[i];
        if (ref.primary_asset == primary_asset && ref.secondary_asset == secondary_asset)
        {
            new_params.permissions.erase(i);
            break;
        }
    }
    reconstruct(null, new_params);
}
uint256 transfer_liquidity(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, const real320&in primary_value, const real320&in secondary_value, const real320&in price, const real320&in min_price, const real320&in max_price, const real320&in fee_rate)
{
    pool_ref ref;
    ref.primary_asset = primary_asset;
    ref.secondary_asset = secondary_asset;
    require(!tx::paid(), "payment not permitted");
    require(params.ref.deployer_account == tx::from(), "deployer account must be the tx sender");
    if (pools.has(ref))
    {
        dlp::repay_liquidity(primary_asset, secondary_asset, true);
        require(!pools.has(ref), "failed to withdraw the pool");
    }

    payable value;
    pool_size global = liquidity_of(null, primary_asset, secondary_asset);
    value.plus(primary_asset, primary_value);
    value.plus(secondary_asset, secondary_value);
    require(global.primary_value >= primary_value, "not enough primary value available");
    require(global.secondary_value >= secondary_value, "not enough secondary value available");

    uint256 pool_id = params.ref.dex_account.protected_call<uint256>(dex::deposit_pool(), value, primary_asset, secondary_asset, price, min_price, max_price, fee_rate);
    dex::pool pool = params.ref.dex_account.static_call<dex::pool>(dex::pool_of(), pool_id);
    pool_state state;
    state.id = pool.id;
    state.price = !pool.min_price.nan() && !pool.max_price.nan() ? pool.price * pool.price : pool.price;
    state.primary_value = pool.primary_value;
    state.secondary_value = pool.secondary_value;
    state.primary_revenue = pool.primary_revenue;
    state.secondary_revenue = pool.secondary_revenue;
    state.block_number = block::number();
    pools.insert(ref, state);
    return pool_id;
}
void pull_liquidity(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset)
{
    require(!tx::paid(), "payment not permitted");
    require(params.ref.deployer_account == tx::from(), "deployer account must be the tx sender");
    dlp::repay_liquidity(primary_asset, secondary_asset, true);
}
void deposit_liquidity(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset)
{
    pool_size size; payable value = tx::value();
    for (usize i = 0; i < value.size(); i++)
    {
        uint256 asset = value[i];
        if (asset == primary_asset)
            size.primary_value = value.of(asset);
        else if (asset == secondary_asset)
            size.secondary_value = value.of(asset);
        else
            require(false, "liquidity must be paid with " + coin::name_of(primary_asset) + " and/or with " + coin::name_of(secondary_asset));
    }

    bool permitted = false;
    for (usize i = 0; !permitted && i < params.ref.permissions.size(); i++)
    {
        const pool_ref@ target = params.ref.permissions[i];
        permitted = permitted || (target.primary_asset == primary_asset && target.secondary_asset == secondary_asset);
    }

    pool_ref_event ref;
    ref.primary_asset = primary_asset;
    ref.secondary_asset = secondary_asset;
    ref.owner = tx::from();
    require(size.primary_value.positive() || size.secondary_value.positive(), "liquidity must be paid " + coin::name_of(primary_asset) + " and/or with " + coin::name_of(secondary_asset));
    require(permitted, "liquidity deposit not permitted");
    if (shares.has(ref, ref.owner))
    {
        pool_size prev_size = shares[ref, ref.owner];
        size.primary_value += prev_size.primary_value;
        size.secondary_value += prev_size.secondary_value;
        size.reward_value = prev_size.reward_value;
    }
    shares.insert(ref, ref.owner, size);
    log::emit(ref);
}
void withdraw_liquidity(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, const real320&in primary_value, const real320&in secondary_value)
{
    require(!tx::paid(), "payment not permitted");
    pool_ref_event ref;
    ref.primary_asset = primary_asset;
    ref.secondary_asset = secondary_asset;
    ref.owner = tx::from();
    
    address self = tx::to();
    pool_size size = shares.has(ref, ref.owner) ? shares[ref, ref.owner] : pool_size();
    real320 primary_request = primary_value.nan() ? size.primary_value : primary_value;
    real320 secondary_request = secondary_value.nan() ? size.secondary_value : secondary_value;
    if (self.balance_of(primary_asset) < primary_request || self.balance_of(secondary_asset) < secondary_request)
    {
        require(pools.has(ref), "not enough balance to cover withdrawal");
        ref.batch = dlp::repay_liquidity(primary_asset, secondary_asset, false);
        size = shares[ref, ref.owner];
        primary_request = primary_value.nan() ? size.primary_value : primary_value;
        secondary_request = secondary_value.nan() ? size.secondary_value : secondary_value;
    }
    
    require(primary_request.positive() || secondary_request.positive(), "primary/secondary withdrawal values must be positive");
    require(size.primary_value >= primary_request, "not enough primary value to redeem");
    require(size.secondary_value >= secondary_request, "not enough secondary value to redeem");
    require(self.balance_of(primary_asset) >= primary_request && self.balance_of(secondary_asset) >= secondary_request, "not enough balance to cover withdrawal");
    bool preserve_share = size.primary_value > primary_request || size.secondary_value > secondary_value;
    size.primary_value -= primary_request;
    size.secondary_value -= secondary_request;
    ref.owner.pay(primary_asset, primary_request);
    ref.owner.pay(secondary_asset, secondary_request);
    if (!preserve_share && size.reward_value.positive())
    {
        real320 value = math::min<real320>(size.reward_value, math::min<real320>(params.ref.reward_balance, tx::to().balance_of(coin::native())));
        config new_params = params.ref;
        new_params.reward_balance -= value;
        params = new_params;
        ref.owner.pay(coin::native(), value);
        log::event(new_params, null);
    }
    shares.insert_if(preserve_share, ref, ref.owner, size);
    log::emit(ref);
}
void withdraw_reward(pmut@)
{
    require(params.ref.deployer_account == tx::from(), "withdrawal not permitted");
    real320 value = math::min<real320>(params.ref.reward_balance, tx::to().balance_of(coin::native()));
    config new_params = params.ref;
    new_params.reward_balance = 0;
    params = new_params;
    new_params.deployer_account.pay(coin::native(), value);
    log::event(new_params, null);
}
pool_state pool_of(pconst@, const uint256&in primary_asset, const uint256&in secondary_asset)
{
    pool_ref ref;
    ref.primary_asset = primary_asset;
    ref.secondary_asset = secondary_asset;
    require(pools.has(ref), "pool not found");
    return pools[ref];
}
pool_size share_of(pconst@, const uint256&in primary_asset, const uint256&in secondary_asset, const address&in owner)
{
    pool_ref ref;
    ref.primary_asset = primary_asset;
    ref.secondary_asset = secondary_asset;
    require(shares.has(ref, owner), "share not found");
    return shares[ref, owner];
}
pool_size liquidity_of(pconst@, const uint256&in primary_asset, const uint256&in secondary_asset)
{
    pool_ref ref; pool_size local, global;
    ref.primary_asset = primary_asset;
    ref.secondary_asset = secondary_asset;
    ranging_slice slice = shares.x(ref);
    while (slice.next(local))
    {
        global.primary_value += local.primary_value;
        global.secondary_value += local.secondary_value;
    }
    return global;
}
config params_of(pconst@)
{
    return params.ref;
}