enum order_condition
{
    market,
    limit,
    stop,
    stop_limit,
    trailing_stop,
    trailing_stop_limit
}

enum order_side
{
    buy,
    sell
}

enum order_policy
{
    deferred,
    deferred_all,
    immediate,
    immediate_all
}

enum ref_type
{
    order,
    pool
}

enum ref_flag
{
    synced = 1 << 0,
    erased = 1 << 1,
    forced = 1 << 2,
    match = 1 << 3
}

class asset_pair
{
    uint256 id;
    uint256 primary_asset;
    uint256 secondary_asset;
}

class asset_pair_ref
{
    uint256 id;
    order_side side;

    asset_pair_ref() { }
    asset_pair_ref(const uint256&in new_id, order_side new_side)
    {
        id = new_id;
        side = new_side;
    }
}

class asset_tier
{
    uint64 block_number;
    uint256 asset;
    real320 volume;
    string symbol;

    uint256 poly_asset() const
    {
        return symbol.empty() ? asset : coin::token(symbol);
    }
    bool poly() const
    {
        return !symbol.empty();
    }
}

class account_tier
{
    uint64 block_number = 0;
    real320 volume = 0;
    
    real320 maker_fee(const asset_tier&in asset) const
    {
        real320 limit = asset.volume * params.ref.asset_volume_target;
        real320 ratio = math::pow<real320>(limit > 0 ? math::min<real320>(1, volume / limit) : 0, params.ref.maker_fee_exponent);
        return math::lerp<real320>(params.ref.max_maker_fee, params.ref.min_maker_fee, ratio);
    }
    real320 taker_fee(const asset_tier&in asset) const
    {
        real320 limit = asset.volume * params.ref.asset_volume_target;
        real320 ratio = math::pow<real320>(limit > 0 ? math::min<real320>(1, volume / limit) : 0, params.ref.taker_fee_exponent);
        return math::lerp<real320>(params.ref.max_taker_fee, params.ref.min_taker_fee, ratio);
    }
}

class account_asset_tier
{
    account_tier account;
    real320 maker_fee;
    real320 taker_fee;
}

class order
{
    uint256 id;
    uint256 pair_id;
    address account;
    order_condition condition;
    order_side side;
    order_policy policy;
    real320 price;
    real320 stop_price;
    real320 value;
    real320 slippage;
    real320 trailing_step;
    real320 trailing_distance;
    
    real320 worst_price(const real320&in market_price) const
    {
        if (slippage.nan() || market_price.nan())
            return market_price;

        real320 distance = slippage.negative() ? slippage * -market_price : slippage;
        return side == order_side::buy ? market_price + distance : math::max<real320>(market_price - distance, 0);
    }
    real320 best_price() const
    {
        if (condition != order_condition::market || price.nan() || slippage.nan())
            return price;
        
        return side == order_side::buy ? (slippage.negative() ? price / (real320(1) - slippage) : math::max<real320>(price - slippage, 0)) : (slippage.negative() ? price / (real320(1) + slippage) : price + slippage);
    }
    real320 best_stop_price(const real320&in market_price) const
    {
        if (trailing_step.nan() || trailing_distance.nan())
            return stop_price;

        bool buy = side == order_side::buy;
        real320 step = trailing_step.negative() ? trailing_step * -stop_price : trailing_step;
        real320 distance = trailing_distance.negative() ? trailing_distance * -stop_price : trailing_distance;
        real320 new_stop_price = buy ? market_price + distance : math::max<real320>(market_price - distance, 0);
        return buy ? (stop_price - new_stop_price >= step ? new_stop_price : stop_price) : (new_stop_price - stop_price >= step ? new_stop_price : stop_price);
    }
    real320 best_trigger_price() const
    {
        if (trailing_step.nan() || trailing_distance.nan())
            return stop_price;

        real320 delta = (trailing_step.negative() ? trailing_step * -stop_price : trailing_step) + (trailing_distance.negative() ? trailing_distance * -stop_price : trailing_distance);
        return side == order_side::buy ? stop_price - delta : math::max<real320>(stop_price + delta, 0);
    }
    real320 side_value(order_side target_side, const real320&in market_price) const
    {
        if (side == target_side)
            return value;
        
        return side == order_side::buy ? (value / market_price) : (value * market_price);
    }
    bool all_or_none() const
    {
        return policy == order_policy::deferred_all || policy == order_policy::immediate_all;
    }
    bool immediate() const
    {
        return policy == order_policy::immediate || policy == order_policy::immediate_all;
    }
    bool deferred() const
    {
        return policy == order_policy::deferred || policy == order_policy::deferred_all;
    }
}

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

    real320 index_price(order_side side) const
    {
        if (!primary_value.positive() && !secondary_value.positive())
            return real320::enan();
        
        bool sqrt_price = concentrated();
        real320 result = sqrt_price ? price * price : price;
        result *= real320(1) + (side == order_side::buy ? -fee_rate : fee_rate);
        if (sqrt_price && (result < min_price * min_price || result > max_price * max_price))
            return real320::enan();
                
        return result;
    }
    real320 concentrated_amount0(const real320&in sqrt_price)
    {
        return liquidity * math::max<real320>(0, max_price - sqrt_price) / sqrt_price / max_price;
    }
    real320 concentrated_amount1(const real320&in sqrt_price)
    {
        return liquidity * math::max<real320>(0, sqrt_price - min_price);
    }
    bool concentrated() const
    {
        return !min_price.nan() && !max_price.nan();
    }
}

class mixed_id
{
    uint256 value;
    ref_type type;

    mixed_id() { }
    mixed_id(ref_type new_type, const uint256&in new_value)
    {
        value = new_value;
        type = new_type;
    }
}

class mixed_ptr
{
    ref_type type;
    order@ order_ptr;
    pool@ pool_ptr;
    uint8 flags;
    
    mixed_ptr() { }
    mixed_ptr(order@ target, uint8 new_flags = 0)
    {
        require(target !is null, "order ptr must not be null");
        type = ref_type::order;
        flags = new_flags;
        @order_ptr = target;
    }
    mixed_ptr(pool@ target, uint8 new_flags = 0)
    {
        require(target !is null, "pool ptr must not be null");
        type = ref_type::pool;
        flags = new_flags;
        @pool_ptr = target;
    }
    uint256 id() const
    {
        if (type == ref_type::order)
            return order_ptr !is null ? order_ptr.id : 0;
        else if (type == ref_type::pool)
            return pool_ptr !is null ? pool_ptr.id : 0;
        return 0;
    }
}

class swap
{
    uint256 maker_order_or_pool_id;
    uint256 taker_order_id;
    ref_type type;
    order_side side;
    real320 price;
    real320 quantity;
}

class trade
{
    real320 price;
    real320 primary_value;
    real320 secondary_value;
    order@ taker_order;
    order@ maker_order;
    pool@ maker_pool;

    swap to_swap() const
    {
        swap log;
        log.maker_order_or_pool_id = maker_pool is null ? maker_order.id : maker_pool.id;
        log.taker_order_id = taker_order.id;
        log.type = maker_pool is null ? ref_type::order : ref_type::pool;
        log.side = taker_order.side;
        log.price = price;
        log.quantity = primary_value;
        return log;
    }
}

class context
{
    array<order@> stack;
    array<mixed_ptr> ptrs;
    array<trade> trades;
    
    void insert(const mixed_ptr&in target)
    {
        uint256 id = target.id();
        for (usize i = 0; i < ptrs.size(); i++)
        {
            mixed_ptr@ ptr = ptrs[i];
            if (ptr.id() != id)
                continue;
                
            ptr = target;
            if (ptr.order_ptr !is null && (ptr.flags & ref_flag::forced) > 0 && (ptr.flags & ref_flag::match) > 0)
                stack.push(@ptr.order_ptr);
            return;
        }

        ptrs.push(target);
        if ((target.flags & ref_flag::match) > 0 && target.order_ptr !is null)
            stack.push(@target.order_ptr);
    }
}

class config
{
    address hook_account;
    address deployer_account;
    real320 pool_exit_fee = 0.00;
    real320 max_pool_fee_rate = 0.01;
    real320 min_maker_fee = 0.0000;
    real320 max_maker_fee = 0.0000;
    real320 maker_fee_exponent = 3;
    real320 min_taker_fee = 0.0000;
    real320 max_taker_fee = 0.0000;
    real320 taker_fee_exponent = 4;
    real320 asset_volume_target = 0.400;
    uint64 asset_reset_days = 90;
    uint64 account_reset_days = 30;
}

ranging<address, uint256, order> orders;
ranging<address, uint256, pool> pools;
ranging<asset_pair_ref, uint256, mixed_id> limits;
ranging<asset_pair_ref, uint256, uint256> stops;
ranging<asset_pair_ref, uint256, uint256> triggers;
ranging<address, uint256, account_tier> accounts;
ranging<asset_pair, uint256, bool> pairs;
mapping<uint256, asset_tier> assets;
varying<uint256> tracker;
varying<config> params;

namespace dex
{
    uint256 use_id()
    {
        uint256 id = tracker.empty() ? 1 : tracker.ref;
        tracker = id + 1;
        return id;
    }
    asset_pair use_pair(const uint256&in new_primary_asset, const uint256&in new_secondary_asset, bool may_allocate)
    {
        asset_pair pair;
        asset_tier primary_asset = asset_of(null, new_primary_asset);
        asset_tier secondary_asset = asset_of(null, new_secondary_asset);
        pair.primary_asset = primary_asset.poly_asset();
        pair.secondary_asset = secondary_asset.poly_asset();
        require(pair.primary_asset != pair.secondary_asset, "primary/secondary assets must be distinct");
        if (!pairs.x(pair).next(null, pair.id) && may_allocate)
        {
            uint256 id = use_id();
            pairs.insert(pair, id, false);
            pair.id = id;
        }
        return pair;
    }
    context use_order(const address&in account, const payable&in value, const asset_pair&in pair, order_side side, order_policy policy, order_condition condition, const real320&in price, const real320&in stop_price, const real320&in slippage, const real320&in trailing_step, const real320&in trailing_distance)
    {
        order use;
        use.pair_id = pair.id;
        use.account = account;
        use.side = side;
        use.policy = policy;
        use.condition = condition;
        use.price = price;
        use.stop_price = stop_price;
        use.slippage = slippage;
        use.trailing_step = trailing_step;
        use.trailing_distance = trailing_distance;
        
        uint256 paying_asset = use.side == order_side::buy ? pair.secondary_asset : pair.primary_asset;
        for (usize i = 0; i < value.size(); i++)
        {
            asset_tier paying = asset_of(null, value[i]);
            require(paying.poly_asset() == paying_asset, "order must be paid with " + coin::name_of(paying_asset));
            use.value += value.of(paying.asset);
            if (paying.poly())
                tx::to().mint(paying.symbol, value.of(paying.asset));
        }

        require(!use.account.empty(), "order account must be set");
        require(use.side == order_side::buy || use.side == order_side::sell, "order side mismatch");
        require(use.policy == order_policy::deferred || use.policy == order_policy::deferred_all || use.policy == order_policy::immediate || use.policy == order_policy::immediate_all, "order execution policy mismatch");
        require(use.value.positive(), "order must be paid with " + coin::name_of(paying_asset));
        switch (use.condition)
        {
            case order_condition::market:
            {
                require(use.immediate(), "order must use immediate execution policy");
                require(use.price.positive(), "order must have slippage price");
                require(use.stop_price.nan(), "order must not have stop price");
                require(use.trailing_step.nan() && use.trailing_distance.nan(), "order must not have trailing step/distance");
                real320 slippage_price = to_level_price(use.pair_id, use.side);
                use.slippage = use.side == order_side::buy ? (use.price - slippage_price) : (slippage_price - use.price);
                require(slippage_price.positive() && !use.slippage.negative(), "order has nothing to match against");
                break;
            }
            case order_condition::limit:
                require(use.deferred(), "order must use deferred execution policy");
                require(use.price.positive(), "order must have price");
                require(use.stop_price.nan(), "order must not have stop price");
                require(use.slippage.nan(), "order must not have slippage");
                require(use.trailing_step.nan() && use.trailing_distance.nan(), "order must not have trailing step/distance");
                break;
            case order_condition::stop:
                require(use.deferred(), "order must use deferred execution policy");
                require(use.price.nan(), "order must not have price");
                require(use.stop_price.positive(), "order must have stop price");
                require(!use.slippage.nan(), "order must have slippage");
                require(use.trailing_step.nan() && use.trailing_distance.nan(), "order must not have trailing step/distance");
                break;
            case order_condition::stop_limit:
                require(use.deferred(), "order must use deferred execution policy");
                require(use.price.positive(), "order must have price");
                require(use.stop_price.positive(), "order must have stop price");
                require(use.slippage.nan(), "order must not have slippage");
                require(use.trailing_step.nan() && use.trailing_distance.nan(), "order must not have trailing step/distance");
                break;
            case order_condition::trailing_stop:
            {
                require(use.deferred(), "order must use deferred execution policy");
                require(use.price.nan(), "order must not have price");
                require(use.stop_price.positive(), "order must not have stop price");
                require(!use.slippage.nan(), "order must have slippage");
                require(!use.trailing_step.nan() && !use.trailing_distance.nan(), "order must have trailing step/distance");
                real320 target_price = to_level_price(use.pair_id, use.side);
                use.stop_price = target_price.positive() ? use.best_stop_price(target_price) : use.stop_price;
                break;
            }
            case order_condition::trailing_stop_limit:
            {
                require(use.deferred(), "order must use deferred execution policy");
                require(use.price.positive(), "order must have price");
                require(use.stop_price.positive(), "order must have stop price");
                require(use.slippage.nan(), "order must not have slippage");
                require(!use.trailing_step.nan() && !use.trailing_distance.nan(), "order must have trailing step/distance");
                real320 target_price = to_level_price(use.pair_id, use.side);
                use.stop_price = target_price.positive() ? use.best_stop_price(target_price) : use.stop_price;
                break;
            }
            default:
                require(false, "order condition mismatch");
                return context();
        }

        array<mixed_ptr> ptrs;
        context ctx;
        ctx.stack.push(@use);
        while (!ctx.stack.empty())
        {
            order@ taker = ctx.stack.front();
            usize prev_trades = ctx.trades.size();
            ctx.insert(mixed_ptr(taker));
            if ((taker.condition == order_condition::market || taker.condition == order_condition::limit) && taker.value.positive())
            {
                mixed_id id; real320 taker_value = taker.value, trigger_price = real320::enan();
                order_side anti_side = taker.side == order_side::buy ? order_side::sell : order_side::buy;
                ranging_slice slice = to_price_slice(limits.x(asset_pair_ref(pair.id, anti_side)), taker.side, taker.price);
                while (taker.value.positive() && slice.next(id))
                {
                    if (id.type == ref_type::order)
                    {
                        order maker;
                        if (!orders.y(id.value).next(maker) || taker.account == maker.account)
                            continue;
                        
                        real320 match_price = taker.side == order_side::buy ? math::max<real320>(maker.best_price(), taker.best_price()) : math::min<real320>(maker.best_price(), taker.best_price());
                        real320 taker_primary_value = taker.side_value(order_side::sell, match_price), maker_primary_value = maker.side_value(order_side::sell, match_price);
                        real320 taker_secondary_value = taker.side_value(order_side::buy, match_price), maker_secondary_value = maker.side_value(order_side::buy, match_price);
                        if (maker.all_or_none() && (taker_primary_value < maker_primary_value || taker_secondary_value < maker_secondary_value))
                            continue;

                        trade next;
                        @next.taker_order = taker;
                        @next.maker_order = maker;
                        next.price = match_price;    
                        next.primary_value = math::min<real320>(taker_primary_value, maker_primary_value);
                        next.secondary_value = math::min<real320>(taker_secondary_value, maker_secondary_value);
                        maker.value -= taker.side == order_side::sell ? next.secondary_value : next.primary_value;
                        taker.value -= taker.side == order_side::sell ? next.primary_value : next.secondary_value;
                        ptrs.push(mixed_ptr(next.maker_order));
                        ctx.trades.push(next);
                    }
                    else if (id.type == ref_type::pool)
                    {
                        pool maker;
                        if (!pools.y(id.value).next(maker) || taker.account == maker.account)
                            continue;
                        
                        real320 fee_rate = real320(1) - maker.fee_rate;
                        real320 amount_in, amount_in_after_fee, amount_out;
                        if (taker.side == order_side::buy)
                        {
                            amount_out = maker.primary_value;
                            if (maker.concentrated())
                            {
                                amount_in = math::min<real320>(taker.value, maker.concentrated_amount1(math::sqrt<real320>(taker.price)) - maker.secondary_value);
                                amount_in_after_fee = amount_in * fee_rate;
                                maker.secondary_value = maker.secondary_value + amount_in_after_fee;
                                maker.price = (maker.secondary_value + maker.liquidity * maker.min_price) / maker.liquidity;
                                maker.primary_value = maker.concentrated_amount0(maker.price);
                            }
                            else
                            {
                                amount_in = math::min<real320>(taker.value, math::sqrt<real320>(maker.liquidity / taker.price) * taker.price - maker.secondary_value);
                                amount_in_after_fee = amount_in * fee_rate;
                                maker.secondary_value = maker.secondary_value + amount_in_after_fee;
                                maker.primary_value = maker.liquidity / maker.secondary_value;
                                maker.price = maker.secondary_value / maker.primary_value;
                            }
                            amount_out -= maker.primary_value;
                            maker.secondary_revenue += amount_in - amount_in_after_fee;
                        }
                        else
                        {
                            amount_out = maker.secondary_value;
                            if (maker.concentrated())
                            {
                                amount_in = math::min<real320>(taker.value, maker.concentrated_amount0(math::sqrt<real320>(taker.price)) - maker.primary_value);
                                amount_in_after_fee = amount_in * fee_rate;
                                maker.primary_value = maker.primary_value + amount_in_after_fee;
                                maker.price = (maker.liquidity * maker.max_price) / (maker.primary_value * maker.max_price + maker.liquidity);
                                maker.secondary_value = maker.concentrated_amount1(maker.price);
                            }
                            else
                            {
                                amount_in = math::min<real320>(taker.value, math::sqrt<real320>(maker.liquidity / taker.price) - maker.primary_value);
                                amount_in_after_fee = amount_in * fee_rate;
                                maker.primary_value = maker.primary_value + amount_in_after_fee;
                                maker.secondary_value = maker.liquidity / maker.primary_value;
                                maker.price = maker.secondary_value / maker.primary_value;
                            }
                            amount_out -= maker.secondary_value;
                            maker.primary_revenue += amount_in - amount_in_after_fee;
                        }

                        bool within_reserve_range = maker.price.positive() && maker.primary_value.positive() && maker.secondary_value.positive();
                        bool within_price_range = !maker.concentrated() || (maker.price >= maker.min_price && maker.price <= maker.max_price);
                        if (!amount_in_after_fee.positive() || !within_reserve_range || !within_price_range)
                            continue;
                        
                        real320 index_price = maker.index_price(taker.side);
                        if (index_price.positive() && (trigger_price.nan() || (taker.side == order_side::buy ? index_price < trigger_price : index_price > trigger_price)))
                            trigger_price = index_price;
                        
                        trade next;
                        @next.taker_order = taker;
                        @next.maker_pool = maker;
                        next.price = taker.side == order_side::buy ? (amount_in_after_fee / amount_out) : (amount_out / amount_in_after_fee);
                        next.primary_value = taker.side == order_side::sell ? amount_in_after_fee : amount_out;
                        next.secondary_value = taker.side == order_side::sell ? amount_out : amount_in_after_fee;
                        taker.value -= amount_in;
                        ptrs.push(mixed_ptr(next.maker_pool));
                        ctx.trades.push(next);
                    }
                }

                if (!taker.all_or_none() || !taker.value.positive())
                {
                    slice = to_price_slice(limits.x(asset_pair_ref(pair.id, anti_side)), taker.side, trigger_price);
                    while (!trigger_price.nan() && slice.next(id))
                    {
                        order trigger_order;
                        if (id.type == ref_type::order && orders.y(id.value).next(trigger_order))
                            ptrs.push(mixed_ptr(trigger_order, ref_flag::match));
                    }
                    for (usize i = 0; i < ptrs.size(); i++)
                        ctx.insert(ptrs[i]);
                }
                else
                {
                    taker.value = taker_value;
                    ctx.trades.resize(prev_trades);
                }
                ptrs.clear();
            }
            
            step_context(ctx);
            ctx.stack.pop_front();
            if (prev_trades >= ctx.trades.size())
                continue;

            order trigger_order; real320 last_price = ctx.trades.back().price;
            asset_pair_ref buy_ref = asset_pair_ref(pair.id, order_side::buy);
            asset_pair_ref sell_ref = asset_pair_ref(pair.id, order_side::sell);
            ranging_slice buy_slice = to_price_slice(triggers.x(buy_ref), order_side::sell, last_price);
            ranging_slice sell_slice = to_price_slice(triggers.x(sell_ref), order_side::buy, last_price);
            while ((buy_slice.next(trigger_order.id) || sell_slice.next(trigger_order.id)) && orders.y(trigger_order.id).next(trigger_order))
            {
                order new_order = trigger_order;
                new_order.stop_price = new_order.best_stop_price(last_price);
                if (new_order.stop_price != trigger_order.stop_price)
                    ctx.insert(mixed_ptr(new_order));
            }

            step_context(ctx);
            buy_slice = to_price_slice(stops.x(buy_ref), order_side::buy, last_price);
            sell_slice = to_price_slice(stops.x(sell_ref), order_side::sell, last_price);
            while ((buy_slice.next(trigger_order.id) || sell_slice.next(trigger_order.id)) && orders.y(trigger_order.id).next(trigger_order))
            {
                if (trigger_order.condition == order_condition::stop || trigger_order.condition == order_condition::trailing_stop)
                {
                    order new_order = trigger_order;
                    new_order.condition = order_condition::market;
                    new_order.policy = new_order.policy == order_policy::deferred ? order_policy::immediate : order_policy::immediate_all;
                    new_order.price = new_order.worst_price(to_level_price(trigger_order.pair_id, trigger_order.side));
                    new_order.stop_price = real320::enan();
                    new_order.trailing_step = real320::enan();
                    new_order.trailing_distance = real320::enan();
                    ctx.insert(mixed_ptr(new_order, new_order.price.nan() ? 0 : (ref_flag::forced | ref_flag::match)));
                }
                else if (trigger_order.condition == order_condition::stop_limit || trigger_order.condition == order_condition::trailing_stop_limit)
                {
                    order new_order = trigger_order;
                    new_order.condition = order_condition::limit;
                    new_order.stop_price = real320::enan();
                    new_order.trailing_step = real320::enan();
                    new_order.trailing_distance = real320::enan();
                    ctx.insert(mixed_ptr(new_order, ref_flag::forced | ref_flag::match));
                }
            }
            step_context(ctx);
        }
        return ctx;
    }
    context use_pool(const address&in account, const payable&in value, const asset_pair&in pair, const real320&in price, const real320&in min_price, const real320&in max_price, const real320&in fee_rate, const real320&in exit_fee)
    {
        pool use;
        use.account = account;
        use.pair_id = pair.id;
        use.primary_value = 0;
        use.secondary_value = 0;
        use.price = min_price.positive() && max_price.positive() ? math::sqrt<real320>(price) : price;
        use.min_price = min_price.positive() ? math::sqrt<real320>(min_price) : real320::enan();
        use.max_price = max_price.positive() ? math::sqrt<real320>(max_price) : real320::enan();
        use.fee_rate = fee_rate;
        use.exit_fee = exit_fee;

        for (usize i = 0; i < value.size(); i++)
        {
            asset_tier paying = asset_of(null, value[i]);
            uint256 poly_asset = paying.poly_asset();
            require(poly_asset == pair.primary_asset || poly_asset == pair.secondary_asset, "pool must be paid with " + coin::name_of(pair.primary_asset) + " and " + coin::name_of(pair.secondary_asset));
            (poly_asset == pair.primary_asset ? use.primary_value : use.secondary_value) += value.of(paying.asset);
            if (paying.poly())
                tx::to().mint(paying.symbol, value.of(paying.asset));
        }

        real320 secondary_value_target;
        require(!use.account.empty(), "pool account must be set");
        require(use.primary_value.positive(), "pool must be paid with " + coin::name_of(pair.primary_asset));
        require(use.secondary_value.positive(), "pool must be paid with " + coin::name_of(pair.secondary_asset));
        require(use.price.positive() && (!use.concentrated() || (use.price >= use.min_price && use.price <= use.max_price)), "pool price is not in correct range");
        require(!use.concentrated() || (use.min_price.positive() && use.max_price.positive() && use.min_price <= use.max_price), "pool must have correct range of price");
        require(use.fee_rate >= 0 && use.fee_rate <= params.ref.max_pool_fee_rate, "pool fee rate must be within [0%; max_pool_fee_rate]");
        require(use.exit_fee >= 0 && use.exit_fee <= 1, "pool exit fee must be within [0%; 100%]");
        if (use.concentrated())
        {
            use.liquidity = use.primary_value * use.price * use.max_price / math::max<real320>(0, use.max_price - use.price);
            secondary_value_target = use.concentrated_amount1(use.price);
        }
        else
        {
            secondary_value_target = use.primary_value * use.price;
            use.liquidity = use.primary_value * secondary_value_target;
        }
        require(use.secondary_value >= secondary_value_target, "pool under-pays the secondary value");
        require(use.liquidity.positive(), "pool liquidity falls of the curve (incorrect primary/secondary ratio)");
        use.secondary_revenue = use.secondary_value - secondary_value_target;
        use.secondary_value = secondary_value_target;

        context ctx;
        ctx.insert(mixed_ptr(use));
        step_context(ctx);
        return ctx;
    }
    void step_context(context&inout ctx)
    {
        for (usize i = 0; i < ctx.ptrs.size(); i++)
        {
            mixed_ptr@ ptr = ctx.ptrs[i];
            if ((ptr.flags & ref_flag::synced) > 0)
                continue;
            
            bool active = !((ptr.flags & ref_flag::erased) > 0);
            if (ptr.type == ref_type::order)
            {
                order@ ref = @ptr.order_ptr;
                active = active && !ref.immediate() && ref.value.positive();
                ref.id = !active || ref.id ? ref.id : use_id();
                if (ref.id > 0)
                {
                    asset_pair_ref level = asset_pair_ref(ref.pair_id, ref.side);
                    bool limit = active && ref.condition == order_condition::limit;
                    bool stop = active && (ref.condition == order_condition::stop || ref.condition == order_condition::stop_limit || ref.condition == order_condition::trailing_stop || ref.condition == order_condition::trailing_stop_limit);
                    bool trigger = active && (ref.condition == order_condition::trailing_stop || ref.condition == order_condition::trailing_stop_limit);
                    orders.insert_if(active, ref.account, ref.id, ref);
                    limits.insert_if(limit, level, ref.id, mixed_id(ref_type::order, ref.id), limit ? alg::to_r256(ref.price) : 0);
                    stops.insert_if(stop, level, ref.id, ref.id, stop ? alg::to_r256(ref.stop_price) : 0);
                    triggers.insert_if(trigger, level, ref.id, ref.id, trigger ? alg::to_r256(ref.best_trigger_price()) : 0);
                }
            }
            else if (ptr.type == ref_type::pool)
            {
                pool@ ref = @ptr.pool_ptr;
                ref.id = ref.id ? ref.id : use_id();
        
                mixed_id id = mixed_id(ref_type::pool, ref.id);
                real320 bid_price = active ? ref.index_price(order_side::buy) : real320::enan();
                real320 ask_price = active ? ref.index_price(order_side::sell) : real320::enan();
                pools.insert_if(active, ref.account, ref.id, ref);
                limits.insert_if(!bid_price.nan(), asset_pair_ref(ref.pair_id, order_side::buy), ref.id, id, bid_price.nan() ? 0 : alg::to_r256(bid_price));
                limits.insert_if(!ask_price.nan(), asset_pair_ref(ref.pair_id, order_side::sell), ref.id, id, ask_price.nan() ? 0 : alg::to_r256(ask_price));
            }
            ptr.flags = ref_flag::synced | (active ? 0 : ref_flag::erased);
        }
    }
    uint256 pay_context(const asset_pair&in pair, const context&in ctx)
    {
        batch_payout payout;
        for (usize i = 0; i < ctx.ptrs.size(); i++)
        {
            const mixed_ptr@ ptr = ctx.ptrs[i];
            if (ptr.type == ref_type::order)
            {
                log::event(ptr.order_ptr, ptr.order_ptr.id);
                if ((ptr.flags & ref_flag::erased) > 0)
                    payout.to(ptr.order_ptr.account, ptr.order_ptr.side == order_side::buy ? pair.secondary_asset : pair.primary_asset, ptr.order_ptr.value);
            }
            else if (ptr.type == ref_type::pool)
            {
                log::event(ptr.pool_ptr, ptr.pool_ptr.id);
                if (!((ptr.flags & ref_flag::erased) > 0))
                    continue;
                    
                real320 primary_fee = ptr.pool_ptr.primary_revenue * ptr.pool_ptr.exit_fee, secondary_fee = ptr.pool_ptr.secondary_revenue * ptr.pool_ptr.exit_fee;
                payout.to(ptr.pool_ptr.account, pair.primary_asset, ptr.pool_ptr.primary_value + ptr.pool_ptr.primary_revenue - primary_fee);
                payout.to(ptr.pool_ptr.account, pair.secondary_asset, ptr.pool_ptr.secondary_value + ptr.pool_ptr.secondary_revenue - secondary_fee);
                payout.to(params.ref.deployer_account, pair.primary_asset, primary_fee);
                payout.to(params.ref.deployer_account, pair.secondary_asset, secondary_fee);
            }
        }

        if (!ctx.trades.empty())
        {
            asset_tier primary_tier = asset_of(null, pair.primary_asset);
            asset_tier secondary_tier = asset_of(null, pair.secondary_asset);
            for (usize i = 0; i < ctx.trades.size(); i++)
            {
                const trade@ next = ctx.trades[i];
                bool taker_buys = next.taker_order.side == order_side::buy;
                uint256 maker_asset = taker_buys ? pair.primary_asset : pair.secondary_asset;
                uint256 taker_asset = taker_buys ? pair.secondary_asset : pair.primary_asset;
                real320 maker_value = taker_buys ? next.primary_value : next.secondary_value;
                real320 taker_value = taker_buys ? next.secondary_value : next.primary_value;
                address maker_account = next.maker_pool is null ? next.maker_order.account : next.maker_pool.account;
                account_tier maker_tier = account_of(null, maker_account, maker_asset);
                account_tier taker_tier = account_of(null, next.taker_order.account, taker_asset);
                if (next.maker_order !is null)
                {
                    real320 maker_fee = taker_value * maker_tier.maker_fee(taker_buys ? primary_tier : secondary_tier);
                    real320 taker_fee = maker_value * taker_tier.taker_fee(taker_buys ? secondary_tier : primary_tier);
                    payout.to(next.maker_order.account, taker_asset, taker_value - maker_fee);
                    payout.to(next.taker_order.account, maker_asset, maker_value - taker_fee);
                    payout.to(params.ref.deployer_account, pair.primary_asset, taker_buys ? taker_fee : maker_fee);
                    payout.to(params.ref.deployer_account, pair.secondary_asset, taker_buys ? maker_fee : taker_fee);
                }
                else if (next.maker_pool !is null)
                    payout.to(next.taker_order.account, maker_asset, maker_value);
                primary_tier.volume += taker_buys ? maker_value : taker_value;
                secondary_tier.volume += taker_buys ? taker_value : maker_value;
                maker_tier.volume += maker_value;
                taker_tier.volume += taker_value;
                maker_tier.block_number = taker_tier.block_number = block::number();
                accounts.insert_if(maker_tier.volume.positive(), maker_account, maker_asset, maker_tier);
                accounts.insert_if(taker_tier.volume.positive(), next.taker_order.account, taker_asset, taker_tier);
                log::emit(next.to_swap());
            }
            primary_tier.block_number = secondary_tier.block_number = block::number();
            assets.insert(primary_tier.asset, primary_tier);
            assets.insert(secondary_tier.asset, secondary_tier);
        }

        payout.pay();
        if (!params.ref.hook_account.empty())
            params.ref.hook_account.call<void>("void swap_hook(pmut@)", payable());

        return ctx.ptrs.empty() ? 0 : ctx.ptrs.front().id();
    }
    real320 to_level_price(const uint256&in pair_id, order_side side)
    {
        uint256 result;
        return to_price_slice(limits.x(asset_pair_ref(pair_id, side == order_side::buy ? order_side::sell : order_side::buy)), side).next(null, null, result) ? alg::from_r256(result) : real320::enan();
    }
    ranging_slice to_price_slice(ranging_slice&in slice, order_side side, const real320&in price = real320::enan())
    {
        uint256 limit = price.nan() ? 0 : alg::to_r256(price);
        return side == order_side::buy ? slice.lte(price.nan() ? math::max_value<uint256>() : limit).asc() : slice.gte(price.nan() ? 0 : limit).desc();
    }
}

void construct(pmut@)
{
    config new_params;
    new_params.deployer_account = tx::from();
    reconstruct(null, new_params);
}
void reconstruct(pmut@, const config&in new_params)
{
    require(!tx::paid(), "payment not permitted");
    require(params.empty() || params.ref.deployer_account == tx::from(), "prev. deployer account must be the tx sender");
    require(new_params.hook_account.empty() || new_params.hook_account.callable("void swap_hook(pmut@)"), "hook account not pormitted");
    require(new_params.pool_exit_fee >= 0 && new_params.pool_exit_fee <= 1, "pool exit fee must be within [0%; 100%]");
    require(new_params.max_pool_fee_rate >= 0 && new_params.max_pool_fee_rate <= 1, "max pool fee rate must be within [0%; 100%]");
    require(new_params.min_maker_fee >= 0 && new_params.min_maker_fee <= new_params.max_maker_fee, "min maker fee must be within [0%; max_maker_fee]");
    require(new_params.max_maker_fee <= 1 && new_params.max_maker_fee >= new_params.min_maker_fee, "max maker fee must be within [min_maker_fee; 100%]");
    require(new_params.maker_fee_exponent >= 1, "maker fee exponent must be within [1; +inf)");
    require(new_params.min_taker_fee >= 0 && new_params.min_taker_fee <= new_params.max_taker_fee, "min taker fee must be within [0%; max_taker_fee]");
    require(new_params.max_taker_fee <= 1 && new_params.max_taker_fee >= new_params.min_taker_fee, "max taker fee must be within [min_taker_fee; 100%]");
    require(new_params.taker_fee_exponent >= 1, "taker fee exponent must be within [1; +inf)");
    require(new_params.asset_reset_days >= 1 && new_params.account_reset_days >= 1, "asset/account reset days must both be within [1; +inf)");
    params = new_params;
    log::event(new_params, null);
}
void unify_asset(pmut@, const uint256&in asset, const string&in symbol)
{
    asset_tier tier = asset_of(null, asset);
    require(!tx::paid(), "payment not permitted");
    require(params.ref.deployer_account == tx::from(), "setup not permitted");
    tier.symbol = symbol;
    tier.block_number = block::number();
    assets.insert(tier.asset, tier);
    log::event(tier, tier.asset);
}
void repay_asset(pmut@, const uint256&in repayment_asset)
{
    payable value = tx::value();
    require(value.size() == 1, "payment not permitted");
    uint256 payment_asset = value[0];
    real320 payment_value = value.of(payment_asset);
    asset_tier tier = asset_of(null, repayment_asset);
    require(tier.poly_asset() == payment_asset, "cannot repay asset with " + coin::name_of(tier.asset));
    tx::to().burn(coin::token_of(payment_asset), payment_value);
    tx::from().pay(tier.asset, payment_value);
}
uint256 market_order(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, order_side side, order_policy policy, const real320&in slippage_price)
{
    asset_pair pair = dex::use_pair(primary_asset, secondary_asset, true);
    return dex::pay_context(pair, dex::use_order(tx::from(), tx::value(), pair, side, policy, order_condition::market, slippage_price, real320::enan(), real320::enan(), real320::enan(), real320::enan()));
}
uint256 limit_order(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, order_side side, order_policy policy, const real320&in price)
{
    asset_pair pair = dex::use_pair(primary_asset, secondary_asset, true);
    return dex::pay_context(pair, dex::use_order(tx::from(), tx::value(), pair, side, policy, order_condition::limit, price, real320::enan(), real320::enan(), real320::enan(), real320::enan()));
}
uint256 stop_order(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, order_side side, order_policy policy, const real320&in stop_price, const real320&in slippage)
{
    asset_pair pair = dex::use_pair(primary_asset, secondary_asset, true);
    return dex::pay_context(pair, dex::use_order(tx::from(), tx::value(), pair, side, policy, order_condition::stop, real320::enan(), stop_price, slippage, real320::enan(), real320::enan()));
}
uint256 stop_limit_order(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, order_side side, order_policy policy, const real320&in stop_price, const real320&in price)
{
    asset_pair pair = dex::use_pair(primary_asset, secondary_asset, true);
    return dex::pay_context(pair, dex::use_order(tx::from(), tx::value(), pair, side, policy, order_condition::stop_limit, price, stop_price, real320::enan(), real320::enan(), real320::enan()));
}
uint256 trailing_stop_order(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, order_side side, order_policy policy, const real320&in stop_price, const real320&in slippage, const real320&in trailing_step, const real320&in trailing_distance)
{
    asset_pair pair = dex::use_pair(primary_asset, secondary_asset, true);
    return dex::pay_context(pair, dex::use_order(tx::from(), tx::value(), pair, side, policy, order_condition::trailing_stop, real320::enan(), stop_price, slippage, trailing_step, trailing_distance));
}
uint256 trailing_stop_limit_order(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, order_side side, order_policy policy, const real320&in stop_price, const real320&in price, const real320&in trailing_step, const real320&in trailing_distance)
{
    asset_pair pair = dex::use_pair(primary_asset, secondary_asset, true);
    return dex::pay_context(pair, dex::use_order(tx::from(), tx::value(), pair, side, policy, order_condition::trailing_stop_limit, price, stop_price, real320::enan(), trailing_step, trailing_distance));
}
void withdraw_order(pmut@, const uint256&in order_id)
{
    order target;
    require(!tx::paid(), "payment not permitted");
    require(orders.y(order_id).next(target) && target.account == tx::from(), "withdrawal not permitted");
    context ctx;
    ctx.insert(mixed_ptr(@target, ref_flag::erased));
    dex::step_context(ctx);
    dex::pay_context(pair_of(null, target.pair_id), ctx);
}
uint256 deposit_pool(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, const real320&in price, const real320&in min_price, const real320&in max_price, const real320&in fee_rate)
{
    asset_pair pair = dex::use_pair(primary_asset, secondary_asset, true);
    return dex::pay_context(pair, dex::use_pool(tx::from(), tx::value(), pair, price, min_price, max_price, fee_rate, params.ref.pool_exit_fee));
}
void withdraw_pool(pmut@, const uint256&in pool_id)
{
    pool target;
    require(!tx::paid(), "payment not permitted");
    require(pools.y(pool_id).next(target) && target.account == tx::from(), "withdrawal not permitted");
    context ctx;
    ctx.insert(mixed_ptr(@target, ref_flag::erased));
    dex::step_context(ctx);
    dex::pay_context(pair_of(null, target.pair_id), ctx);
}
real320 best_price_of(pconst@, const uint256&in primary_asset, const uint256&in secondary_asset, order_side side)
{
    return dex::to_level_price(dex::use_pair(primary_asset, secondary_asset, false).id, side);
}
order order_of(pconst@, const uint256&in order_id)
{
    order result;
    require(orders.y(order_id).next(result), "order not found");
    return result;
}
bool order_alive(pconst@, const uint256&in order_id)
{
    return orders.y(order_id).next();
}
pool pool_of(pconst@, const uint256&in pool_id)
{
    pool result;
    require(pools.y(pool_id).next(result), "pool not found");
    return result;
}
bool pool_alive(pconst@, const uint256&in pool_id)
{
    return pools.y(pool_id).next();
}
asset_pair pair_of(pconst@, const uint256&in pair_id)
{
    asset_pair pair;
    require(pairs.y(pair_id).next(null, pair), "pair not found");
    pair.id = pair_id;
    return pair;
}
asset_tier asset_of(pconst@, const uint256&in asset)
{
    asset_tier result = assets.has(asset) ? assets[asset] : asset_tier();
    result.asset = asset;
    if (block::time_between(block::number(), result.block_number) > params.ref.asset_reset_days)
        result.volume = 0;
    return result;
}
account_tier account_of(pconst@, const address&in account, const uint256&in asset)
{
    account_tier result = accounts.has(account, asset) ? accounts[account, asset] : account_tier();
    if (block::time_between(block::number(), result.block_number) > params.ref.account_reset_days)
        result = account_tier();
    return result;
}
account_asset_tier account_asset_of(pconst@, const address&in account, const uint256&in asset)
{
    asset_tier asset_tier = asset_of(null, asset);
    account_asset_tier result;
    result.account = account_of(null, account, asset);
    result.maker_fee = result.account.maker_fee(asset_tier);
    result.taker_fee = result.account.taker_fee(asset_tier);
    return result;
}
config params_of(pconst@)
{
    return params.ref;
}