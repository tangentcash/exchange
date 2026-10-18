namespace dex
{
    enum order_side { buy, sell }
    enum order_policy { immediate_all = 3 }
    string market_order() { return "uint256 market_order(pmut@, const uint256&in primary_asset, const uint256&in secondary_asset, order_side side, order_policy policy, const real320&in slippage_price)"; }
}

namespace ams
{
    booking_ref execute_swap_path(const booking_ref&in input, const array<swap>&in path, usize offset)
    {
        address self = tx::to();
        booking_ref output = input;
        for (usize i = 0; i < path.size(); i++)
        {
            const swap@ order = path[i];
            string id = "swap #" + string::from(uint16(offset) + uint16(i + 1));
            require(order.price.positive(), id + " price must be positive");
            require(order.amount.positive(), id + " amount must be positive");
            require(order.asset != output.asset, id + " cannot swap an asset into itself");
            require(order.slippage >= 0 && order.slippage < 1,  id + " slippage must be within [0; 1) range");
            require(order.amount <= output.amount, id + " paying amount must be less than or equal to " + string::from(output.amount));

            payable order_value; order_value.plus(output.asset, output.amount);
            real320 receiving_value = self.balance_delta_of(order.asset);
            real320 slippage_price = order.price * (order.buying ? (real320(1) + order.slippage) : (real320(1) - order.slippage));
            uint256 primary_asset = (order.buying ? order.asset : output.asset);
            uint256 secondary_asset = (order.buying ? output.asset : order.asset);
            uint256 order_id = params.ref.dex_account.call<uint256>(dex::market_order(), order_value, primary_asset, secondary_asset, order.buying ? dex::order_side::buy : dex::order_side::sell, dex::order_policy::immediate_all, slippage_price);
            receiving_value = self.balance_delta_of(order.asset) - receiving_value;
            require(order_id == 0, id + " left a dangling market order in book");
            require(receiving_value.positive() && receiving_value >= order.amount, id + " received only " + string::from(receiving_value) + " but expected at least " + string::from(order.amount) + " of " + coin::name_of(order.asset));

            real320 price = order.buying ? (output.amount / receiving_value) : (receiving_value / output.amount);
            require(price.positive() && (order.buying ? price <= slippage_price : price >= slippage_price), id + " executed at price " + string::from(price) + " but expected no worse than " + string::from(slippage_price));
            output.asset = order.asset;
            output.amount = receiving_value;
        }
        return output;
    }
    void expire_deposit_transactions()
    {
        uint256 spent_hash; array<uint256> spent_hashes;
        ranging_slice spent_slice = deposits.y(true).lte(block::number());
        while (spent_slice.next(spent_hash))
            spent_hashes.push(spent_hash);
        for (usize i = 0; i < spent_hashes.size(); i++)
            deposits.erase(spent_hashes[i], true);
    }
}

class booking_ref
{
    uint256 asset;
    real320 amount;
}

class post_booking : booking_ref
{
    string public_key;
    uint256 block_number;
    uint256 withdraw_hash;
}

class fee_ref
{
    uint256 asset;
    real320 fee;
}

class swap
{
    uint256 asset;
    real320 price;
    real320 amount;
    real320 slippage;
    bool buying;
}

class swap_ref
{
    booking_ref input;
    booking_ref output;
}

class config
{
    address dex_account;
    address deployer_account;
    real320 fee_overflow = 0.01;
    uint64 book_validity_blocks = 7200;
    uint64 deposit_validity_blocks = 21600;
}

ranging<booking_ref, bool, string> bookings;
ranging<uint256, bool, post_booking> deposits;
mapping<uint256, real320> fees;
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
    require(!tx::paid(), "payment not permitted");
    require(params.empty() || params.ref.deployer_account == tx::from(), "prev. deployer account must be the tx sender");
    require(!new_params.deployer_account.empty() && !new_params.dex_account.empty(), "deployer/dex accounts must not be empty");
    require(new_params.book_validity_blocks >= 2400, "booking validity period is too low");
    require(new_params.deposit_validity_blocks >= 7200, "deposit validity period is too low");
    require(new_params.fee_overflow >= 0 && new_params.fee_overflow <= 1, "fee overflow must be within [0; 1] range");
    params = new_params;
    log::event(new_params, null);
}
void reconstruct_fee(pmut@, const uint256&in asset, const real320&in fee)
{
    require(!tx::paid(), "payment not permitted");
    require(params.ref.deployer_account == tx::from(), "deployer account must be the tx sender");
    require(coin::bridged(asset) && coin::base_of(asset) == asset, "must use off-chain base asset");
    require(fee.positive(), "fee must be positive");

    fee_ref ref;
    ref.asset = asset;
    ref.fee = fee;
    fees.insert(ref.asset, ref.fee);
    log::emit(ref);
}
void reconstruct_link(pmut@, const uint256&in bridge_hash, const string&in routing_address)
{
    uint256 asset = tx::asset();
    require(!tx::paid(), "payment not permitted");
    require(bridge::requires_link(), "link not permitted");
    require(params.ref.deployer_account == tx::from(), "deployer account must be the tx sender");
    require(coin::bridged(asset) && fees.has(asset), "fee for " + coin::name_of(asset) + " must be set");
    require(bridge::requires_link(), "link requires routing feature flag");
    bridge::link(bridge_hash, asset, routing_address);
}
void book_deposit(pmut@, const uint256&in asset, const real320&in amount, const string&in public_key)
{
    uint256 network = coin::base_of(asset);
    require(!tx::paid(), "payment not permitted");
    require(coin::bridged(asset), "must book off-chain asset");
    require(alg::epoint264(public_key), "must book with a valid public key");
    require(fees.has(network), "booking for " + coin::name_of(asset) + " is not yet available");

    real320 fee = fees[network];
    require(amount > fee, "must book at least " + string::from(fee) + " " + coin::name_of(asset));

    booking_ref ref;
    array<booking_ref> refs;
    ranging_slice slice = bookings.y(true).lte(block::number());
    while (slice.next(ref))
        refs.push(ref); 
    for (usize i = 0; i < refs.size(); i++)
        bookings.erase(refs[i], true);
    
    ref.asset = asset;
    ref.amount = amount;
    require(!bookings.has_x(ref), "provided network and amount is already booked");
    bookings.insert(ref, true, public_key, block::number() + params.ref.book_validity_blocks);
    log::emit(ref);
}
void spend_deposit(pmut@, const uint256&in deposit_hash, const uint256&in bridge_hash, const string&in routing_address, const array<swap>&in token_path, const array<swap>&in fee_path, const string&in signature)
{
    block::tx deposit = block::tx(deposit_hash);
    require(!tx::paid(), "payment not permitted");
    require(bridge::requires_send(), "send not permitted");
    require(bridge_hash > 0, "bridge hash must be set");
    require(!routing_address.empty(), "routing address must be set");
    require(deposit_hash > 0 && !deposits.has(deposit_hash, true), "deposit transaction spent");
    require(!signature.empty(), "signature must be set");
    require(deposit.exists() && deposit.success(), "deposit transaction not found");

    payable payments = deposit.payments_to(tx::to());
    require(payments.size() == 1, "deposit transaction must pay once");

    booking_ref input;
    input.asset = payments[0];
    input.amount = payments.of(input.asset);
    require(coin::bridged(input.asset), "must pay off-chain asset");
    require(input.amount.positive(), "must pay positive amount");
    require(bookings.has(input, true), "booking not found");

    abi message;
    usize path_size = token_path.size() + fee_path.size();
    message.wu256(deposit_hash);
    message.wu256(bridge_hash);
    message.wu256(input.asset);
    message.wr320(input.amount);
    message.wu160(tx::to());
    message.wu160(tx::from());
    message.wstr(routing_address);
    message.wu256(path_size);
    for (usize i = 0; i < path_size; i++)
    {
        const swap@ order = i < token_path.size() ? token_path[i] : fee_path[i - token_path.size()];
        message.wu256(order.asset);
        message.wr320(order.price);
        message.wr320(order.amount);
        message.wr320(order.slippage);
        message.wu8(order.buying);
    }

    string booking_public_key; uint256 booking_block_number;
    require(bookings.x(input).next(null, booking_public_key, booking_block_number), "booking not found");
    require(deposit.block_number() + params.ref.book_validity_blocks >= booking_block_number.u64(), "deposit transaction came before booking");
    require(block::number() - deposit.block_number() <= params.ref.book_validity_blocks, "deposit transaction has timed out");
    require(block::number() <= booking_block_number.u64(), "booking has timed out");

    string provided_public_key = alg::erecover264(alg::blake2b256(message.data()), signature);
    require(booking_public_key == provided_public_key, "booking signature does not match");
    ams::expire_deposit_transactions();
    bookings.erase(input, true);

    booking_ref output = ams::execute_swap_path(input, token_path, 0);
    uint256 output_network = coin::base_of(output.asset);
    require(fees.has(output_network), "send for " + coin::name_of(output_network) + " is not yet available");

    real320 fee = fees[output_network];
    require(output_network == output.asset ? fee_path.empty() : !fee_path.empty(), "unexpected fee swap path");
    if (output_network != output.asset)
    {
        const swap@ fee_order = fee_path[0];
        booking_ref fee_input = output;
        require(fee_order.price.positive(), "fee swap price must be positive");
        require(fee_order.amount.positive(), "fee swap amount must be positive");
        require(fee_order.slippage >= 0 && fee_order.slippage < 1, "fee swap slippage must be within [0; 1) range");
        fee_input.amount = fee_order.buying ? ((real320(1) + fee_order.slippage) * fee_order.amount * fee_order.price) : (fee_order.amount / (fee_order.price * (real320(1) - fee_order.slippage)));
        require(fee_input.amount.positive(), "fee swap budget must be positive");
        output.amount = output.amount - fee_input.amount;
        require(output.amount.positive(), "send amount must be greater than the " + string::from(fee_input.amount) + " fee swap budget of " + coin::name_of(output.asset));

        booking_ref fee_output = ams::execute_swap_path(fee_input, fee_path, token_path.size());
        require(fee_output.asset == output_network, "send requires fee paid in " + coin::name_of(output_network) + " but got " + coin::name_of(fee_output.asset));
        require(fee_output.amount >= fee, "send requires a fee of " + string::from(fee) + " but got " + string::from(fee_output.amount) + " of " + coin::name_of(output_network) + ", underpaying");
        require(fee_output.amount <= fee * (real320(1) + params.ref.fee_overflow), "send requires a fee of just " + string::from(fee) + " but got " + string::from(fee_output.amount) + " of " + coin::name_of(output_network) + ", overpaying");

        real320 dust = fee_output.amount - fee;
        if (dust.positive())
            params.ref.deployer_account.pay(output_network, dust);
    }
    else
    {
        output.amount = output.amount - fee;
        require(output.amount.positive(), "send amount must be greater than the " + string::from(fee) + " fee of " + coin::name_of(output.asset));
    }

    swap_ref event;
    event.input = input;
    event.output = output;
    log::emit(event);

    post_booking booking;
    booking.asset = output.asset;
    booking.amount = output.amount;
    booking.public_key = booking_public_key;
    booking.block_number = block::number() + params.ref.deposit_validity_blocks;
    booking.withdraw_hash = tx::hash();
    deposits.insert(deposit_hash, true, booking, booking.block_number);
    log::emit(booking);

    require(coin::bridged(output.asset), "must use off-chain base asset to send");
    require(bridge::requires_send(), "send requires withdrawal feature flag");
    bridge::send(bridge_hash, output.asset, routing_address, output.amount, fee);
}
void respend_deposit(pmut@, const uint256&in deposit_hash, const uint256&in broadcast_hash, const uint256&in bridge_hash, const string&in routing_address, const string&in signature)
{
    block::tx deposit = block::tx(deposit_hash);
    require(!tx::paid(), "payment not permitted");
    require(bridge::requires_send(), "send not permitted");
    require(bridge_hash > 0, "bridge hash must be set");
    require(!routing_address.empty(), "routing address must be set");
    require(!signature.empty(), "signature must be set");
    require(deposit_hash > 0 && deposit.exists() && deposit.success(), "deposit transaction not found");
    require(deposits.has(deposit_hash, true), "deposit transaction unspent");
    require(block::number() - deposit.block_number() <= params.ref.deposit_validity_blocks, "deposit transaction is too old to resend");
    ams::expire_deposit_transactions();

    post_booking ref = deposits[deposit_hash, true];
    uint256 network = coin::base_of(ref.asset);
    require(coin::bridged(ref.asset) && !ref.public_key.empty() && ref.amount.positive(), "deposit cannot be resent");
    require(fees.has(network), "send for " + coin::name_of(network) + " is not yet available");
    require(block::number() - ref.block_number.u64() <= params.ref.deposit_validity_blocks, "deposit is too old to resend");

    block::tx broadcast = block::tx(broadcast_hash);
    require(broadcast_hash > 0 && broadcast.exists(), "broadcast transaction not found");
    require(!broadcast.success() && broadcast.by(ref.withdraw_hash), "broadcast transaction must not have sent a proof and must be initiated by this booking");

    abi message;
    message.wu256(deposit_hash);
    message.wu256(broadcast_hash);
    message.wu256(bridge_hash);
    message.wu256(ref.asset);
    message.wr320(ref.amount);
    message.wu160(tx::to());
    message.wu160(tx::from());
    message.wstr(routing_address);
    require(ref.public_key == alg::erecover264(alg::blake2b256(message.data()), signature), "resend signature does not match booking key");

    post_booking resent;
    resent.asset = ref.asset;
    resent.amount = ref.amount;
    resent.public_key = ref.public_key;
    resent.block_number = block::number() + params.ref.deposit_validity_blocks;
    resent.withdraw_hash = tx::hash();
    deposits.insert(deposit_hash, true, resent, resent.block_number);
    log::emit(resent);

    bridge::send(bridge_hash, ref.asset, routing_address, ref.amount, fees[network]);
}
void repay_deposit(pmut@, const uint256&in deposit_hash)
{
    block::tx deposit = block::tx(deposit_hash);
    require(!tx::paid(), "payment not permitted");
    require(params.ref.deployer_account == tx::from(), "deployer account must be the tx sender");
    require(deposit_hash > 0 && deposit.exists() && deposit.success(), "deposit transaction not found");
    require(!deposits.has(deposit_hash, true), "deposit transaction spent");
    require(block::number() - deposit.block_number() > params.ref.book_validity_blocks, "deposit transaction is time locked");
    require(block::number() - deposit.block_number() <= params.ref.deposit_validity_blocks, "deposit transaction is too old to repay");
    ams::expire_deposit_transactions();

    post_booking booking;
    booking.amount = real320::enan();
    booking.block_number = block::number() + params.ref.deposit_validity_blocks;
    deposits.insert(deposit_hash, true, booking, booking.block_number);

    payable payments = deposit.payments_to(tx::to());
    require(!payments.empty(), "deposit transaction must pay");
    for (usize i = 0; i < payments.size(); i++)
        params.ref.deployer_account.pay(payments[i], payments.of(payments[i]));
}
real320 fee_of(pconst@, const uint256&in asset)
{
    require(fees.has(asset), "fee not found");
    return fees[asset];
}
bool deposit_spent(pconst@, const uint256&in deposit_hash)
{
    return deposits.has(deposit_hash, true);
}
bool booking_alive(pconst@, const uint256&in asset, const real320&in amount)
{
    booking_ref ref;
    ref.asset = asset;
    ref.amount = amount;
    return bookings.has(ref, true);
}
post_booking booking_of(pconst@, const uint256&in asset, const real320&in amount)
{
    post_booking ref;
    ref.asset = asset;
    ref.amount = amount;
    require(bookings.x(booking_ref(ref)).next(null, ref.public_key, ref.block_number), "booking not found");
    return ref;
}
config params_of(pconst@)
{
    return params.ref;
}