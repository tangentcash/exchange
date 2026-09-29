import { AssetId, ByteUtil, Chain, Hashsig, LiquidityPool, Messages, RPC, SchemaUtil, Seckey, Signing, Spot, Stream, Transactions, Uint256, UiUtil } from 'tangentsdk';
import { Log } from './logging';
import BigNumber from 'bignumber.js';
import process from 'node:process';
import fs from 'fs';

let wake: { pending: boolean, resolve: ((value: 'wake' | 'timeout') => void) | null } = { pending: false, resolve: null };
let socket: WebSocket | null = null;
const deactivatedPools: Record<string, number> = { };

function watch(exchangeUrl: string, getAccounts: () => string[]): void {
    let client: WebSocket;
    try {
        client = new WebSocket(exchangeUrl);
    } catch (exception) {
        Log.error(`relay watch: failed to connect (${exchangeUrl}):`, exception);
        setTimeout(() => watch(exchangeUrl, getAccounts), 5_000);
        return;
    }

    socket = client;
    client.onopen = () => {
        client.send(JSON.stringify({ method: 'post://', params: { accounts: getAccounts() } }));
        Log.info(`relay watch: connected (${exchangeUrl})`);
    };
    client.onmessage = (event: MessageEvent) => {
        try {
            const notification = JSON.parse(String(event.data)).notification;
            if (notification?.type == 'update:pool' && notification.data?.active === false) {
                const poolId = String(notification.data?.poolId);
                const now = Date.now();
                const seen = deactivatedPools[poolId];
                deactivatedPools[poolId] = now;
                if (seen != null && now - seen < 60_000) {
                    Log.info(`relay watch: duplicate deactivation (poolId: ${poolId}) ignored`);
                    return;
                }

                if (Object.keys(deactivatedPools).length > 64) {
                    for (let id in deactivatedPools) {
                        if (now - deactivatedPools[id] >= 60_000)
                            delete deactivatedPools[id];
                    }
                }

                Log.info(`relay watch: underlying lp deactivated (poolId: ${poolId})`);
                if (wake.resolve != null) {
                    const resolve = wake.resolve;
                    wake.resolve = null;
                    resolve('wake');
                } else {
                    wake.pending = true;
                }
            }
        } catch { }
    };
    client.onclose = () => {
        Log.error(`relay watch: disconnected (${exchangeUrl}), retrying in 5s`);
        socket = null;
        setTimeout(() => watch(exchangeUrl, getAccounts), 5_000);
    };
    client.onerror = () => {
        try { client.close(); } catch { }
    };
}
async function call(exchangeUrl: string, path: string, args: Record<string, unknown>): Promise<unknown> {
    try {
        const target = new URL(`${exchangeUrl}/${path}`);
        for (let key in args)
            target.searchParams.set(key, args[key]?.toString() || '');

        const response = await fetch(target);
        const result = await response.json();
        return result ? result.result : null;
    } catch {
        return null;
    }
}
async function send(confirmTimeout: number, address: string, buildTransaction: (nonce: Uint256, gasPrice: BigNumber, gasLimit: Uint256) => Stream): Promise<string> {
    const gasPrice = new BigNumber((await RPC.getGasPrice(new AssetId(), 0.95))?.price.toString() || 0);
    const nonce = new Uint256((await RPC.getNextAccountNonce(address))?.toString());
    const transaction = buildTransaction(nonce, gasPrice, new Uint256(1_000_000));
    try {
        const receipt = await RPC.simulateTransaction(transaction.encode());
        const gasLimit = new Uint256(receipt?.relative_gas_use?.toString() || 0);
        if (!gasLimit.gt(0))
            throw new Error('Failed to simulate transaction');

        const finalizedTransaction = buildTransaction(nonce, gasPrice, gasLimit);
        const transactionHash = await RPC.submitTransaction(finalizedTransaction.encode());
        if (!transactionHash)
            throw new Error('Failed to submit transaction');

        const deadline = Date.now() + confirmTimeout * 1_000;
        while (true) {
            let confirmation: unknown = null;
            try {
                confirmation = await RPC.getTransactionByHash(transactionHash);
            } catch {
                confirmation = null;
            }
            if (confirmation)
                break;
            if (Date.now() >= deadline)
                throw new Error(`Transaction ${transactionHash} not confirmed within ${confirmTimeout}s`);
            await new Promise((resolve) => setTimeout(resolve, 1_000));
        }

        return transactionHash;
    } catch (exception) {
        Log.info('TX message:', transaction.encode());
        throw exception;
    }
}
async function rebalance(confirmTimeout: number, address: string, secretKey: Seckey, pool: {
    delegatedAccount: string,
    primaryAsset: AssetId,
    secondaryAsset: AssetId,
    primaryReserve: BigNumber,
    secondaryReserve: BigNumber,
    price: BigNumber,
    feeRate: BigNumber,
    range: number | null,
    name: string,
    pull: boolean
}): Promise<void> {
    const priceRange = pool.range ? LiquidityPool.toRange(pool.primaryReserve, pool.secondaryReserve, pool.price, pool.range) : null;
    const minPrice = priceRange?.minPrice || null, maxPrice = priceRange?.maxPrice || null;
    let secondaryValue = LiquidityPool.toSecondaryValue(pool.primaryReserve, pool.price, minPrice, maxPrice);
    if (!secondaryValue)
        throw new Error('Insufficient primary reserve');

    let primaryValue: BigNumber = pool.primaryReserve;
    if (secondaryValue.gt(pool.secondaryReserve)) {
        secondaryValue = pool.secondaryReserve;
        const rebalanced = LiquidityPool.toPrimaryValue(pool.secondaryReserve, pool.price, minPrice, maxPrice);
        if (!rebalanced)
            throw new Error('Insufficient secondary reserve');
        primaryValue = rebalanced;
    }

    primaryValue = BigNumber.min(pool.primaryReserve, primaryValue);
    secondaryValue = BigNumber.min(pool.secondaryReserve, secondaryValue);
    Log.info(`LP ${pool.name} rebalancing${pool.pull ? ' (pulled)' : ''} at ${pool.price.toString()} (range: ${minPrice != null && maxPrice != null ? `${minPrice.toString()}-${maxPrice.toString()}` : 'uniform'}, fee: ${pool.feeRate.toString()})`);
    return console.log('SIMULATION:', {
        callable: pool.delegatedAccount,
        function: UiUtil.toFunction(Spot.DLP.transferLiquidity),
        args: [pool.primaryAsset.toUint256().toString(), pool.secondaryAsset.toUint256().toString(), primaryValue.toString(), secondaryValue.toString(), pool.price.toString(), (pool.range ? minPrice : new BigNumber(-1))?.toString(), (pool.range ? maxPrice : new BigNumber(-1))?.toString(), pool.feeRate.toString()]
    });
    const transactionHash = await send(confirmTimeout, address, (nonce: Uint256, gasPrice: BigNumber, gasLimit: Uint256) => {
        const transaction = {
            signature: new Hashsig(),
            asset: new AssetId(),
            nonce: nonce,
            gasPrice: gasPrice,
            gasLimit: gasLimit,
            callable: Signing.decodeAddress(pool.delegatedAccount),
            pays: [],
            function: UiUtil.toFunction(Spot.DLP.transferLiquidity),
            args: [pool.primaryAsset.toUint256(), pool.secondaryAsset.toUint256(), primaryValue, secondaryValue, pool.price, pool.range ? minPrice : new BigNumber(-1), pool.range ? maxPrice : new BigNumber(-1), pool.feeRate]
        };

        let stream = new Stream();
        SchemaUtil.store(stream, transaction, Messages.asSigningSchema(new Transactions.Call()));
        const signature = Signing.sign(stream.hash(), secretKey);
        if (!signature)
            throw new Error('Failed to sign a transaction');

        stream = new Stream();
        SchemaUtil.store(stream, { ...transaction, signature: signature }, new Transactions.Call());
        return stream;
    });

    Log.info(`LP ${pool.name} renewal finalized (price: ${pool.price.toString()}, tx: ${transactionHash})`);
}
async function main() {
    let config;
    BigNumber.config({ DECIMAL_PLACES: 18, ROUNDING_MODE: 1 });
    try {
        config = JSON.parse(fs.readFileSync(process.argv[2]).toString('utf8'));
        if (typeof config != 'object')
            throw new Error('config must be an object');

        if (!config.network || !['regtest', 'testnet', 'mainnet'].includes(config.network))
            throw new Error('invalid network');

        if (typeof config.validator != 'string')
            throw new Error('invalid validator url');

        if (typeof config.exchange != 'string')
            throw new Error('invalid exchange');

        if (config.pairs != null && (typeof config.pairs != 'object' || Array.isArray(config.pairs)))
            throw new Error('invalid pairs config');

        Chain.props = (Chain as unknown as Record<string, typeof Chain.props>)[config.network];
        RPC.applyValidator(config.validator);
    } catch (exception) {
        Log.error('path', process.argv[2] || null, ' failed to load a config:', exception);
        return process.exit(1);
    }

    const secretKey = Signing.decodeSecretKey(typeof config.secretKey == 'string' ? config.secretKey : (process.env[config.secretKeyEnv || 'WSK'] || ''));
    if (!secretKey) {
        Log.error('invalid secret key env');
        return process.exit(1);
    }

    const address = Signing.encodeAddress(Signing.derivePublicKeyHash(Signing.derivePublicKey(secretKey)));
    if (!address) {
        Log.error('invalid address');
        return process.exit(1);
    } else {
        Log.info(`LP delegator account: ${address}`);
    }
    
    const pools: Record<string, { failCount: number, backoffUntil: number, lastRedeploy: number, lastPrice: BigNumber | null }> = { };
    const pairs: Record<string, { twapInterval?: number, threshold?: number, feeRate?: number, range?: number }> = config.pairs || { };
    const checkInterval = config.checkInterval || 1_200;
    const confirmTimeout = config.confirmTimeout || 120;
    const twapInterval = config.twapInterval || 600;
    const threshold = new BigNumber(config.threshold ?? config.treshold ?? 0.01);
    const feeRate = new BigNumber(config.feeRate || 0.0005);
    const range = (config.range === null || config.range === 0) ? null : Number(config.range ?? 0.05);
    const rebalanceCooldown = config.rebalanceCooldown ?? 180;
    const backoffBase = config.backoff?.base ?? 15;
    const backoffCap = config.backoff?.cap ?? 300;
    let registeredAccounts: string[] = [address];
    watch(typeof config.relay == 'string' ? config.relay : config.exchange.replace(/^https/, 'wss').replace(/^http/, 'ws'), () => registeredAccounts);

    let delegatedPoolsSize: number | null = null;
    while (true) {
        const nextCycle = Date.now() + checkInterval * 1_000;
        const rawPools = await call(config.exchange, 'account/delegations', { account: address });
        const fetched = Array.isArray(rawPools);
        const delegatedPools = (fetched ? rawPools : []) as Record<string, any>[];
        if (!fetched)
            Log.error('failed to fetch delegations, retrying within 10s');
        if (delegatedPools.length != delegatedPoolsSize) {
            delegatedPoolsSize = delegatedPools.length;
            Log.info(`LP delegations (${delegatedPools.length}):`, delegatedPools);
        }

        if (fetched) {
            registeredAccounts = [address, ...new Set(delegatedPools.map((x) => x.delegatorAccount as string))];
            if (socket != null && socket.readyState == WebSocket.OPEN)
                socket.send(JSON.stringify({ method: 'post://', params: { accounts: registeredAccounts } }));
        }
        
        const names = new Set<string>();
        for (let i = 0; i < delegatedPools.length; i++) {
            const row = delegatedPools[i];
            const assets = { primary: new AssetId(row.primaryAsset), secondary: new AssetId(row.secondaryAsset) };
            const name = `${assets.primary.token || assets.primary.chain}/${assets.secondary.token || assets.secondary.chain}`;
            names.add(name);
            let state = pools[name];
            if (!state)
                state = pools[name] = { failCount: 0, backoffUntil: 0, lastRedeploy: 0, lastPrice: null };

            try {
                if (Date.now() < state.backoffUntil)
                    continue;

                const primaryAsset = new AssetId(row.primaryAsset as string);
                const secondaryAsset = new AssetId(row.secondaryAsset as string);
                const primaryReserve = new BigNumber(row.primaryLiquidity as string);
                const secondaryReserve = new BigNumber(row.secondaryLiquidity as string);
                const spotPrice = new BigNumber((row.price as string) || '0');
                if (spotPrice.gt(0))
                    state.lastPrice = spotPrice;
                if (!primaryReserve.gt(0) || !secondaryReserve.gt(0))
                    continue;

                const override = pairs[name] || { };
                const pairTwap = override.twapInterval ?? twapInterval;
                const pairThreshold = override.threshold != null ? new BigNumber(override.threshold) : threshold;
                const pairFeeRate = override.feeRate != null ? new BigNumber(override.feeRate) : feeRate;
                const pairRange = override.range === null || override.range === 0 ? null : (override.range != null ? Number(override.range) : range);

                if (!new BigNumber((row.poolId as string) || '0').gt(0)) {
                    Log.info(`LP ${name} pulled: no active underlying lp (liquidity ${primaryReserve.toString()}/${secondaryReserve.toString()})`);
                    const twap = new BigNumber(String((await call(config.exchange, 'market/price', { primaryAssetHash: primaryAsset.id, secondaryAssetHash: secondaryAsset.id, interval: pairTwap })) ?? '0'));
                    const last = twap.gt(0) ? twap : new BigNumber(String((await call(config.exchange, 'market/price', { primaryAssetHash: primaryAsset.id, secondaryAssetHash: secondaryAsset.id })) ?? '0'));
                    const anchor = last.gt(0) ? last : (state.lastPrice && state.lastPrice.gt(0) ? state.lastPrice : null);
                    if (!anchor) {
                        Log.info(`LP ${name} pulled: waiting for a price anchor (retry on next wake/cycle)`);
                        continue;
                    }

                    await rebalance(confirmTimeout, address, secretKey, {
                        delegatedAccount: row.delegatorAccount as string,
                        primaryAsset,
                        secondaryAsset,
                        primaryReserve,
                        secondaryReserve,
                        price: anchor,
                        feeRate: pairFeeRate,
                        range: pairRange,
                        name,
                        pull: true
                    });
                    state.lastRedeploy = Date.now();
                } else {
                    const price = new BigNumber(String((await call(config.exchange, 'market/price', { primaryAssetHash: primaryAsset.id, secondaryAssetHash: secondaryAsset.id, interval: pairTwap })) ?? '0'));
                    if (!price.gt(0)) {
                        Log.info(`LP ${name} skipped: no market price`);
                        continue;
                    }

                    const delta = spotPrice.gt(0) ? price.minus(spotPrice).dividedBy(spotPrice).abs() : new BigNumber(Math.max(pairThreshold.toNumber(), 1));
                    if (!delta.gt(pairThreshold))
                        Log.info(`LP ${name} passed: ${ByteUtil.bigNumberToString(price)} +${delta.multipliedBy(100).toFixed(2)}% dev`);
                    else if (rebalanceCooldown > 0 && state.lastRedeploy > 0 && Date.now() - state.lastRedeploy < rebalanceCooldown * 1_000)
                        Log.info(`LP ${name} passed: ${delta.multipliedBy(100).toFixed(2)}% dev within rebalance cooldown`);
                    else {
                        Log.info(`LP ${name} staled: ${ByteUtil.bigNumberToString(spotPrice)} +${delta.multipliedBy(100).toFixed(2)}% dev`);
                        deactivatedPools[String(row.poolId)] = Date.now();
                        await rebalance(confirmTimeout, address, secretKey, {
                            delegatedAccount: row.delegatorAccount as string,
                            primaryAsset,
                            secondaryAsset,
                            primaryReserve,
                            secondaryReserve,
                            price,
                            feeRate: pairFeeRate,
                            range: pairRange,
                            name,
                            pull: false
                        });
                        state.lastRedeploy = Date.now();
                    }
                }

                state.failCount = 0;
                state.backoffUntil = 0;
            } catch (exception) {
                state.failCount++;
                const delay = Math.min(backoffCap, backoffBase * Math.pow(2, state.failCount - 1));
                state.backoffUntil = Date.now() + delay * 1_000;
                Log.error(`LP ${name} renewal failed (attempt ${state.failCount}, retry in ${delay}s):`, exception);
            }
        }

        if (fetched) {
            for (let name in pools) {
                if (!names.has(name))
                    delete pools[name];
            }
        }

        const remaining = nextCycle - Date.now();
        Log.info(`LP cycle finalized (next: ${new Date(nextCycle)})`);
        if (remaining > 0 && !wake.pending) {
            await new Promise<'wake' | 'timeout'>((resolve) => {
                wake.resolve = resolve;
                setTimeout(() => resolve('timeout'), fetched ? remaining : Math.min(remaining, 10_000));
            });
            wake.resolve = null;
        }
        wake.pending = false;
    }
}

main();