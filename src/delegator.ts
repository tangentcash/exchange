import { AssetId, ByteUtil, Chain, Hashing, Hashsig, LiquidityPool, Messages, RPC, SchemaUtil, Seckey, Signing, Spot, Stream, Transactions, Uint256, UiUtil } from 'tangentsdk';
import { Log } from './logging';
import BigNumber from 'bignumber.js';
import process from 'node:process';
import fs from 'fs';
import WebSocket from 'ws';

type RuleConfig = {
    checkInterval?: number,
    twapInterval?: number,
    threshold?: number,
    fee?: number,
    range?: number | null,
    reserve?: number,
    cooldownInterval?: number,
    rebalanceInterval?: number | null
};

type DelegatorConfig = {
    network: string,
    validator: string,
    exchange: string,
    relay?: string,
    secretKey?: string,
    secretKeyEnv?: string,
    confirmTimeout: number,
    refreshInterval: number,
    callTimeout: number,
    simulation: boolean,
    watch?: {
        connectTimeout?: number
    },
    backoff: {
        base: number,
        cap: number
    },
    rules: Record<string, RuleConfig>
};

type Rule = {
    checkInterval: number,
    twapInterval: number,
    threshold: BigNumber,
    fee: BigNumber,
    range: number | null,
    reserve: BigNumber,
    cooldownInterval: number,
    rebalanceInterval: number | null
};

type PoolState = {
    failCount: number,
    backoffUntil: number,
    lastRedeploy: number,
    lastPrice: string | null,
    nextCheckAt: number
};

type DelegationRow = {
    primaryAsset: string,
    secondaryAsset: string,
    primaryLiquidity: string,
    secondaryLiquidity: string,
    price: string,
    poolId: string,
    delegatorAccount: string
};

type ExchangeOptions = {
    url: string,
    accounts?: string[],
    callTimeout?: number,
    connectTimeout?: number,
    onNotification?: (type: string, data: any) => void
};

class Exchange {
    static url: string;
    static accounts: string[] = [ ];
    static callTimeout = 30;
    static connectTimeout = 15;
    static onNotification: (type: string, data: any) => void = () => { };
    static socket: WebSocket | null = null;
    static connecting = false;
    static reconnectTimer: NodeJS.Timeout | null = null;
    static awaitables: (() => void)[ ] = [ ];
    static requests = { pending: new Map<string, (value: any | Error) => void>( ), count: 0 };

    static init(options: ExchangeOptions): void {
        Exchange.url = options.url;
        Exchange.accounts = options.accounts || [ ];
        Exchange.callTimeout = options.callTimeout ?? Exchange.callTimeout;
        Exchange.connectTimeout = options.connectTimeout ?? Exchange.connectTimeout;
        if (options.onNotification != null)
            Exchange.onNotification = options.onNotification;
    }
    static establish(): void {
        if (Exchange.socket != null || Exchange.connecting || Exchange.reconnectTimer != null)
            return;

        Exchange.connecting = true;
        let client: WebSocket;
        try {
            client = new WebSocket(`${Exchange.url}/`);
        } catch (exception) {
            Log.error(`exchange: failed to connect (${Exchange.url}):`, exception);
            Exchange.connecting = false;
            Exchange.retry();
            Exchange.flush();
            return;
        }

        const connectTimer = setTimeout(() => {
            if (client.readyState == WebSocket.CONNECTING) {
                Log.error(`exchange: connect timeout after ${Exchange.connectTimeout}s (${Exchange.url})`);
                try { client.terminate(); } catch { }
            }
        }, Exchange.connectTimeout * 1_000);

        client.on('open', () => {
            clearTimeout(connectTimer);
            Exchange.connecting = false;
            Exchange.socket = client;
            Log.info(`exchange: connected (${Exchange.url})`);
            Exchange.subscribe();
            Exchange.flush();
        });
        client.on('message', (data: unknown) => {
            try {
                const message = JSON.parse(String(data));
                if (message != null && typeof message.notification == 'object' && message.notification != null && typeof message.notification.type == 'string')
                    Exchange.onNotification(message.notification.type, message.notification.data);
                else if (message != null && message.id != null && message.id != 'connect') {
                    const pending = Exchange.requests.pending.get(String(message.id));
                    if (pending != null)
                        pending(message);
                }
            } catch { }
        });
        client.on('close', () => {
            clearTimeout(connectTimer);
            Exchange.connecting = false;
            if (Exchange.socket == client)
                Exchange.socket = null;

            Log.error(`exchange: disconnected (${Exchange.url}), retrying in 5s`);
            for (let pending of Exchange.requests.pending.values())
                pending(new Error('connection reset'));

            Exchange.retry();
        });
        client.on('error', (exception: unknown) => {
            Log.error(`exchange: socket error (${Exchange.url}):`, exception);
            try { client.close(); } catch { }
        });
    }
    static subscribe(accounts?: string[]): void {
        Exchange.accounts = accounts || Exchange.accounts;
        if (Exchange.socket != null && Exchange.socket.readyState == WebSocket.OPEN) {
            try { Exchange.socket.send(JSON.stringify({ method: 'post://', params: { accounts: Exchange.accounts }, id: 'connect' })); } catch (exception) {
                Log.error('exchange: failed to (re)subscribe accounts:', exception);
            }
        }
    }
    static retry(): void {
        if (Exchange.reconnectTimer == null) {
            Exchange.reconnectTimer = setTimeout(() => {
                Exchange.reconnectTimer = null;
                Exchange.establish();
            }, 5_000);
        }
    }
    static flush(): void {
        for (let awaitable of Exchange.awaitables.splice(0))
            awaitable();
    }
    static async fetch(location: string, args: Record<string, unknown>): Promise<unknown> {
        if (Exchange.socket == null || Exchange.socket.readyState != WebSocket.OPEN)
            await new Promise<void>((resolve) => {
                Exchange.awaitables.push(resolve);
                Exchange.establish();
            });

        const socket = Exchange.socket;
        if (socket == null || socket.readyState != WebSocket.OPEN)
            throw new Error(`exchange call ${location} failed: connection reset`);

        const id = (++Exchange.requests.count).toString();
        const data: any = await new Promise((resolve, reject) => {
            const settle = (value: any | Error) => {
                Exchange.requests.pending.delete(id);
                clearTimeout(timeout);
                if (value instanceof Error)
                    reject(value);
                else
                    resolve(value);
            };
            Exchange.requests.pending.set(id, settle);
            const timeout = setTimeout(() => settle(new Error(`connection timed out after ${Exchange.callTimeout}s`)), Exchange.callTimeout * 1_000);
            try { socket.send(JSON.stringify({ id: id, method: 'get://' + location, params: args })); } catch {
                settle(new Error('connection reset'));
            }
        });

        if (data.error != null)
            throw new Error(`${data.error} — E${ByteUtil.uint8ArrayToHexString(Hashing.hash160(ByteUtil.byteStringToUint8Array(String(data.error)))).substring(0, 8).toUpperCase()}`);
        return data.result ?? null;
    }
    static async marketPrice(primary: AssetId, secondary: AssetId, interval?: number): Promise<BigNumber> {
        const args: Record<string, unknown> = { primaryAssetHash: primary.id, secondaryAssetHash: secondary.id };
        if (interval != null)
            args.interval = interval;
        return new BigNumber(String((await Exchange.fetch('market/price', args)) ?? '0'));
    }
}

const pools: Record<string, PoolState> = { };
const wakeListeners: (() => void)[] = [ ];
const deactivatedPools: Record<string, number> = { };
let config: DelegatorConfig;
let address: string;
let secretKey: Seckey;
let wakeups = 0;

function resolveConfig(raw: string): DelegatorConfig {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed != 'object' || parsed == null || Array.isArray(parsed))
        throw new Error('config must be an object');

    const source = parsed as Record<string, unknown>;
    if (typeof source.network != 'string' || !['regtest', 'testnet', 'mainnet'].includes(source.network))
        throw new Error('invalid network');

    if (typeof source.validator != 'string')
        throw new Error('invalid validator url');

    if (typeof source.exchange != 'string')
        throw new Error('invalid exchange');

    if (source.rules != null && (typeof source.rules != 'object' || Array.isArray(source.rules)))
        throw new Error('invalid rules config');

    const rules = (source.rules || { }) as Record<string, RuleConfig>;
    for (let key in rules) {
        const rule = rules[key];
        if (typeof rule != 'object' || rule == null || Array.isArray(rule))
            throw new Error(`invalid config for rule ${key}`);
        if (rule.reserve != null && (typeof rule.reserve != 'number' || rule.reserve < 0 || rule.reserve >= 1))
            throw new Error(`invalid reserve for rule ${key}`);
        if (rule.rebalanceInterval != null && rule.rebalanceInterval !== 0 && !(typeof rule.rebalanceInterval == 'number' && rule.rebalanceInterval > 0))
            throw new Error(`invalid rebalance interval for rule ${key}`);
    }

    const backoff = typeof source.backoff == 'object' && source.backoff != null ? source.backoff as Record<string, unknown> : { };
    const numberOr = (source: Record<string, unknown>, key: string, fallback: number): number => typeof source[key] == 'number' ? source[key] as number : fallback;
    const stringOr = (source: Record<string, unknown>, key: string): string | undefined => typeof source[key] == 'string' ? source[key] as string : undefined;
    return {
        network: source.network,
        validator: source.validator,
        exchange: source.exchange,
        relay: stringOr(source, 'relay'),
        secretKey: stringOr(source, 'secretKey'),
        secretKeyEnv: stringOr(source, 'secretKeyEnv'),
        confirmTimeout: numberOr(source, 'confirmTimeout', 120),
        refreshInterval: numberOr(source, 'refreshInterval', 300),
        callTimeout: numberOr(source, 'callTimeout', 30),
        simulation: source.simulation === true,
        watch: typeof source.watch == 'object' && source.watch != null ? source.watch as { connectTimeout?: number } : undefined,
        backoff: { base: numberOr(backoff, 'base', 15), cap: numberOr(backoff, 'cap', 300) },
        rules
    };
}
function resolveRule(name: string): Rule {
    const merged: RuleConfig = { ...config.rules['*'], ...config.rules[name] };
    return {
        checkInterval: merged.checkInterval || 1_200,
        twapInterval: merged.twapInterval || 600,
        threshold: merged.threshold != null ? new BigNumber(merged.threshold) : new BigNumber(0.01),
        fee: merged.fee != null ? new BigNumber(merged.fee) : new BigNumber(0.0005),
        range: merged.range === null || merged.range === 0 ? null : Number(merged.range ?? 0.05),
        reserve: merged.reserve != null ? new BigNumber(merged.reserve) : new BigNumber(0),
        cooldownInterval: merged.cooldownInterval ?? 180,
        rebalanceInterval: merged.rebalanceInterval || null
    };
}
async function rebalance(pool: {
    delegatedAccount: string,
    primaryAsset: AssetId,
    secondaryAsset: AssetId,
    primaryReserve: BigNumber,
    secondaryReserve: BigNumber,
    price: BigNumber,
    fee: BigNumber,
    range: number | null,
    name: string,
    reserve: BigNumber,
    pull: boolean
}): Promise<void> {
    const priceRange = pool.range ? LiquidityPool.toRange(pool.primaryReserve, pool.secondaryReserve, pool.price, pool.range) : null;
    const minPrice = priceRange?.minPrice || null, maxPrice = priceRange?.maxPrice || null;
    const allocate = BigNumber(1).minus(pool.reserve);
    const primaryCap = pool.primaryReserve.multipliedBy(allocate);
    const secondaryCap = pool.secondaryReserve.multipliedBy(allocate);
    let secondaryValue = LiquidityPool.toSecondaryValue(primaryCap, pool.price, minPrice, maxPrice);
    if (!secondaryValue)
        throw new Error('Insufficient primary reserve');

    let primaryValue: BigNumber = primaryCap;
    if (secondaryValue.gt(secondaryCap)) {
        secondaryValue = secondaryCap;
        const rebalanced = LiquidityPool.toPrimaryValue(secondaryCap, pool.price, minPrice, maxPrice);
        if (!rebalanced)
            throw new Error('Insufficient secondary reserve');
        primaryValue = rebalanced;
    }

    primaryValue = BigNumber.min(primaryCap, primaryValue);
    secondaryValue = BigNumber.min(secondaryCap, secondaryValue);
    Log.info(`LP ${pool.name} rebalancing${pool.pull ? ' (pulled)' : ''} at ${pool.price.toString()} (range: ${minPrice != null && maxPrice != null ? `${minPrice.toString()}-${maxPrice.toString()}` : 'uniform'}, fee: ${pool.fee.toString()}, alloc: ${allocate.multipliedBy(100).toFixed(2)}%)`);
    const nonce = new Uint256((await RPC.getNextAccountNonce(address))?.toString());
    const gasPrice = new BigNumber((await RPC.getGasPrice(new AssetId(), 0.95))?.price.toString() || 0);
    const build = (gasLimit: Uint256): Stream => {
        const transaction = {
            signature: new Hashsig(),
            asset: new AssetId(),
            nonce: nonce,
            gasPrice: gasPrice,
            gasLimit: gasLimit,
            callable: Signing.decodeAddress(pool.delegatedAccount),
            pays: [],
            function: UiUtil.toFunction(Spot.DLP.transferLiquidity),
            args: [pool.primaryAsset.toUint256(), pool.secondaryAsset.toUint256(), primaryValue, secondaryValue, pool.price, pool.range ? minPrice : new BigNumber(-1), pool.range ? maxPrice : new BigNumber(-1), pool.fee]
        };

        let stream = new Stream();
        SchemaUtil.store(stream, transaction, Messages.asSigningSchema(new Transactions.Call()));
        const signature = Signing.sign(stream.hash(), secretKey);
        if (!signature)
            throw new Error('Failed to sign a transaction');

        stream = new Stream();
        SchemaUtil.store(stream, { ...transaction, signature: signature }, new Transactions.Call());
        return stream;
    };

    const simulation = build(new Uint256(1_000_000));
    try {
        const receipt = await RPC.simulateTransaction(simulation.encode());
        const gasLimit = new Uint256(receipt?.relative_gas_use?.toString() || 0);
        if (!gasLimit.gt(0))
            throw new Error('Failed to simulate transaction');

        if (config.simulation) {
            Log.info(`LP ${pool.name} renewal simulated (price: ${pool.price.toString()}, gas: ${gasLimit.toString()}): ${JSON.stringify(receipt, null, 2)}`);
            return;
        }

        const transactionHash = await RPC.submitTransaction(build(gasLimit).encode());
        if (!transactionHash)
            throw new Error('Failed to submit transaction');

        const deadline = Date.now() + config.confirmTimeout * 1_000;
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
                throw new Error(`Transaction ${transactionHash} not confirmed within ${config.confirmTimeout}s`);
            await new Promise<void>((resolve) => setTimeout(resolve, 1_000));
        }

        Log.info(`LP ${pool.name} renewal finalized (price: ${pool.price.toString()}, tx: ${transactionHash})`);
    } catch (exception) {
        Log.info('TX message:', simulation.encode());
        throw exception;
    }
}
async function main(): Promise<void> {
    BigNumber.config({ DECIMAL_PLACES: 18, ROUNDING_MODE: 1 });
    try {
        config = resolveConfig(fs.readFileSync(process.argv[2]).toString('utf8'));
        Chain.props = (Chain as unknown as Record<string, typeof Chain.props>)[config.network];
        RPC.applyValidator(config.validator);
    } catch (exception) {
        Log.error('path', process.argv[2] || null, ' failed to load a config:', exception);
        return process.exit(1);
    }

    const decodedSecretKey = Signing.decodeSecretKey(config.secretKey ?? (process.env[config.secretKeyEnv || 'WSK'] || ''));
    if (!decodedSecretKey) {
        Log.error('invalid secret key env');
        return process.exit(1);
    }

    secretKey = decodedSecretKey;
    address = Signing.encodeAddress(Signing.derivePublicKeyHash(Signing.derivePublicKey(secretKey))) || '';
    if (!address) {
        Log.error('invalid address');
        return process.exit(1);
    }

    let startedAt = Date.now(), woke = false;
    Log.info(`LP delegator account: ${address}`);
    Exchange.init({
        url: config.relay || config.exchange.replace(/^https/, 'wss').replace(/^http/, 'ws'),
        accounts: [address],
        callTimeout: config.callTimeout,
        connectTimeout: config.watch?.connectTimeout,
        onNotification: (type: string, data: any): void => {
            const pool = data?.pool;
            if (type != 'update:pool' || data?.active !== false || pool == null)
                return;

            const poolId = String(pool), now = Date.now();
            if ((deactivatedPools[poolId] || 0) + 10_000 >= now)
                return;

            deactivatedPools[poolId] = now; wakeups++;
            Log.info(`LP pool ${poolId} invalidated by relay, forcing cycle`);
            for (let listener of wakeListeners.splice(0))
                listener();
        }
    });

    while (true) {
        const sequence = wakeups, time = Date.now(), forced = woke; woke = false;
        Log.info(`LP loop: cycle started${forced ? ' (forced by relay wake)' : ''} (LPs: ${Object.keys(pools).length})`);
        const rows = ((await Exchange.fetch('account/delegations', { account: address }).catch((exception) => {
            Log.error(exception);
            return null;
        })) || []) as DelegationRow[];
        if (!Array.isArray(rows)) {
            Log.error('failed to fetch delegations, retrying within 10s');
            await new Promise((resolve) => setTimeout(resolve, 10_000));
            continue;
        }

        const mutations: Record<string, string | boolean> = { };
        for (let row of rows) {
            const assets = { primary: new AssetId(row.primaryAsset), secondary: new AssetId(row.secondaryAsset) };
            const name = `${assets.primary.token || assets.primary.chain}/${assets.secondary.token || assets.secondary.chain}`;
            const rule = resolveRule(name);
            const state: PoolState = pools[name] || (pools[name] = { failCount: 0, backoffUntil: 0, lastRedeploy: 0, lastPrice: null, nextCheckAt: 0 });
            const due = forced || Date.now() >= state.nextCheckAt;
            mutations[name] = state.nextCheckAt == 0 ? JSON.stringify({ ...rule, ...row }, null, 2) : false;
            state.nextCheckAt = time + rule.checkInterval * 1_000;
            if (Date.now() < state.backoffUntil) {
                state.nextCheckAt = state.backoffUntil;
                continue;
            } else if (!due) {
                continue;
            }

            try {
                const primaryReserve = new BigNumber(row.primaryLiquidity);
                const secondaryReserve = new BigNumber(row.secondaryLiquidity);
                const spotPrice = new BigNumber(row.price || '0');
                const redeploy = async (price: BigNumber, pull: boolean) => {
                    await rebalance({
                        delegatedAccount: row.delegatorAccount,
                        primaryAsset: assets.primary,
                        secondaryAsset: assets.secondary,
                        primaryReserve,
                        secondaryReserve,
                        price,
                        fee: rule.fee,
                        reserve: rule.reserve,
                        range: rule.range,
                        name,
                        pull
                    });
                    state.lastRedeploy = Date.now();
                };
                state.lastPrice = spotPrice.gt(0) ? spotPrice.toString() : state.lastPrice;
                if (!primaryReserve.gt(0) || !secondaryReserve.gt(0))
                    Log.info(`LP ${name} idle: zero reserves (liquidity ${primaryReserve.toString()}/${secondaryReserve.toString()})`);
                else if (!new BigNumber(row.poolId || '0').gt(0)) {
                    Log.info(`LP ${name} pulled: no active underlying lp (liquidity ${primaryReserve.toString()}/${secondaryReserve.toString()})`);
                    const twap = await Exchange.marketPrice(assets.primary, assets.secondary, rule.twapInterval);
                    const last = twap.gt(0) ? twap : await Exchange.marketPrice(assets.primary, assets.secondary);
                    const anchor = last.gt(0) ? last : (state.lastPrice != null && new BigNumber(state.lastPrice).gt(0) ? new BigNumber(state.lastPrice) : null);
                    if (anchor != null)
                        await redeploy(anchor, true);
                    else
                        Log.info(`LP ${name} pulled: waiting for a price anchor (retry on next wake/check)`);
                } else {
                    const price = await Exchange.marketPrice(assets.primary, assets.secondary, rule.twapInterval);
                    const delta = spotPrice.gt(0) ? price.minus(spotPrice).dividedBy(spotPrice).abs() : new BigNumber(Math.max(rule.threshold.toNumber(), 1));
                    const expired = rule.rebalanceInterval != null && Date.now() - Math.max(state.lastRedeploy, startedAt) >= rule.rebalanceInterval * 1_000;
                    if (!price.gt(0))
                        Log.info(`LP ${name} skipped: no market price`);
                    else if (!delta.gt(rule.threshold) && !expired)
                        Log.info(`LP ${name} passed: ${ByteUtil.bigNumberToString(price)} +${delta.multipliedBy(100).toFixed(2)}% dev`);
                    else if (rule.cooldownInterval > 0 && state.lastRedeploy > 0 && Date.now() - state.lastRedeploy < rule.cooldownInterval * 1_000) {
                        Log.info(`LP ${name} passed: ${delta.multipliedBy(100).toFixed(2)}% dev within cooldown interval`);
                        state.nextCheckAt = state.lastRedeploy + rule.cooldownInterval * 1_000;
                    } else {
                        Log.info(expired && !delta.gt(rule.threshold) ? `LP ${name} expired: rebalancing per ${rule.rebalanceInterval}s forced period (+${delta.multipliedBy(100).toFixed(2)}% dev)` : `LP ${name} staled: ${ByteUtil.bigNumberToString(spotPrice)} +${delta.multipliedBy(100).toFixed(2)}% dev`);
                        deactivatedPools[String(row.poolId)] = Date.now();
                        await redeploy(price, false);
                    }
                }
                
                state.failCount = 0;
                state.backoffUntil = 0;
                if (rule.rebalanceInterval != null)
                    state.nextCheckAt = Math.min(state.nextCheckAt, Math.max(state.lastRedeploy, startedAt) + rule.rebalanceInterval * 1_000);
            } catch (exception) {
                state.failCount++;
                const delay = Math.min(config.backoff.cap, config.backoff.base * Math.pow(2, state.failCount - 1));
                state.backoffUntil = Date.now() + delay * 1_000;
                state.nextCheckAt = state.backoffUntil;
                Log.error(`LP ${name} renewal failed (attempt ${state.failCount}, retry in ${delay}s):`, exception);
            }
        }

        for (let name in pools) {
            const mutation = mutations[name];
            if (typeof mutation == 'string') {
                Log.info(`LP ${name} now watching: ${mutation}`);
            } else if (typeof mutation != 'boolean') {
                delete pools[name];
                Log.info(`LP ${name} watching stopped`);
            }
        }

        Exchange.subscribe([address, ...new Set(rows.map((x) => x.delegatorAccount))]);
        let nextCycle = time + config.refreshInterval * 1_000;
        for (let name in pools) {
            nextCycle = Math.min(nextCycle, Math.max(pools[name].nextCheckAt, pools[name].backoffUntil));
        }
        
        nextCycle = Math.max(nextCycle, Date.now() + 1_000);
        Log.info(`LP loop: cycle finished in ${Date.now() - time}ms, next cycle in ${Math.max(0, Math.ceil((nextCycle - Date.now()) / 1_000))}s (at ${new Date(nextCycle).toISOString()})`);
        if (wakeups == sequence) {
            await new Promise<void>((resolve) => {
                const finish = () => {
                    clearTimeout(timer);
                    const index = wakeListeners.indexOf(finish);
                    if (index >= 0)
                        wakeListeners.splice(index, 1);
                    resolve();
                };
                const timer = setTimeout(finish, Math.max(0, nextCycle - Date.now()));
                wakeListeners.push(finish);
            });
        }

        if (wakeups != sequence) {
            Log.info('LP loop: wake signal received, forcing next cycle immediately');
            woke = true;
        }
    }
}

main();
