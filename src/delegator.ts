import { AssetId, ByteUtil, Chain, Hashsig, LiquidityPool, Messages, RPC, SchemaUtil, Seckey, Signing, Spot, Stream, Transactions, Uint256, UiUtil } from 'tangentsdk';
import { Log } from './logging';
import BigNumber from 'bignumber.js';
import process from 'node:process';
import fs from 'fs';
import WebSocket from 'ws';

type RuleConfig = { checkInterval?: number, twapInterval?: number, threshold?: number, fee?: number, range?: number | null, reserve?: number, cooldownInterval?: number, rebalanceInterval?: number | null };
type ResolvedConfig = { checkInterval: number, twapInterval: number, threshold: BigNumber, fee: BigNumber, range: number | null, reserve: BigNumber, cooldownInterval: number, rebalanceInterval: number | null };
type DelegationRow = { primaryAsset: string, secondaryAsset: string, primaryLiquidity: string, secondaryLiquidity: string, price: string, poolId: string, delegatorAccount: string };
type DelegatorConfig = { network: string, validator: string, exchange: string, relay?: string, secretKey?: string, secretKeyEnv?: string, confirmTimeout?: number, refreshInterval?: number, watch?: { pingInterval?: number, connectTimeout?: number }, backoff?: { base?: number, cap?: number }, rules?: Record<string, RuleConfig> };

const delegations: Record<string, DelegationRow> = { };
const workers: Record<string, { stopped: boolean }> = { };
const wakeListeners: (() => void)[] = [ ];
const deactivatedPools: Record<string, number> = { };
let config: DelegatorConfig;
let address: string;
let secretKey: Seckey;
let confirmTimeout = 120, backoffBase = 15, backoffCap = 300;
let rules: Record<string, RuleConfig> = { }, wildcard: RuleConfig = { };
let wakeSeq = 0;
let txLock: Promise<void> = Promise.resolve();
let socket: WebSocket | null = null;
let reconnectTimer: NodeJS.Timeout | null = null;
let watchPingInterval = 30, watchConnectTimeout = 15;

function notify(): void {
    wakeSeq++;
    for (let listener of wakeListeners.splice(0))
        listener();
}
function wait(deadline: number): Promise<void> {
    const { promise, resolve } = Promise.withResolvers<void>();
    const finish = () => {
        const index = wakeListeners.indexOf(finish);
        if (index >= 0)
            wakeListeners.splice(index, 1);
        clearTimeout(timer);
        resolve();
    };
    wakeListeners.push(finish);
    const timer = setTimeout(finish, Math.max(0, deadline - Date.now()));
    return promise;
}
function reconnect(exchangeUrl: string, getAccounts: () => string[]): void {
    if (reconnectTimer != null)
        return;
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        if (socket != null && (socket.readyState == WebSocket.OPEN || socket.readyState == WebSocket.CONNECTING))
            return;
        connect(exchangeUrl, getAccounts);
    }, 5_000);
}
function connect(exchangeUrl: string, getAccounts: () => string[]): void {
    let client: WebSocket;
    try {
        client = new WebSocket(exchangeUrl);
    } catch (exception) {
        Log.error(`relay watch: failed to connect (${exchangeUrl}):`, exception);
        reconnect(exchangeUrl, getAccounts);
        return;
    }

    socket = client;
    let alive = true, heartbeat: NodeJS.Timeout | undefined;
    const connectTimer = setTimeout(() => {
        if (client.readyState == WebSocket.CONNECTING) {
            Log.error(`relay watch: connect timeout (${exchangeUrl}), reconnecting`);
            try { client.terminate(); } catch { }
        }
    }, watchConnectTimeout * 1_000);

    client.on('open', () => {
        alive = true;
        clearTimeout(connectTimer);
        client.send(JSON.stringify({ method: 'post://', params: { accounts: getAccounts() } }));
        Log.info(`relay watch: connected (${exchangeUrl})`);
        heartbeat = setInterval(() => {
            if (!alive) {
                Log.error(`relay watch: connection stale (no pong within ${watchPingInterval}s), reconnecting`);
                try { client.terminate(); } catch { }
                return;
            }

            alive = false;
            try { client.ping(); } catch { }
        }, watchPingInterval * 1_000);
    });
    client.on('pong', () => {
        alive = true;
    });
    client.on('message', (data: unknown) => {
        alive = true;
        try {
            const notification = JSON.parse(String(data)).notification;
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
                notify();
            }
        } catch { }
    });
    client.on('close', () => {
        clearInterval(heartbeat);
        if (socket == client)
            socket = null;
        Log.error(`relay watch: disconnected (${exchangeUrl}), retrying in 5s`);
        reconnect(exchangeUrl, getAccounts);
    });
    client.on('error', (exception: unknown) => {
        Log.error(`relay watch: socket error (${exchangeUrl}):`, exception);
        try { client.close(); } catch { }
    });
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

        const transactionHash = await RPC.submitTransaction(build(gasLimit).encode());
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
            const pending = Promise.withResolvers<void>();
            setTimeout(pending.resolve, 1_000);
            await pending.promise;
        }

        Log.info(`LP ${pool.name} renewal finalized (price: ${pool.price.toString()}, tx: ${transactionHash})`);
    } catch (exception) {
        Log.info('TX message:', simulation.encode());
        throw exception;
    }
}
async function delegate(name: string): Promise<void> {
    const worker = workers[name] = { stopped: false };
    const state = { failCount: 0, backoffUntil: 0, lastRedeploy: 0, lastPrice: null as BigNumber | null };
    const merged: RuleConfig = { ...wildcard, ...rules[name] };
    const cfg: ResolvedConfig = {
        checkInterval: merged.checkInterval || 1_200,
        twapInterval: merged.twapInterval || 600,
        threshold: merged.threshold != null ? new BigNumber(merged.threshold) : new BigNumber(0.01),
        fee: merged.fee != null ? new BigNumber(merged.fee) : new BigNumber(0.0005),
        range: (merged.range === null || merged.range === 0) ? null : Number(merged.range ?? 0.05),
        reserve: merged.reserve != null ? new BigNumber(merged.reserve) : new BigNumber(0),
        cooldownInterval: merged.cooldownInterval ?? 180,
        rebalanceInterval: (merged.rebalanceInterval === 0) ? null : (merged.rebalanceInterval ?? null)
    };
    Log.info(`LP ${name} worker started (check interval: ${cfg.checkInterval}s)`);
    while (!worker.stopped) {
        const row = delegations[name];
        let nextCheck = Date.now() + cfg.checkInterval * 1_000;
        if (row != null) {
            const seqBefore = wakeSeq;
            try {
                if (Date.now() < state.backoffUntil)
                    nextCheck = state.backoffUntil;
                else {
                    const primaryAsset = new AssetId(row.primaryAsset);
                    const secondaryAsset = new AssetId(row.secondaryAsset);
                    const primaryReserve = new BigNumber(row.primaryLiquidity);
                    const secondaryReserve = new BigNumber(row.secondaryLiquidity);
                    const spotPrice = new BigNumber(row.price || '0');
                    const redeploy = (price: BigNumber, pull: boolean) => {
                        const transfer = () => rebalance({
                            delegatedAccount: row.delegatorAccount,
                            primaryAsset,
                            secondaryAsset,
                            primaryReserve,
                            secondaryReserve,
                            price,
                            fee: cfg.fee,
                            reserve: cfg.reserve,
                            range: cfg.range,
                            name,
                            pull
                        });

                        const result = txLock.then(transfer, transfer);
                        txLock = result.then(() => { }, () => { });
                        return result;
                    };
                    if (spotPrice.gt(0))
                        state.lastPrice = spotPrice;
                    if (!primaryReserve.gt(0) || !secondaryReserve.gt(0))
                        Log.info(`LP ${name} idle: zero reserves (liquidity ${primaryReserve.toString()}/${secondaryReserve.toString()})`);
                    else if (!new BigNumber(row.poolId || '0').gt(0)) {
                        Log.info(`LP ${name} pulled: no active underlying lp (liquidity ${primaryReserve.toString()}/${secondaryReserve.toString()})`);
                        const twap = new BigNumber(String((await call(config.exchange, 'market/price', { primaryAssetHash: primaryAsset.id, secondaryAssetHash: secondaryAsset.id, interval: cfg.twapInterval })) ?? '0'));
                        const last = twap.gt(0) ? twap : new BigNumber(String((await call(config.exchange, 'market/price', { primaryAssetHash: primaryAsset.id, secondaryAssetHash: secondaryAsset.id })) ?? '0'));
                        const anchor = last.gt(0) ? last : (state.lastPrice && state.lastPrice.gt(0) ? state.lastPrice : null);
                        if (!anchor)
                            Log.info(`LP ${name} pulled: waiting for a price anchor (retry on next wake/check)`);
                        else {
                            await redeploy(anchor, true);
                            state.lastRedeploy = Date.now();
                        }
                    } else {
                        const price = new BigNumber(String((await call(config.exchange, 'market/price', { primaryAssetHash: primaryAsset.id, secondaryAssetHash: secondaryAsset.id, interval: cfg.twapInterval })) ?? '0'));
                        if (!price.gt(0))
                            Log.info(`LP ${name} skipped: no market price`);
                        else {
                            const delta = spotPrice.gt(0) ? price.minus(spotPrice).dividedBy(spotPrice).abs() : new BigNumber(Math.max(cfg.threshold.toNumber(), 1));
                            const expired = cfg.rebalanceInterval != null && (state.lastRedeploy == 0 || Date.now() - state.lastRedeploy >= cfg.rebalanceInterval * 1_000);
                            if (!delta.gt(cfg.threshold) && !expired)
                                Log.info(`LP ${name} passed: ${ByteUtil.bigNumberToString(price)} +${delta.multipliedBy(100).toFixed(2)}% dev`);
                            else if (cfg.cooldownInterval > 0 && state.lastRedeploy > 0 && Date.now() - state.lastRedeploy < cfg.cooldownInterval * 1_000) {
                                Log.info(`LP ${name} passed: ${delta.multipliedBy(100).toFixed(2)}% dev within cooldown interval`);
                                nextCheck = state.lastRedeploy + cfg.cooldownInterval * 1_000;
                            } else {
                                Log.info(expired && !delta.gt(cfg.threshold) ? `LP ${name} expired: rebalancing per ${cfg.rebalanceInterval}s forced period (+${delta.multipliedBy(100).toFixed(2)}% dev)` : `LP ${name} staled: ${ByteUtil.bigNumberToString(spotPrice)} +${delta.multipliedBy(100).toFixed(2)}% dev`);
                                deactivatedPools[String(row.poolId)] = Date.now();
                                await redeploy(price, false);
                                state.lastRedeploy = Date.now();
                            }
                        }
                    }
                    state.failCount = 0;
                    state.backoffUntil = 0;
                    if (cfg.rebalanceInterval != null && state.lastRedeploy > 0)
                        nextCheck = Math.min(nextCheck, state.lastRedeploy + cfg.rebalanceInterval * 1_000);
                }
            } catch (exception) {
                state.failCount++;
                const delay = Math.min(backoffCap, backoffBase * Math.pow(2, state.failCount - 1));
                state.backoffUntil = Date.now() + delay * 1_000;
                nextCheck = state.backoffUntil;
                Log.error(`LP ${name} renewal failed (attempt ${state.failCount}, retry in ${delay}s):`, exception);
            }
            if (wakeSeq != seqBefore)
                continue;
        }
        await wait(nextCheck);
    }
    if (workers[name] == worker)
        delete workers[name];
    Log.info(`LP ${name} worker stopped`);
}
async function main(): Promise<void> {
    BigNumber.config({ DECIMAL_PLACES: 18, ROUNDING_MODE: 1 });
    try {
        const parsed: unknown = JSON.parse(fs.readFileSync(process.argv[2]).toString('utf8'));
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

        const rules: Record<string, RuleConfig> = { };
        for (let key in (source.rules as Record<string, unknown>) || { }) {
            const entry = (source.rules as Record<string, unknown>)[key];
            if (typeof entry != 'object' || entry == null || Array.isArray(entry))
                throw new Error(`invalid config for rule ${key}`);

            const rule = entry as RuleConfig; // all fields optional, behavioral numeric bounds validated below
            if (rule.reserve != null && (typeof rule.reserve != 'number' || rule.reserve < 0 || rule.reserve >= 1))
                throw new Error(`invalid reserve for rule ${key}`);
            if (rule.rebalanceInterval != null && rule.rebalanceInterval !== 0 && !(typeof rule.rebalanceInterval == 'number' && rule.rebalanceInterval > 0))
                throw new Error(`invalid rebalance interval for rule ${key}`);
            rules[key] = rule;
        }

        config = {
            network: source.network,
            validator: source.validator,
            exchange: source.exchange,
            relay: typeof source.relay == 'string' ? source.relay : undefined,
            secretKey: typeof source.secretKey == 'string' ? source.secretKey : undefined,
            secretKeyEnv: typeof source.secretKeyEnv == 'string' ? source.secretKeyEnv : undefined,
            confirmTimeout: typeof source.confirmTimeout == 'number' ? source.confirmTimeout : undefined,
            refreshInterval: typeof source.refreshInterval == 'number' ? source.refreshInterval : undefined,
            watch: typeof source.watch == 'object' && source.watch != null ? source.watch as { pingInterval?: number, connectTimeout?: number } : undefined, // nested numerics consumed with ?? defaults
            backoff: typeof source.backoff == 'object' && source.backoff != null ? source.backoff as { base?: number, cap?: number } : undefined, // nested numerics consumed with ?? defaults
            rules
        };
        Chain.props = (Chain as unknown as Record<string, typeof Chain.props>)[source.network];
        RPC.applyValidator(source.validator);
    } catch (exception) {
        Log.error('path', process.argv[2] || null, ' failed to load a config:', exception);
        return process.exit(1);
    }

    const decodedSecretKey = Signing.decodeSecretKey(typeof config.secretKey == 'string' ? config.secretKey : (process.env[config.secretKeyEnv || 'WSK'] || ''));
    if (!decodedSecretKey) {
        Log.error('invalid secret key env');
        return process.exit(1);
    }
    secretKey = decodedSecretKey;

    const encodedAddress = Signing.encodeAddress(Signing.derivePublicKeyHash(Signing.derivePublicKey(secretKey)));
    if (!encodedAddress) {
        Log.error('invalid address');
        return process.exit(1);
    }
    address = encodedAddress;
    Log.info(`LP delegator account: ${address}`);

    rules = config.rules || { };
    wildcard = rules['*'] || { };
    confirmTimeout = config.confirmTimeout || 120;
    backoffBase = config.backoff?.base ?? 15;
    backoffCap = config.backoff?.cap ?? 300;
    watchPingInterval = config.watch?.pingInterval ?? 30;
    watchConnectTimeout = config.watch?.connectTimeout ?? 15;
    const refreshInterval = config.refreshInterval || 300;
    let registeredAccounts: string[] = [address];
    connect(typeof config.relay == 'string' ? config.relay : config.exchange.replace(/^https/, 'wss').replace(/^http/, 'ws'), () => registeredAccounts);

    let loggedCount: number | null = null;
    while (true) {
        const seqBefore = wakeSeq;
        const rawPools = await call(config.exchange, 'account/delegations', { account: address });
        const fetched = Array.isArray(rawPools);
        const rows = (fetched ? rawPools : []) as DelegationRow[];
        if (!fetched)
            Log.error('failed to fetch delegations, retrying within 10s');
        else {
            const names = new Set<string>();
            for (let row of rows) {
                const assets = { primary: new AssetId(row.primaryAsset), secondary: new AssetId(row.secondaryAsset) };
                const name = `${assets.primary.token || assets.primary.chain}/${assets.secondary.token || assets.secondary.chain}`;
                names.add(name);
                delegations[name] = row;
                if (workers[name] == null || workers[name].stopped)
                    void delegate(name);
            }
            let removed = false;
            for (let name in workers) {
                if (!names.has(name)) {
                    workers[name].stopped = true;
                    delete delegations[name];
                    removed = true;
                }
            }
            if (removed)
                notify();
            registeredAccounts = [address, ...new Set(rows.map((x) => x.delegatorAccount))];
            if (socket != null && socket.readyState == WebSocket.OPEN)
                socket.send(JSON.stringify({ method: 'post://', params: { accounts: registeredAccounts } }));
            if (rows.length != loggedCount) {
                loggedCount = rows.length;
                Log.info(`LP delegations (${rows.length}):`, rows);
            }
        }
        if (wakeSeq != seqBefore)
            continue;
        await wait(Date.now() + (fetched ? refreshInterval : 10) * 1_000);
    }
}

main();
